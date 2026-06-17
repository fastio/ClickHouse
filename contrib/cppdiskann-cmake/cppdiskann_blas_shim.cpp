#include "cppdiskann_blas_shim.h"

#include <cmath>
#include <cstddef>
#include <cstring>
#include <mutex>

namespace
{

std::mutex log_callback_mutex;
cppdiskann::LogCallback log_callback;

bool isTranspose(CBLAS_TRANSPOSE trans)
{
    return trans == CblasTrans;
}

float matrixAt(const float * matrix, size_t ld, size_t row, size_t col, bool transposed)
{
    return transposed ? matrix[col * ld + row] : matrix[row * ld + col];
}

}

namespace cppdiskann
{

void setLogCallback(LogCallback callback)
{
    std::lock_guard lock(log_callback_mutex);
    log_callback = std::move(callback);
}

bool logMessage(LogLevel level, std::string_view message)
{
    LogCallback callback;
    {
        std::lock_guard lock(log_callback_mutex);
        callback = log_callback;
    }

    if (!callback)
        return false;

    callback(level, message);
    return true;
}

LogLevel parseLogLevel(std::string_view level)
{
    if (level == "ERROR")
        return LogLevel::Error;
    if (level == "WARNING")
        return LogLevel::Warning;
    if (level == "INFO")
        return LogLevel::Info;
    return LogLevel::Debug;
}

LogStream::LogStream(LogLevel level_)
    : level(level_)
{
}

LogStream::~LogStream()
{
    try
    {
        const auto message = stream.str();
        if (!logMessage(level, message))
            std::cerr << message;
    }
    catch (...)
    {
        /// Logging must never throw from a stream destructor.
    }
}

LogStream & LogStream::operator<<(std::ostream & (*manipulator)(std::ostream &))
{
    stream << manipulator;
    return *this;
}

LogStream & LogStream::operator<<(std::ios & (*manipulator)(std::ios &))
{
    stream << manipulator;
    return *this;
}

LogStream & LogStream::operator<<(std::ios_base & (*manipulator)(std::ios_base &))
{
    stream << manipulator;
    return *this;
}

}

extern "C"
{

float cblas_snrm2(size_t n, const float * x, size_t incx)
{
    float sum = 0.0f;
    for (size_t i = 0; i < n; ++i)
    {
        const float value = x[i * incx];
        sum += value * value;
    }
    return std::sqrt(sum);
}

void cblas_sgemm(
    CBLAS_LAYOUT layout,
    CBLAS_TRANSPOSE transa,
    CBLAS_TRANSPOSE transb,
    size_t m,
    size_t n,
    size_t k,
    float alpha,
    const float * a,
    size_t lda,
    const float * b,
    size_t ldb,
    float beta,
    float * c,
    size_t ldc)
{
    if (layout != CblasRowMajor)
        return;

    const bool transpose_a = isTranspose(transa);
    const bool transpose_b = isTranspose(transb);
    for (size_t row = 0; row < m; ++row)
    {
        for (size_t col = 0; col < n; ++col)
        {
            float acc = 0.0f;
            for (size_t inner = 0; inner < k; ++inner)
                acc += matrixAt(a, lda, row, inner, transpose_a) * matrixAt(b, ldb, col, inner, transpose_b);
            c[row * ldc + col] = alpha * acc + beta * c[row * ldc + col];
        }
    }
}

int sgemm_(
    const char * transa,
    const char * transb,
    FINTEGER * m,
    FINTEGER * n,
    FINTEGER * k,
    const float * alpha,
    const float * a,
    FINTEGER * lda,
    const float * b,
    FINTEGER * ldb,
    float * beta,
    float * c,
    FINTEGER * ldc)
{
    const bool transpose_a = transa && std::strncmp(transa, "T", 1) == 0;
    const bool transpose_b = transb && std::strncmp(transb, "T", 1) == 0;
    cblas_sgemm(
        CblasRowMajor,
        transpose_a ? CblasTrans : CblasNoTrans,
        transpose_b ? CblasTrans : CblasNoTrans,
        static_cast<size_t>(*m),
        static_cast<size_t>(*n),
        static_cast<size_t>(*k),
        *alpha,
        a,
        static_cast<size_t>(*lda),
        b,
        static_cast<size_t>(*ldb),
        *beta,
        c,
        static_cast<size_t>(*ldc));
    return 0;
}

}
