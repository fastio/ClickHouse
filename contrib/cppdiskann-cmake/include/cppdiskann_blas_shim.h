#pragma once

#include <cstddef>
#include <cstdint>
#include <functional>
#include <future>
#include <iostream>
#include <shared_mutex>
#include <sstream>
#include <string>
#include <string_view>

namespace cppdiskann
{
enum class LogLevel
{
    Debug,
    Info,
    Warning,
    Error,
};

using LogCallback = std::function<void(LogLevel, std::string_view)>;

void setLogCallback(LogCallback callback);
bool logMessage(LogLevel level, std::string_view message);
LogLevel parseLogLevel(std::string_view level);

class LogStream
{
public:
    explicit LogStream(LogLevel level_);
    ~LogStream();

    template <typename T>
    LogStream & operator<<(const T & value)
    {
        stream << value;
        return *this;
    }

    LogStream & operator<<(std::ostream & (*manipulator)(std::ostream &));
    LogStream & operator<<(std::ios & (*manipulator)(std::ios &));
    LogStream & operator<<(std::ios_base & (*manipulator)(std::ios_base &));

private:
    LogLevel level;
    std::ostringstream stream;
};
}

#ifndef FINTEGER
#define FINTEGER long
#endif

#ifndef ROUND_UP
#define ROUND_UP(X, Y) ((((X) + (Y) - 1) / (Y)) * (Y))
#endif

#ifndef DIV_ROUND_UP
#define DIV_ROUND_UP(X, Y) (((X) + (Y) - 1) / (Y))
#endif

#ifndef LOG_KNOWHERE_DEBUG_
#define LOG_KNOWHERE_DEBUG_ ::cppdiskann::LogStream(::cppdiskann::LogLevel::Debug)
#endif

#ifndef LOG_KNOWHERE_INFO_
#define LOG_KNOWHERE_INFO_ ::cppdiskann::LogStream(::cppdiskann::LogLevel::Info)
#endif

#ifndef LOG_KNOWHERE_WARNING_
#define LOG_KNOWHERE_WARNING_ ::cppdiskann::LogStream(::cppdiskann::LogLevel::Warning)
#endif

#ifndef LOG_KNOWHERE_ERROR_
#define LOG_KNOWHERE_ERROR_ ::cppdiskann::LogStream(::cppdiskann::LogLevel::Error)
#endif

#ifndef ERROR
#define ERROR 0
#endif

#ifndef WARNING
#define WARNING 1
#endif

#ifndef INFO
#define INFO 2
#endif

#ifndef LOG
#define LOG(level) ::cppdiskann::LogStream(::cppdiskann::parseLogLevel(#level))
#endif

namespace folly
{
struct Unit
{
};

template <typename T>
using Future = std::future<T>;
}

enum CBLAS_LAYOUT
{
    CblasRowMajor = 101,
    CblasColMajor = 102,
};

enum CBLAS_TRANSPOSE
{
    CblasNoTrans = 111,
    CblasTrans = 112,
};

extern "C"
{
float cblas_snrm2(size_t n, const float * x, size_t incx);

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
    size_t ldc);

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
    FINTEGER * ldc);
}
