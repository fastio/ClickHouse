#pragma once

#include <cppdiskann_blas_shim.h>

#define LOG_KNOWHERE_DEBUG_ ::cppdiskann::LogStream(::cppdiskann::LogLevel::Debug)
#define LOG_KNOWHERE_INFO_ ::cppdiskann::LogStream(::cppdiskann::LogLevel::Info)
#define LOG_KNOWHERE_WARNING_ ::cppdiskann::LogStream(::cppdiskann::LogLevel::Warning)
#define LOG_KNOWHERE_ERROR_ ::cppdiskann::LogStream(::cppdiskann::LogLevel::Error)
