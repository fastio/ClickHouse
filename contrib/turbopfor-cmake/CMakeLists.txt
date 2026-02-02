option(ENABLE_TURBOPFOR "Enable TurboPFor integer compression library" ${ENABLE_LIBRARIES})

if (NOT ENABLE_TURBOPFOR)
    message(STATUS "Not using TurboPFor")
    return()
endif()

set(LIBRARY_DIR "${ClickHouse_SOURCE_DIR}/contrib/TurboPFor")
set(TURBOPFOR_ORIGINAL_DIR "${LIBRARY_DIR}/third_party/TurboPFor")

# Check if submodule exists
if (NOT EXISTS "${LIBRARY_DIR}/include/turbopfor.h")
    message(STATUS "TurboPFor: source not found, disabling")
    return()
endif()

# TurboPFor requires SSE4.2 at minimum
if (NOT ARCH_AMD64)
    message(STATUS "TurboPFor: only x86_64 is supported, disabling")
    return()
endif()

# Scalar sources (always included)
set(TURBOPFOR_SCALAR_SRCS
    "${LIBRARY_DIR}/src/dispatch.cpp"
    "${LIBRARY_DIR}/src/scalar/p4enc32.cpp"
    "${LIBRARY_DIR}/src/scalar/p4d1dec32.cpp"
    "${LIBRARY_DIR}/src/scalar/p4_scalar_bitpack32.cpp"
    "${LIBRARY_DIR}/src/scalar/p4_scalar_bitunpack32.cpp"
    "${LIBRARY_DIR}/src/scalar/p4_scalar_bitunpackd1_32.cpp"
    "${LIBRARY_DIR}/src/scalar/p4_scalar_internal.cpp"
    "${LIBRARY_DIR}/src/scalar/bitpack128v32_scalar.cpp"
    "${LIBRARY_DIR}/src/scalar/p4enc128v32_scalar.cpp"
    "${LIBRARY_DIR}/src/scalar/p4d1dec128v32_scalar.cpp"
    "${LIBRARY_DIR}/src/scalar/bitpack256v32_scalar.cpp"
    "${LIBRARY_DIR}/src/scalar/p4enc256v32_scalar.cpp"
    "${LIBRARY_DIR}/src/scalar/p4d1dec256v32_scalar.cpp"
)

# SIMD SSE4.2 sources for 128-element blocks
set(TURBOPFOR_SIMD_SSE_SRCS
    "${LIBRARY_DIR}/src/simd/p4enc128v32.cpp"
    "${LIBRARY_DIR}/src/simd/p4d1dec128v32.cpp"
    "${LIBRARY_DIR}/src/simd/p4_simd_internal.cpp"
    "${LIBRARY_DIR}/src/simd/bitpack128v32_simd.cpp"
    "${LIBRARY_DIR}/src/simd/bitunpack128v32_simd.cpp"
)

# SIMD AVX2 sources for 256-element blocks
set(TURBOPFOR_SIMD_AVX2_SRCS
    "${LIBRARY_DIR}/src/simd/p4enc256v32.cpp"
    "${LIBRARY_DIR}/src/simd/p4d1dec256v32.cpp"
    "${LIBRARY_DIR}/src/simd/p4_simd_internal_256v.cpp"
    "${LIBRARY_DIR}/src/simd/bitpack256v32_simd.cpp"
    "${LIBRARY_DIR}/src/simd/bitunpack256v32_simd.cpp"
)

# Original TurboPFor reference library sources (needed for scalar fallback)
set(TURBOPFOR_REF_SRCS
    "${TURBOPFOR_ORIGINAL_DIR}/lib/v8.c"
    "${TURBOPFOR_ORIGINAL_DIR}/lib/vint.c"
    "${TURBOPFOR_ORIGINAL_DIR}/lib/trlec.c"
    "${TURBOPFOR_ORIGINAL_DIR}/lib/trled.c"
    "${TURBOPFOR_ORIGINAL_DIR}/lib/vsimple.c"
    "${TURBOPFOR_ORIGINAL_DIR}/lib/bitutil.c"
    "${TURBOPFOR_ORIGINAL_DIR}/lib/bitpack.c"
    "${TURBOPFOR_ORIGINAL_DIR}/lib/bitunpack.c"
    "${TURBOPFOR_ORIGINAL_DIR}/lib/vp4c.c"
    "${TURBOPFOR_ORIGINAL_DIR}/lib/vp4d.c"
    "${TURBOPFOR_ORIGINAL_DIR}/lib/transpose.c"
)

# Build all sources together
add_library(_turbopfor
    ${TURBOPFOR_SCALAR_SRCS}
    ${TURBOPFOR_SIMD_SSE_SRCS}
    ${TURBOPFOR_SIMD_AVX2_SRCS}
    ${TURBOPFOR_REF_SRCS}
)

target_include_directories(_turbopfor SYSTEM PUBLIC
    "${LIBRARY_DIR}/include"
)

target_include_directories(_turbopfor PRIVATE
    "${LIBRARY_DIR}/src"
    "${TURBOPFOR_ORIGINAL_DIR}/lib/include_"
)

# Base compile options
target_compile_options(_turbopfor PRIVATE
    -Wno-conversion
    -Wno-sign-conversion
    -Wno-old-style-cast
    -Wno-sign-compare
    -Wno-unused-parameter
    -Wno-implicit-fallthrough
    -Wno-unused-function
    -Wno-unused-variable
    -Wno-shorten-64-to-32
    -fstrict-aliasing
)

# Enable SIMD preprocessor definitions
target_compile_definitions(_turbopfor PRIVATE
    ENABLE_SSE42
    ENABLE_AVX2
    SSE2_ON
)

# Set SSE4.2 flags for SSE SIMD source files
set_source_files_properties(
    ${TURBOPFOR_SIMD_SSE_SRCS}
    PROPERTIES COMPILE_OPTIONS "-mssse3;-msse4.1;-msse4.2;-mpopcnt"
)

# Set AVX2 flags for AVX2 SIMD source files
set_source_files_properties(
    ${TURBOPFOR_SIMD_AVX2_SRCS}
    PROPERTIES COMPILE_OPTIONS "-mssse3;-msse4.1;-msse4.2;-mpopcnt;-mavx2"
)

# Reference library needs SSE flags
set_source_files_properties(
    ${TURBOPFOR_REF_SRCS}
    PROPERTIES COMPILE_OPTIONS "-mssse3;-msse4.1;-msse4.2;-mpopcnt"
)

set_target_properties(_turbopfor PROPERTIES
    CXX_STANDARD 20
    CXX_STANDARD_REQUIRED ON
    POSITION_INDEPENDENT_CODE TRUE
)

add_library(ch_contrib::turbopfor ALIAS _turbopfor)
message(STATUS "Using TurboPFor")
