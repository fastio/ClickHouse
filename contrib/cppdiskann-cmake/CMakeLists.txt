if (NOT OS_LINUX OR NOT ARCH_AMD64)
    message (STATUS "Not using cppdiskann: only Linux x86_64 is supported")
    return ()
endif ()

find_library (CPPDISKANN_OPENMP_LIBRARY
    NAMES
        libgomp.a
        gomp
        libgomp.so.1
    PATHS
        /usr/lib/gcc/x86_64-linux-gnu/11
        /usr/lib/gcc/x86_64-linux-gnu/12
        /usr/lib/gcc/x86_64-linux-gnu/13
        /usr/lib/x86_64-linux-gnu
        /lib/x86_64-linux-gnu
)
if (NOT CPPDISKANN_OPENMP_LIBRARY)
    message (STATUS "Not using cppdiskann: GNU OpenMP runtime was not found")
    return ()
endif ()

find_path (CPPDISKANN_OPENMP_INCLUDE_DIR omp.h
    PATHS
        /usr/bin/llvm-21/lib/clang/21/include
        /usr/lib/llvm-21/lib/clang/21/include
        /usr/lib/gcc/x86_64-linux-gnu/11/include
)
if (NOT CPPDISKANN_OPENMP_INCLUDE_DIR)
    message (STATUS "Not using cppdiskann: OpenMP headers were not found")
    return ()
endif ()

find_path (CPPDISKANN_LIBAIO_INCLUDE_DIR libaio.h PATHS /usr/include)
find_library (CPPDISKANN_LIBAIO_LIBRARY
    NAMES
        aio
        libaio.so.1
    PATHS
        /usr/lib/x86_64-linux-gnu
        /lib/x86_64-linux-gnu
)
if (NOT CPPDISKANN_LIBAIO_INCLUDE_DIR OR NOT CPPDISKANN_LIBAIO_LIBRARY)
    message (STATUS "Not using cppdiskann: libaio headers or library were not found")
    return ()
endif ()

set (CPPDISKANN_PROJECT_DIR "${ClickHouse_SOURCE_DIR}/contrib/diskann-internal")
set (CPPDISKANN_SRC_DIR "${CPPDISKANN_PROJECT_DIR}/src")
set (CPPDISKANN_GENERATED_INCLUDE_DIR "${CMAKE_CURRENT_BINARY_DIR}/include")

file (MAKE_DIRECTORY "${CPPDISKANN_GENERATED_INCLUDE_DIR}")
configure_file (
    "${CPPDISKANN_LIBAIO_INCLUDE_DIR}/libaio.h"
    "${CPPDISKANN_GENERATED_INCLUDE_DIR}/libaio.h"
    COPYONLY
)
configure_file (
    "${CPPDISKANN_OPENMP_INCLUDE_DIR}/omp.h"
    "${CPPDISKANN_GENERATED_INCLUDE_DIR}/omp.h"
    COPYONLY
)

set (CPPDISKANN_SRCS
    "${CMAKE_CURRENT_SOURCE_DIR}/cppdiskann_blas_shim.cpp"
    "${CPPDISKANN_SRC_DIR}/ann_exception.cpp"
    "${CPPDISKANN_SRC_DIR}/aisaq_pq_reader.cpp"
    "${CPPDISKANN_SRC_DIR}/aisaq_utils.cpp"
    "${CPPDISKANN_SRC_DIR}/aux_utils.cpp"
    "${CPPDISKANN_SRC_DIR}/distance.cpp"
    "${CPPDISKANN_SRC_DIR}/index.cpp"
    "${CPPDISKANN_SRC_DIR}/linux_aligned_file_reader.cpp"
    "${CPPDISKANN_SRC_DIR}/logger.cpp"
    "${CPPDISKANN_SRC_DIR}/math_utils.cpp"
    "${CPPDISKANN_SRC_DIR}/memory_mapper.cpp"
    "${CPPDISKANN_SRC_DIR}/partition_and_pq.cpp"
    "${CPPDISKANN_SRC_DIR}/pq_flash_aisaq_index.cpp"
    "${CPPDISKANN_SRC_DIR}/pq_flash_index.cpp"
    "${CPPDISKANN_SRC_DIR}/utils.cpp"
)

add_library (_cppdiskann STATIC ${CPPDISKANN_SRCS})
add_library (ch_contrib::cppdiskann ALIAS _cppdiskann)

target_include_directories (_cppdiskann SYSTEM PUBLIC
    "${CPPDISKANN_PROJECT_DIR}/include"
    "${CPPDISKANN_PROJECT_DIR}/include/diskann"
    "${CMAKE_CURRENT_SOURCE_DIR}/include"
    "${CPPDISKANN_GENERATED_INCLUDE_DIR}"
)

target_compile_features (_cppdiskann PUBLIC cxx_std_17)
target_compile_options (_cppdiskann PRIVATE
    -std=c++17
    -w
    -include
    "${CMAKE_CURRENT_SOURCE_DIR}/include/cppdiskann_blas_shim.h"
    -mavx2
    -mfma
    -msse2
    -ftree-vectorize
    -fopenmp=libgomp
    -fopenmp-simd
    -funroll-loops
)
target_compile_definitions (_cppdiskann PUBLIC
    USE_AVX2
)

target_link_libraries (_cppdiskann PUBLIC
    boost::headers_only
    "${CPPDISKANN_OPENMP_LIBRARY}"
    "${CPPDISKANN_LIBAIO_LIBRARY}"
    pthread
    m
    dl
)
