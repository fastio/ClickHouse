option (ENABLE_DISKANN_CPP "Enable Microsoft DiskANN C++ library" ${ENABLE_LIBRARIES})

if (NOT ENABLE_DISKANN_CPP)
    message (STATUS "Not using DiskANN C++")
    return ()
endif ()

if (NOT OS_LINUX OR NOT ARCH_AMD64)
    message (STATUS "Not using DiskANN C++: only Linux x86_64 is supported")
    return ()
endif ()

# Prefer the static libgomp.a shipped with GCC over the system libgomp.so.1.
# The shared library on recent Ubuntu hosts carries versioned references to
# GLIBC_2.34 / GLIBC_2.32 symbols (pthread_create, dlopen, ...), which lld
# rejects under --no-allow-shlib-undefined when linking against ClickHouse's
# bundled sysroot (glibc 2.31). The static archive has no such version
# requirements and links cleanly against the sysroot libc/libpthread.
find_library (DISKANN_CPP_OPENMP_LIBRARY
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
if (NOT DISKANN_CPP_OPENMP_LIBRARY)
    message (STATUS "Not using DiskANN C++: GNU OpenMP runtime was not found")
    return ()
endif ()

find_path (DISKANN_CPP_OPENMP_INCLUDE_DIR omp.h
    PATHS
        /usr/bin/llvm-21/lib/clang/21/include
        /usr/lib/llvm-21/lib/clang/21/include
        /usr/lib/gcc/x86_64-linux-gnu/11/include
)
if (NOT DISKANN_CPP_OPENMP_INCLUDE_DIR)
    message (STATUS "Not using DiskANN C++: OpenMP headers were not found")
    return ()
endif ()

find_path (DISKANN_CPP_LIBAIO_INCLUDE_DIR libaio.h
    PATHS
        /usr/include
)
find_library (DISKANN_CPP_LIBAIO_LIBRARY
    NAMES
        aio
        libaio.so.1
    PATHS
        /usr/lib/x86_64-linux-gnu
        /lib/x86_64-linux-gnu
)
if (NOT DISKANN_CPP_LIBAIO_INCLUDE_DIR OR NOT DISKANN_CPP_LIBAIO_LIBRARY)
    message (STATUS "Not using DiskANN C++: libaio headers or library were not found")
    return ()
endif ()

find_path (DISKANN_CPP_MKL_INCLUDE_DIR mkl.h
    PATHS
        /opt/intel/oneapi/mkl/latest/include
        /usr/include/mkl
        /opt/intel/mkl/include
)

find_library (DISKANN_CPP_MKL_CORE_LIBRARY mkl_core
    PATHS
        /opt/intel/oneapi/mkl/latest/lib/intel64
        /usr/lib/x86_64-linux-gnu
        /opt/intel/mkl/lib/intel64
)
find_library (DISKANN_CPP_MKL_INTEL_ILP64_LIBRARY mkl_intel_ilp64
    PATHS
        /opt/intel/oneapi/mkl/latest/lib/intel64
        /usr/lib/x86_64-linux-gnu
        /opt/intel/mkl/lib/intel64
)
find_library (DISKANN_CPP_MKL_THREAD_LIBRARY mkl_gnu_thread
    PATHS
        /opt/intel/oneapi/mkl/latest/lib/intel64
        /usr/lib/x86_64-linux-gnu
        /opt/intel/mkl/lib/intel64
)

if (NOT DISKANN_CPP_MKL_INCLUDE_DIR
    OR NOT DISKANN_CPP_MKL_CORE_LIBRARY
    OR NOT DISKANN_CPP_MKL_INTEL_ILP64_LIBRARY
    OR NOT DISKANN_CPP_MKL_THREAD_LIBRARY)
    message (STATUS "Not using DiskANN C++: Intel MKL headers or libraries were not found")
    return ()
endif ()

set (DISKANN_CPP_PROJECT_DIR "${ClickHouse_SOURCE_DIR}/contrib/diskann_cpp")
set (DISKANN_CPP_SRC_DIR "${DISKANN_CPP_PROJECT_DIR}/src")
set (DISKANN_CPP_GENERATED_INCLUDE_DIR "${CMAKE_CURRENT_BINARY_DIR}/include")

file (MAKE_DIRECTORY "${DISKANN_CPP_GENERATED_INCLUDE_DIR}")
configure_file (
    "${DISKANN_CPP_LIBAIO_INCLUDE_DIR}/libaio.h"
    "${DISKANN_CPP_GENERATED_INCLUDE_DIR}/libaio.h"
    COPYONLY
)
configure_file (
    "${DISKANN_CPP_OPENMP_INCLUDE_DIR}/omp.h"
    "${DISKANN_CPP_GENERATED_INCLUDE_DIR}/omp.h"
    COPYONLY
)

set (DISKANN_CPP_SRCS
    "${DISKANN_CPP_SRC_DIR}/abstract_data_store.cpp"
    "${DISKANN_CPP_SRC_DIR}/abstract_index.cpp"
    "${DISKANN_CPP_SRC_DIR}/ann_exception.cpp"
    "${DISKANN_CPP_SRC_DIR}/disk_utils.cpp"
    "${DISKANN_CPP_SRC_DIR}/distance.cpp"
    "${DISKANN_CPP_SRC_DIR}/filter_utils.cpp"
    "${DISKANN_CPP_SRC_DIR}/index.cpp"
    "${DISKANN_CPP_SRC_DIR}/index_factory.cpp"
    "${DISKANN_CPP_SRC_DIR}/in_mem_data_store.cpp"
    "${DISKANN_CPP_SRC_DIR}/in_mem_graph_store.cpp"
    "${DISKANN_CPP_SRC_DIR}/linux_aligned_file_reader.cpp"
    "${DISKANN_CPP_SRC_DIR}/logger.cpp"
    "${DISKANN_CPP_SRC_DIR}/math_utils.cpp"
    "${DISKANN_CPP_SRC_DIR}/memory_mapper.cpp"
    "${DISKANN_CPP_SRC_DIR}/natural_number_map.cpp"
    "${DISKANN_CPP_SRC_DIR}/natural_number_set.cpp"
    "${DISKANN_CPP_SRC_DIR}/partition.cpp"
    "${DISKANN_CPP_SRC_DIR}/pq.cpp"
    "${DISKANN_CPP_SRC_DIR}/pq_data_store.cpp"
    "${DISKANN_CPP_SRC_DIR}/pq_flash_index.cpp"
    "${DISKANN_CPP_SRC_DIR}/pq_l2_distance.cpp"
    "${DISKANN_CPP_SRC_DIR}/scratch.cpp"
    "${DISKANN_CPP_SRC_DIR}/utils.cpp"
)

add_library (_diskann_cpp STATIC ${DISKANN_CPP_SRCS})
add_library (ch_contrib::diskann_cpp ALIAS _diskann_cpp)

target_include_directories (_diskann_cpp SYSTEM PUBLIC
    "${DISKANN_CPP_PROJECT_DIR}/include"
    "${DISKANN_CPP_GENERATED_INCLUDE_DIR}"
    "${DISKANN_CPP_MKL_INCLUDE_DIR}"
)

target_compile_features (_diskann_cpp PUBLIC cxx_std_17)
target_compile_options (_diskann_cpp PRIVATE
    -std=c++17
    -w
    -include
    immintrin.h
    -mavx2
    -mfma
    -msse2
    -ftree-vectorize
    -fopenmp=libgomp
    -fopenmp-simd
    -funroll-loops
)
target_compile_definitions (_diskann_cpp PUBLIC
    USE_AVX2
    MKL_ILP64
)

target_link_libraries (_diskann_cpp PUBLIC
    boost::headers_only
    "${DISKANN_CPP_OPENMP_LIBRARY}"
    "${DISKANN_CPP_LIBAIO_LIBRARY}"
    "${DISKANN_CPP_MKL_INTEL_ILP64_LIBRARY}"
    "${DISKANN_CPP_MKL_THREAD_LIBRARY}"
    "${DISKANN_CPP_MKL_CORE_LIBRARY}"
    pthread
    m
    dl
)
