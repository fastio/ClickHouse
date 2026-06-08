option (ENABLE_SPTAG "Enable Microsoft SPTAG (SPANN) library" ${ENABLE_LIBRARIES})

if (NOT ENABLE_SPTAG)
    message (STATUS "Not using SPTAG")
    return ()
endif ()

# Phase-1 scope: link SPTAG only on Linux x86_64. The library's distance
# kernels are hand-written SSE/AVX/AVX-512 intrinsics with no scalar /
# ARM fallback, and the SPANN disk path is exercised only on Linux. ARM
# support requires a parallel SIMD port and is left out of the first cut.
if (NOT OS_LINUX OR NOT ARCH_AMD64)
    message (STATUS "Not using SPTAG: only Linux x86_64 is supported in this build")
    return ()
endif ()

set (SPTAG_PROJECT_DIR "${ClickHouse_SOURCE_DIR}/contrib/SPTAG")
set (SPTAG_ANN_DIR "${SPTAG_PROJECT_DIR}/AnnService")

# DistanceUtils is compiled as a separate static library because it needs
# -mavx2 / -mavx512 to expose the intrinsics SPTAG calls directly. The
# rest of the library is plain C++17 and inherits the build's default
# instruction set.
set (SPTAG_DISTANCE_SRCS
    "${SPTAG_ANN_DIR}/src/Core/Common/DistanceUtils.cpp"
    "${SPTAG_ANN_DIR}/src/Core/Common/SIMDUtils.cpp"
    "${SPTAG_ANN_DIR}/src/Core/Common/InstructionUtils.cpp"
    "${SPTAG_ANN_DIR}/src/Core/Common/CommonUtils.cpp"
)

add_library (_sptag_distance STATIC ${SPTAG_DISTANCE_SRCS})
target_include_directories (_sptag_distance SYSTEM PUBLIC
    "${SPTAG_PROJECT_DIR}"
    "${SPTAG_ANN_DIR}"
)
# SPTAG was written against C++17, and the libc++ shipped with ClickHouse
# rejects `std::atomic<std::shared_ptr<T>>` (used in SPTAG's Logger) when
# compiled at C++23. Appending `-std=c++17` here overrides the global
# `-std=c++23` because clang takes the last `-std` on the command line.
target_compile_options (_sptag_distance PRIVATE -std=c++17)
target_compile_options (_sptag_distance PRIVATE
    -mavx2 -mavx -msse -msse2
    -mavx512f -mavx512bw -mavx512dq
)
# SPTAG was written against a much older toolchain than ClickHouse uses.
# Silence the noise so it doesn't fight our -Werror posture (the parent
# contrib/CMakeLists.txt also sets -w, but it's worth being explicit).
target_compile_options (_sptag_distance PRIVATE -w)

# Collect Core + Helper sources. Exclude:
#   - Server / Aggregator / Socket / Client / IndexBuilder / IndexSearcher /
#     SSDServing / SPFresh / Quantizer / KeyValueTest / UsefulTool /
#     BalancedDataPartition: command-line front-ends or services we don't link.
#   - ExtraSPDKController.cpp: requires SPDK (off in this build).
#   - DistanceUtils / SIMDUtils / InstructionUtils / CommonUtils: live in
#     the _sptag_distance target above.
set (SPTAG_CORE_SRCS
    "${SPTAG_ANN_DIR}/src/Core/VectorIndex.cpp"
    "${SPTAG_ANN_DIR}/src/Core/VectorSet.cpp"
    "${SPTAG_ANN_DIR}/src/Core/MetadataSet.cpp"
    "${SPTAG_ANN_DIR}/src/Core/MultiIndexScan.cpp"
    "${SPTAG_ANN_DIR}/src/Core/ResultIterator.cpp"
    "${SPTAG_ANN_DIR}/src/Core/SPANNResultIterator.cpp"
    "${SPTAG_ANN_DIR}/src/Core/Common/IQuantizer.cpp"
    "${SPTAG_ANN_DIR}/src/Core/Common/NeighborhoodGraph.cpp"
    "${SPTAG_ANN_DIR}/src/Core/Common/TruthSet.cpp"
    "${SPTAG_ANN_DIR}/src/Core/BKT/BKTIndex.cpp"
    "${SPTAG_ANN_DIR}/src/Core/KDT/KDTIndex.cpp"
    "${SPTAG_ANN_DIR}/src/Core/SPANN/SPANNIndex.cpp"
    "${SPTAG_ANN_DIR}/src/Core/SPANN/ExtraFileController.cpp"
)

set (SPTAG_HELPER_SRCS
    "${SPTAG_ANN_DIR}/src/Helper/ArgumentsParser.cpp"
    "${SPTAG_ANN_DIR}/src/Helper/AsyncFileReader.cpp"
    "${SPTAG_ANN_DIR}/src/Helper/Base64Encode.cpp"
    "${SPTAG_ANN_DIR}/src/Helper/CommonHelper.cpp"
    "${SPTAG_ANN_DIR}/src/Helper/Concurrent.cpp"
    "${SPTAG_ANN_DIR}/src/Helper/DynamicNeighbors.cpp"
    "${SPTAG_ANN_DIR}/src/Helper/SimpleIniReader.cpp"
    "${SPTAG_ANN_DIR}/src/Helper/VectorSetReader.cpp"
    "${SPTAG_ANN_DIR}/src/Helper/VectorSetReaders/DefaultReader.cpp"
    "${SPTAG_ANN_DIR}/src/Helper/VectorSetReaders/TxtReader.cpp"
    "${SPTAG_ANN_DIR}/src/Helper/VectorSetReaders/XvecReader.cpp"
)

add_library (_sptag STATIC ${SPTAG_CORE_SRCS} ${SPTAG_HELPER_SRCS})
target_include_directories (_sptag SYSTEM PUBLIC
    "${SPTAG_PROJECT_DIR}"
    "${SPTAG_ANN_DIR}"
)
target_compile_options (_sptag PRIVATE -std=c++17 -w)

# SPTAG ships its own ThirdParty/zstd, but ClickHouse already builds
# zstd as ch_contrib::zstd. We point SPTAG's `#include "zstd.h"` at that
# build so we don't duplicate symbols.
target_link_libraries (_sptag PUBLIC _sptag_distance ch_contrib::zstd)

add_library (ch_contrib::sptag ALIAS _sptag)
