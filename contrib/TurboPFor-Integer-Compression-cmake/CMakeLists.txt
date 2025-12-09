option (ENABLE_TURBOPFOR "Enable TurboPFor" ${ENABLE_LIBRARIES})

if (NOT ENABLE_TURBOPFOR)
    message(STATUS "Not using TurboPFor")
    return()
endif()

set (LIBRARY_DIR "${ClickHouse_SOURCE_DIR}/contrib/TurboPFor-Integer-Compression")

set (SRCS
    "${LIBRARY_DIR}/lib/bic.c"
    "${LIBRARY_DIR}/lib/bitpack.c"
    "${LIBRARY_DIR}/lib/bitunpack.c"
    "${LIBRARY_DIR}/lib/bitutil.c"
    "${LIBRARY_DIR}/lib/eliasfano.c"
    "${LIBRARY_DIR}/lib/fp.c"
    "${LIBRARY_DIR}/lib/icapp.c"
    "${LIBRARY_DIR}/lib/iccodec.c"
    "${LIBRARY_DIR}/lib/idxcr.c"
    "${LIBRARY_DIR}/lib/idxqry.c"
    "${LIBRARY_DIR}/lib/idxseg.c"
    "${LIBRARY_DIR}/lib/transpose_.c"
    "${LIBRARY_DIR}/lib/transpose.c"
    "${LIBRARY_DIR}/lib/trlec.c"
    "${LIBRARY_DIR}/lib/trled.c"
    "${LIBRARY_DIR}/lib/v8.c"
    "${LIBRARY_DIR}/lib/v8pack.c"
    "${LIBRARY_DIR}/lib/vbit.c"
    "${LIBRARY_DIR}/lib/vint.c"
    "${LIBRARY_DIR}/lib/vp4c.c"
    "${LIBRARY_DIR}/lib/vp4d.c"
    "${LIBRARY_DIR}/lib/vsimple.c"
)

add_library (_turbopfor ${SRCS})

target_include_directories (_turbopfor SYSTEM PUBLIC
    "${LIBRARY_DIR}/include"
    "${LIBRARY_DIR}/lib"
)

if (OMIT_HEAVY_DEBUG_SYMBOLS)
    target_compile_options (_turbopfor PRIVATE -g0)
endif()

target_compile_options(_turbopfor PRIVATE
        -Wno-unused-parameter
        -Wno-sign-compare
        -Wno-missing-field-initializers
        -Wno-unused-function
        -Wno-unused-variable
        -Wno-implicit-fallthrough
        -Wno-implicit-function-declaration
)

add_library (ch_contrib::turbopfor ALIAS _turbopfor)
