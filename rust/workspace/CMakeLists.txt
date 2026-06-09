if (OS_FREEBSD)
    # Right nix/libc requires fspacectl and it had been added only since FreeBSD14.
    # And since sysroot has older libraries you will got undefined reference for clickhouse binary.
    #
    # But likely everything should work without this syscall, however it is not
    # possible right now to gently override libraries versions for dependencies,
    # and forking rust modules is a little bit too much for this thing.
    #
    # You can take a look at the details in the following issue [1].
    #
    #   [1]: https://github.com/rust-lang/cargo/issues/5640
    #
    # Update 2024-04: Now prql also requires getrandom() via std::sys::pal::unix::rand::imp::getrandom_fill_bytes
    message(STATUS "Rust build is disabled for FreeBSD because we use old sysroot files")
    return()
endif()

option (ENABLE_PRQL "Enable prql" ${ENABLE_LIBRARIES})
option (ENABLE_SKIM "Enable skim" ${ENABLE_LIBRARIES})
option (ENABLE_POLYGLOT "Enable polyglot SQL transpiler" ${ENABLE_LIBRARIES})
option (ENABLE_DISKANN "Enable diskann" ${ENABLE_LIBRARIES})

if (NOT ENABLE_PRQL AND NOT ENABLE_SKIM AND NOT ENABLE_POLYGLOT AND NOT ENABLE_DISKANN)
    message(STATUS "prql, skim, polyglot, and diskann are disabled")
    return()
endif()

clickhouse_import_crate(MANIFEST_PATH Cargo.toml)

if (ENABLE_PRQL)
    clickhouse_config_crate_flags(_ch_rust_prql)
    target_include_directories(_ch_rust_prql INTERFACE prql/include)
    add_library(ch_rust::prql ALIAS _ch_rust_prql)

    # Strip internal symbols from the PRQL static library after it is built.
    # Only the C FFI functions need to remain visible; all other symbols (67K+)
    # from the chumsky parser combinator are internal and produce enormous names.
    # Skip in tidy builds where CMAKE_AR is a dummy script that doesn't extract archives.
    # Skip in sanitizer builds where instrumentation adds special symbols (e.g.,
    # ___asan_globals_registered) that the partial link + strip process breaks.
    # Skip on Darwin where objects are Mach-O (ld.lld -r only handles ELF).
    # Skip on ppc64le where ld.lld -r produces broken .eh_frame relocations.
    if (NOT CMAKE_AR MATCHES "dummy_compiler_linker" AND NOT SANITIZE AND NOT OS_DARWIN
        AND NOT CMAKE_SYSTEM_PROCESSOR STREQUAL "ppc64le")
        # Use ld.lld for partial linking — it's a cross-linker that handles all ELF architectures,
        # unlike the host ld which may not support the target architecture.
        find_program(_LLD_FOR_STRIP NAMES "ld.lld-${COMPILER_VERSION_MAJOR}" "ld.lld")
        if (_LLD_FOR_STRIP)
            set(_prql_lib "${CMAKE_CURRENT_BINARY_DIR}/lib_ch_rust_prql.a")
            add_custom_target(strip_prql_symbols
                COMMAND bash "${ClickHouse_SOURCE_DIR}/cmake/strip_rust_symbols.sh"
                    "${_prql_lib}" "${CMAKE_AR}" "${CMAKE_OBJCOPY}" "${_LLD_FOR_STRIP}"
                    prql_to_sql prql_free_pointer
                DEPENDS cargo-build__ch_rust_prql
                COMMENT "Stripping internal symbols from PRQL library"
                VERBATIM
            )
            add_dependencies(_ch_rust_prql strip_prql_symbols)
        endif ()
    endif ()
endif()

if (ENABLE_POLYGLOT)
    clickhouse_config_crate_flags(_ch_rust_polyglot)
    target_include_directories(_ch_rust_polyglot INTERFACE polyglot/include)
    add_library(ch_rust::polyglot ALIAS _ch_rust_polyglot)

    if (NOT CMAKE_AR MATCHES "dummy_compiler_linker" AND NOT SANITIZE AND NOT OS_DARWIN
        AND NOT CMAKE_SYSTEM_PROCESSOR STREQUAL "ppc64le")
        find_program(_LLD_FOR_STRIP NAMES "ld.lld-${COMPILER_VERSION_MAJOR}" "ld.lld")
        if (_LLD_FOR_STRIP)
            set(_polyglot_lib "${CMAKE_CURRENT_BINARY_DIR}/lib_ch_rust_polyglot.a")
            add_custom_target(strip_polyglot_symbols
                COMMAND bash "${ClickHouse_SOURCE_DIR}/cmake/strip_rust_symbols.sh"
                    "${_polyglot_lib}" "${CMAKE_AR}" "${CMAKE_OBJCOPY}" "${_LLD_FOR_STRIP}"
                    polyglot_transpile polyglot_free_pointer
                DEPENDS cargo-build__ch_rust_polyglot
                COMMENT "Stripping internal symbols from polyglot library"
                VERBATIM
            )
            add_dependencies(_ch_rust_polyglot strip_polyglot_symbols)
        endif ()
    endif ()
endif()

if (ENABLE_SKIM)
    clickhouse_config_crate_flags(_ch_rust_skim_rust)
    target_include_directories(_ch_rust_skim_rust INTERFACE skim/include)
    add_library(ch_rust::skim ALIAS _ch_rust_skim_rust)
endif()

if (ENABLE_DISKANN)
    clickhouse_config_crate_flags(_ch_rust_diskann_clickhouse)
    target_include_directories(_ch_rust_diskann_clickhouse INTERFACE diskann-clickhouse/include)
    add_library(ch_rust::diskann ALIAS _ch_rust_diskann_clickhouse)

    if (NOT CMAKE_AR MATCHES "dummy_compiler_linker" AND NOT SANITIZE AND NOT OS_DARWIN
        AND NOT CMAKE_SYSTEM_PROCESSOR STREQUAL "ppc64le")
        find_program(_LLD_FOR_STRIP NAMES "ld.lld-${COMPILER_VERSION_MAJOR}" "ld.lld")
        if (_LLD_FOR_STRIP)
            set(_diskann_lib "${CMAKE_CURRENT_BINARY_DIR}/lib_ch_rust_diskann_clickhouse.a")
            add_custom_target(strip_diskann_symbols
                COMMAND bash "${ClickHouse_SOURCE_DIR}/cmake/strip_rust_symbols.sh"
                    "${_diskann_lib}" "${CMAKE_AR}" "${CMAKE_OBJCOPY}" "${_LLD_FOR_STRIP}"
                    diskann_create_disk_builder diskann_drop_builder
                    diskann_builder_set_data_path diskann_builder_set_index_prefix
                    diskann_builder_set_associated_data_path
                    diskann_builder_build diskann_builder_get_status
                    diskann_open_searcher diskann_close_searcher
                    diskann_searcher_num_points diskann_searcher_dimensions
                    diskann_searcher_memory_usage
                    diskann_searcher_payload_record_size
                    diskann_search_disk_index diskann_search_disk_index_with_payload
                    diskann_compute_distances
                    diskann_index_file_exists diskann_last_error
                DEPENDS cargo-build__ch_rust_diskann_clickhouse
                COMMENT "Stripping internal symbols from diskann library"
                VERBATIM
            )
            add_dependencies(_ch_rust_diskann_clickhouse strip_diskann_symbols)
        endif ()
    endif ()
endif()

add_subdirectory (wasmtime)
