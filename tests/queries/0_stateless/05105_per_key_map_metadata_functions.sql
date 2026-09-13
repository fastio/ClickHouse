SELECT extractMapColumn(''), extractMapColumn('0'), extractMapColumn('__M__1.bin'), extractMapColumn('__M__1.bin1');
SELECT extractMapColumn(file) FROM values('file String', '__map__%27key%27.bin', '__map__1.null.bin', 'map.bin', '__missing_separator.bin', '__a%2Eb__1.bin');
SELECT extractMapColumn('x'::FixedString(1)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT extractMapColumn(1); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

CREATE TABLE map_metadata (p UInt32, m Map(String, UInt64), n UInt64)
ENGINE = MergeTree ORDER BY p PARTITION BY p
SETTINGS map_serialization_version = 'with_key_columns', map_serialization_version_for_zero_level_parts = 'with_key_columns', min_rows_for_wide_part = 0, min_bytes_for_wide_part = 0;
SELECT getMapKeys(currentDatabase(), 'map_metadata', 'm');
INSERT INTO map_metadata VALUES (1, {'b': 0, 'a': 1}, 0), (2, {'a': 2, 'c': 3}, 0);
SELECT getMapKeys(currentDatabase(), 'map_metadata', 'm');
SELECT getMapKeys(currentDatabase(), 'map_metadata', 'm', '^1$');
SELECT getMapKeys(currentDatabase(), 'map_metadata', 'm', '^2$', toUInt8(0));
SELECT getMapKeys(currentDatabase(), 'map_metadata', 'm', '^3$');
SELECT getMapKeys(currentDatabase(), 'map_metadata', 'm', '');
SELECT getMapKeys(currentDatabase(), 'map_metadata', 'm') FROM numbers(2);
SELECT getMapKeys(currentDatabase(), 'map_metadata', 'm', '['); -- { serverError CANNOT_COMPILE_REGEXP }
SELECT getMapKeys(currentDatabase(), 'map_metadata', 'n'); -- { serverError BAD_ARGUMENTS }
SELECT getMapKeys('', 'map_metadata', 'm'); -- { serverError BAD_ARGUMENTS }
SELECT getMapKeys(currentDatabase(), 'map_metadata'); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT getMapKeys(currentDatabase(), 'map_metadata', 'm', '', -1); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT getMapKeys(currentDatabase(), 'map_metadata', toString(number)) FROM numbers(1); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
ALTER TABLE map_metadata ADD COLUMN added Map(String, UInt64);
SELECT getMapKeys(currentDatabase(), 'map_metadata', 'added');
ALTER TABLE map_metadata DROP PARTITION 1;
SELECT getMapKeys(currentDatabase(), 'map_metadata', 'm');
DROP TABLE map_metadata;

CREATE TABLE map_metadata_basic (m Map(String, UInt64)) ENGINE = MergeTree ORDER BY tuple()
SETTINGS map_serialization_version = 'basic', map_serialization_version_for_zero_level_parts = 'basic';
SELECT getMapKeys(currentDatabase(), 'map_metadata_basic', 'm'); -- { serverError BAD_ARGUMENTS }
DROP TABLE map_metadata_basic;
