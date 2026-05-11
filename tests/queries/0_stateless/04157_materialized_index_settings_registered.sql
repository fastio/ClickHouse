-- Tags: no-fasttest, no-parallel
-- Verify that all five user-facing MaterializedIndex query settings are
-- registered with the expected type and default. A drift in either column on
-- a future release is caught by the locked reference.

SELECT name, type, default
FROM system.settings
WHERE name IN (
    'disable_materialized_index',
    'enable_materialized_index',
    'force_materialized_index',
    'force_using_materialized_index',
    'materialized_index_overfetch_factor'
)
ORDER BY name;
