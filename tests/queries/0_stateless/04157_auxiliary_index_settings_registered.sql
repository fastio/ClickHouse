-- Tags: no-fasttest, no-parallel
-- Verify that all five user-facing AuxiliaryIndex query settings are
-- registered with the expected type and default. A drift in either column on
-- a future release is caught by the locked reference.

SELECT name, type, default
FROM system.settings
WHERE name IN (
    'disable_auxiliary_index',
    'enable_auxiliary_index',
    'force_auxiliary_index',
    'force_using_auxiliary_index',
    'auxiliary_index_overfetch_factor'
)
ORDER BY name;
