-- Tags: no-fasttest, no-parallel
-- Verify that all five user-facing ANNIndex query settings are
-- registered with the expected type and default. A drift in either column on
-- a future release is caught by the locked reference.

SELECT name, type, default
FROM system.settings
WHERE name IN (
    'disable_ann_index',
    'enable_ann_index',
    'force_ann_index',
    'force_using_ann_index',
    'ann_index_overfetch_factor'
)
ORDER BY name;
