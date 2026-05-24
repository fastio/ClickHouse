SELECT name, type, value
FROM system.merge_tree_settings
WHERE name = 'auxiliary_index_preferred_algorithm';
