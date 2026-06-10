SYSTEM FLUSH LOGS reflection_job_log;

SELECT name, type
FROM system.columns
WHERE database = 'system'
    AND table = 'reflection_job_log'
    AND name IN
    (
        'database',
        'reflection_name',
        'task_id',
        'kind',
        'state',
        'input_source_uuids',
        'output_ann_index_part_uuid',
        'duration_seconds',
        'settings',
        'BuildProfileEvents'
    )
ORDER BY name;
