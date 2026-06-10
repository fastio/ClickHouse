SYSTEM FLUSH LOGS reflection_job_log;

SELECT name, type
FROM system.columns
WHERE database = 'system'
    AND table = 'reflection_job_log'
    AND name IN
    (
        'event_date',
        'event_time',
        'event_type',
        'database',
        'reflection_name',
        'source_database',
        'source_table',
        'task_id',
        'task_kind',
        'task_state',
        'kind',
        'state',
        'input_source_part_uuids',
        'input_source_uuids',
        'input_reflection_part_uuids',
        'output_ann_index_part_uuid',
        'has_output_reflection_part',
        'duration_ms',
        'duration_seconds',
        'settings',
        'profile_events',
        'BuildProfileEvents'
    )
ORDER BY name;
