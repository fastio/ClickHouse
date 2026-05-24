SET allow_experimental_auxiliary_index = 1;

SYSTEM FLUSH LOGS auxiliary_index_log;

SELECT name, type
FROM system.columns
WHERE database = 'system'
    AND table = 'auxiliary_index_log'
    AND name IN
    (
        'task_id',
        'task_kind',
        'input_source_parts',
        'input_auxiliary_index_parts',
        'stage',
        'error_code',
        'error_message',
        'retry_count',
        'next_retry_time'
    )
ORDER BY name;
