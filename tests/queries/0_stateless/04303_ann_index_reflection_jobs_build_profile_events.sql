-- Tags: no-fasttest
-- Guards the `system.reflection_jobs` DiskANN build-progress columns and
-- `BuildProfileEvents` aliases against accidental schema drift.

SELECT name, type
FROM system.columns
WHERE database = 'system'
    AND table = 'reflection_jobs'
    AND name IN
    (
        'next_stage',
        'profile_events',
        'profile_events.Names',
        'profile_events.Values',
        'stage_progress',
        'stage_progress_total',
        'build_next_stage',
        'build_stage_progress',
        'build_stage_progress_total',
        'BuildProfileEvents',
        'BuildProfileEvents.Names',
        'BuildProfileEvents.Values'
    )
ORDER BY name;
