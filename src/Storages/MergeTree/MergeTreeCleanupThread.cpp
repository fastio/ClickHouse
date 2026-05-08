#include <Storages/MergeTree/MergeTreeCleanupThread.h>

#include <Common/ZooKeeper/ZooKeeperCommon.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeSettings.h>

namespace DB
{

namespace MergeTreeSetting
{
    extern const MergeTreeSettingsSeconds lock_acquire_timeout_for_background_operations;
    extern const MergeTreeSettingsUInt64 merge_tree_clear_old_parts_interval_seconds;
    extern const MergeTreeSettingsUInt64 merge_tree_clear_old_temporary_directories_interval_seconds;
    extern const MergeTreeSettingsSeconds temporary_directories_lifetime;
}

MergeTreeCleanupThread::MergeTreeCleanupThread(MergeTreeData & data_)
    : IMergeTreeCleanupThread(data_)
{
}

void MergeTreeCleanupThread::start()
{
    time_after_previous_cleanup_parts.restart();
    time_after_previous_cleanup_temporary_directories.restart();
    IMergeTreeCleanupThread::start();
}

Float32 MergeTreeCleanupThread::iterate()
{
    auto component_guard = Coordination::setCurrentComponent("MergeTreeCleanupThread");
    size_t cleaned_other = 0;
    size_t cleaned_part_like = 0;
    size_t cleaned_parts = 0;

    auto storage_settings = data.getSettings();

    auto shared_lock
        = data.lockForShare(RWLockImpl::NO_QUERY, (*storage_settings)[MergeTreeSetting::lock_acquire_timeout_for_background_operations]);
    if (auto lock = time_after_previous_cleanup_temporary_directories.compareAndRestartDeferred(
            static_cast<double>((*storage_settings)[MergeTreeSetting::merge_tree_clear_old_temporary_directories_interval_seconds])))
    {
        /// Both use relative_data_path which changes during rename, so we do it under share lock
        cleaned_part_like += data.clearOldTemporaryDirectories(
            (*data.getSettings())[MergeTreeSetting::temporary_directories_lifetime].totalSeconds());
    }

    if (auto lock = time_after_previous_cleanup_parts.compareAndRestartDeferred(
            static_cast<double>((*storage_settings)[MergeTreeSetting::merge_tree_clear_old_parts_interval_seconds])))
    {
        cleaned_parts += data.clearOldPartsFromFilesystem(/* force */ false, /* with_pause_fail_point */ true);
        cleaned_other += data.clearOldMutations(/* truncate */ false);
        cleaned_part_like += data.clearEmptyParts();
        cleaned_part_like += data.clearUnusedPatchParts();
        cleaned_part_like += data.unloadPrimaryKeysAndClearCachesOfOutdatedParts();
    }

    constexpr Float32 parts_number_amplification = 1.3f; /// Assuming we merge 4-5 parts each time
    Float32 cleaned_inserted_parts = static_cast<Float32>(cleaned_parts) / parts_number_amplification;
    return cleaned_inserted_parts + static_cast<Float32>(cleaned_part_like) + static_cast<Float32>(cleaned_other);
}

}
