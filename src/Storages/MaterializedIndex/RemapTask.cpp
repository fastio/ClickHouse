#include <Storages/MaterializedIndex/RemapTask.h>

#include <Common/Exception.h>
#include <Common/ProfileEvents.h>
#include <Common/SipHash.h>
#include <Common/logger_useful.h>
#include <Core/UUID.h>
#include <Disks/IDisk.h>
#include <Disks/SingleDiskVolume.h>
#include <IO/ReadBufferFromFileBase.h>
#include <IO/ReadHelpers.h>
#include <IO/ReadSettings.h>
#include <IO/WriteBufferFromFileBase.h>
#include <IO/WriteHelpers.h>
#include <Storages/MaterializedIndex/MergeTreeDataPartMaterializedIndex.h>
#include <Storages/MaterializedIndex/MaterializedIndexPartReverseLookup.h>
#include <Storages/MaterializedIndex/StorageMaterializedIndex.h>
#include <Storages/MergeTree/DataPartStorageOnDiskBase.h>
#include <Storages/MergeTree/DataPartStorageOnDiskFull.h>
#include <Storages/MergeTree/IDataPartStorage.h>
#include <Storages/MergeTree/MergeTreePartInfo.h>

#include <Poco/JSON/Object.h>
#include <Poco/JSON/Parser.h>

#include <fmt/format.h>

#include <filesystem>
#include <future>
#include <utility>


namespace fs = std::filesystem;


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}


/// -----------------------------------------------------------------------------
/// Helpers shared by stages 1-4.
/// -----------------------------------------------------------------------------

namespace
{

/// Read the `part_uuid_dict.bin` of an old materialized-index-part into a vector indexed by
/// `part_uuid_dict_id`. Each on-disk entry is 16 bytes (two little-endian
/// UInt64 halves). An empty dictionary file yields an empty vector.
std::vector<UUID> readPartUuidDict(const IDataPartStorage & storage)
{
    std::vector<UUID> dict;
    if (!storage.existsFile("part_uuid_dict.bin"))
        return dict;

    auto reader = storage.readFile("part_uuid_dict.bin", ReadSettings{}, /*read_hint=*/std::nullopt);
    while (!reader->eof())
    {
        UInt64 hi = 0;
        UInt64 lo = 0;
        readBinaryLittleEndian(hi, *reader);
        readBinaryLittleEndian(lo, *reader);
        UUID uuid;
        UUIDHelpers::getHighBytes(uuid) = hi;
        UUIDHelpers::getLowBytes(uuid) = lo;
        dict.push_back(uuid);
    }
    return dict;
}

/// Scan one `stable_mapping/<seg>.bin` and return `true` as soon as a row's
/// `part_uuid_dict_id` resolves to a UUID present in either delta set. The
/// scan stops at the first hit (affected-segment detection is a boolean
/// classification, not an accumulation).
bool segmentIntersectsDelta(
    const IDataPartStorage & storage,
    size_t segment_index,
    const std::vector<UUID> & dict,
    const std::unordered_set<UUID> & delta_uuids)
{
    const String segment_path = fmt::format("stable_mapping_{}.bin", segment_index);
    if (!storage.existsFile(segment_path))
        return false;
    if (dict.empty() || delta_uuids.empty())
        return false;

    auto reader = storage.readFile(segment_path, ReadSettings{}, std::nullopt);
    while (!reader->eof())
    {
        UInt32 part_uuid_dict_id = 0;
        UInt32 partition_dict_id = 0;
        UInt64 block_number = 0;
        UInt64 block_offset = 0;
        readBinaryLittleEndian(part_uuid_dict_id, *reader);
        readBinaryLittleEndian(partition_dict_id, *reader);
        readBinaryLittleEndian(block_number, *reader);
        readBinaryLittleEndian(block_offset, *reader);

        if (part_uuid_dict_id < dict.size() && delta_uuids.contains(dict[part_uuid_dict_id]))
            return true;
    }
    return false;
}

/// Parse a part name of the form `materialized-index-<partition>_<min>_<max>_<level>` (or
/// with an extra `_<mutation>` suffix), bump `level` by one and rebuild the
/// canonical string.
String bumpLevelInPartName(const String & old_name, MergeTreeDataFormatVersion format_version)
{
    auto info = MergeTreePartInfo::fromPartName(old_name, format_version);
    info.level += 1;
    return info.getPartNameAndCheckFormat(format_version);
}

/// Derive a `tmp_materialized_index_remap_<new_part_name>` storage that lives on the same
/// disk as `source_storage`. The new directory is not created on disk here;
/// stage 2 does that via `createDirectories()`.
MutableDataPartStoragePtr makeRemapTmpStorage(
    const IDataPartStorage & source_storage,
    const MergeTreeData & storage_for_relative_path,
    const String & new_part_name)
{
    const auto & disk_base = dynamic_cast<const DataPartStorageOnDiskBase &>(source_storage);
    const auto disk = disk_base.getDisk();
    const String relative_data_path = storage_for_relative_path.getRelativeDataPath();

    auto volume = std::make_shared<SingleDiskVolume>(
        "remap_tmp_volume_" + new_part_name, disk, /*max_data_part_size=*/0);

    const String tmp_dir_name = String{RemapTask::TEMP_DIRECTORY_PREFIX} + new_part_name;
    return std::make_shared<DataPartStorageOnDiskFull>(std::move(volume), relative_data_path, tmp_dir_name);
}

/// Hardlink every file in `source_storage`'s part root whose name starts
/// with `prefix` into the same flat location in `dest_storage`. MaterializedIndex
/// part directories are flat (no subdirectories), so logical groupings such
/// as `stable_mapping_*`, `mutable_mapping_*` and `algorithm_private_*` are
/// addressed by filename prefix.
void hardlinkOrCopyFilesWithPrefix(
    const IDataPartStorage & source_storage,
    IDataPartStorage & dest_storage,
    const String & prefix,
    LoggerPtr log)
{
    const auto & src_base = dynamic_cast<const DataPartStorageOnDiskBase &>(source_storage);
    const auto disk = src_base.getDisk();
    const String full_part_dir = source_storage.getRelativePath();
    if (!disk->existsDirectory(full_part_dir))
        return;

    dest_storage.createDirectories();

    for (auto it = disk->iterateDirectory(full_part_dir); it->isValid(); it->next())
    {
        const String file_name = fs::path(it->path()).filename();
        if (!file_name.starts_with(prefix))
            continue;
        if (disk->existsDirectory(it->path()))
            continue;
        try
        {
            dest_storage.createHardLinkFrom(source_storage, file_name, file_name);
        }
        catch (...)
        {
            /// Cross-disk (or zero-copy) hardlink failures fall back to
            /// physical copy; log once per failed file rather than throwing
            /// so a single cross-disk part does not abort the whole remap.
            tryLogCurrentException(log, __PRETTY_FUNCTION__);
            dest_storage.copyFileFrom(source_storage, file_name, file_name);
        }
    }
}

/// Hardlink only the flat files (non-directory entries) directly under the
/// part root of `source_storage`. Used for `part_uuid_dict.bin` and similar
/// top-level artifacts.
void hardlinkOrCopyFlatFile(
    const IDataPartStorage & source_storage,
    IDataPartStorage & dest_storage,
    const String & file_name,
    LoggerPtr log)
{
    if (!source_storage.existsFile(file_name))
        return;
    try
    {
        dest_storage.createHardLinkFrom(source_storage, file_name, file_name);
    }
    catch (...)
    {
        tryLogCurrentException(log, __PRETTY_FUNCTION__);
        dest_storage.copyFileFrom(source_storage, file_name, file_name);
    }
}

}


/// -----------------------------------------------------------------------------
/// Pack 1 skeleton stages. Every stage returns false on the first execute()
/// call; the outer driver advances the state machine across all four stages
/// and fulfils the promise with the (empty) new-parts vector. Real data-path
/// implementations land in Pack 2-4.
/// -----------------------------------------------------------------------------

struct RemapTask::PlanAffectedSegmentsStage : public IStage
{
    void setRuntimeContext(StageRuntimeContextPtr, StageRuntimeContextPtr global) override
    {
        global_ctx = std::static_pointer_cast<GlobalRuntimeContext>(global);
    }

    StageRuntimeContextPtr getContextForNextStage() override { return global_ctx; }
    ProfileEvents::Event getTotalTimeProfileEvent() const override { return ProfileEvents::end(); }

    bool execute() override
    {
        auto & ctx = *global_ctx;
        if (ctx.affected_materialized_index_parts.empty())
            return false;
        if (!ctx.storage)
            return false;

        /// Build a single set with every UUID that could flag a segment as
        /// affected. `delta_out` is a raw UUID vector; `delta_in` is a part
        /// vector and we take `part->uuid` for each.
        std::unordered_set<UUID> delta_uuids;
        delta_uuids.reserve(ctx.delta_out_source_uuids.size() + ctx.delta_in_source_parts.size());
        for (const auto & uuid : ctx.delta_out_source_uuids)
            delta_uuids.insert(uuid);
        for (const auto & part : ctx.delta_in_source_parts)
            delta_uuids.insert(part->uuid);

        const auto format_version = ctx.storage->format_version;
        const size_t n = ctx.affected_materialized_index_parts.size();

        ctx.new_materialized_index_parts.resize(n);
        ctx.tmp_storages.reserve(n);
        ctx.affected_seg_ids_per_new_part.assign(n, {});
        ctx.segment_count_per_new_part.assign(n, 0);
        ctx.old_index_per_new_part.resize(n);

        auto log = getLogger("RemapTask");

        for (size_t i = 0; i < n; ++i)
        {
            const auto & old_part = ctx.affected_materialized_index_parts[i];
            if (!old_part)
                continue;

            const auto & old_storage = old_part->getDataPartStorage();

            /// Segment count is recorded in header.json; reading the file
            /// directly keeps the dependency between the Remap path and the
            /// Build on-disk spec localised to one helper.
            size_t segment_count = 0;
            if (old_storage.existsFile("header.json"))
            {
                auto header_reader = old_storage.readFile("header.json", ReadSettings{}, std::nullopt);
                String header_text;
                readStringUntilEOF(header_text, *header_reader);
                try
                {
                    Poco::JSON::Parser parser;
                    auto parsed = parser.parse(header_text);
                    auto obj = parsed.extract<Poco::JSON::Object::Ptr>();
                    if (obj && obj->has("segment_count"))
                        segment_count = obj->getValue<size_t>("segment_count");
                }
                catch (...)
                {
                    tryLogCurrentException(log, __PRETTY_FUNCTION__);
                }
            }
            ctx.segment_count_per_new_part[i] = segment_count;

            /// Scan every segment's stable_mapping to classify it as affected.
            /// Short-circuit: `segmentIntersectsDelta` returns as soon as it
            /// sees one delta-referencing row (sampling is implicit — no
            /// need to scan the whole segment once classification is set).
            if (!delta_uuids.empty())
            {
                const auto dict = readPartUuidDict(old_storage);
                for (size_t seg = 0; seg < segment_count; ++seg)
                {
                    if (segmentIntersectsDelta(old_storage, seg, dict, delta_uuids))
                        ctx.affected_seg_ids_per_new_part[i].insert(seg);
                }
            }

            /// N=M derivation: one new materialized-index-part per old, `level` bumped by one.
            const String new_part_name = bumpLevelInPartName(old_part->name, format_version);
            const auto new_part_info = MergeTreePartInfo::fromPartName(new_part_name, format_version);

            auto new_tmp_storage = makeRemapTmpStorage(old_storage, *ctx.storage, new_part_name);
            ctx.tmp_storages[new_part_name] = new_tmp_storage;

            auto settings = ctx.storage->getSettings();
            auto new_part = std::make_shared<MergeTreeDataPartMaterializedIndex>(
                *ctx.storage,
                *settings,
                new_part_name,
                new_part_info,
                new_tmp_storage,
                /*parent_part_=*/nullptr);
            new_part->is_temp = true;
            ctx.new_materialized_index_parts[i] = std::move(new_part);
            ctx.old_index_per_new_part[i] = i;
        }

        return false;
    }

    void cancel() noexcept override {}

    GlobalRuntimeContextPtr global_ctx;
};


struct RemapTask::DeriveHardlinksStage : public IStage
{
    void setRuntimeContext(StageRuntimeContextPtr, StageRuntimeContextPtr global) override
    {
        global_ctx = std::static_pointer_cast<GlobalRuntimeContext>(global);
    }

    StageRuntimeContextPtr getContextForNextStage() override { return global_ctx; }
    ProfileEvents::Event getTotalTimeProfileEvent() const override { return ProfileEvents::end(); }

    bool execute() override
    {
        auto & ctx = *global_ctx;
        if (ctx.stage2_cursor >= ctx.new_materialized_index_parts.size())
            return false;
        if (ctx.is_cancelled.load(std::memory_order_relaxed))
            return false;

        const size_t i = ctx.stage2_cursor;
        const auto & new_part = ctx.new_materialized_index_parts[i];
        if (!new_part)
        {
            ++ctx.stage2_cursor;
            return ctx.stage2_cursor < ctx.new_materialized_index_parts.size();
        }

        const size_t old_idx = ctx.old_index_per_new_part[i];
        const auto & old_part = ctx.affected_materialized_index_parts[old_idx];
        const auto & old_storage = old_part->getDataPartStorage();
        auto & dest_storage = new_part->getDataPartStorage();

        auto log = getLogger("RemapTask");

        /// algorithm_private_* files are opaque to the framework — every
        /// such file is hardlinked as-is; stable_mapping_* is immutable
        /// across Remap and therefore also full-hardlink.
        hardlinkOrCopyFilesWithPrefix(old_storage, dest_storage, "algorithm_private_", log);
        hardlinkOrCopyFilesWithPrefix(old_storage, dest_storage, "stable_mapping_", log);

        /// mutable_mapping/<seg>.bin: hardlink only the segments that are NOT
        /// in the affected set; stage 3 will rewrite the affected ones.
        const size_t segment_count = ctx.segment_count_per_new_part[i];
        const auto & affected = ctx.affected_seg_ids_per_new_part[i];
        if (segment_count > 0)
        {
            dest_storage.createDirectories();
            for (size_t seg = 0; seg < segment_count; ++seg)
            {
                if (affected.contains(seg))
                    continue;
                const String rel = fmt::format("mutable_mapping_{}.bin", seg);
                if (!old_storage.existsFile(rel))
                    continue;
                try
                {
                    dest_storage.createHardLinkFrom(old_storage, rel, rel);
                }
                catch (...)
                {
                    tryLogCurrentException(log, __PRETTY_FUNCTION__);
                    dest_storage.copyFileFrom(old_storage, rel, rel);
                }
            }
        }

        /// Top-level dictionaries are immutable as long as no new UUIDs /
        /// partition ids have to be appended; stage 3 reopens them in
        /// append mode for Rewrite work. Hardlinking at this point lets
        /// the common no-rewrite case finish with zero writes.
        hardlinkOrCopyFlatFile(old_storage, dest_storage, "part_uuid_dict.bin", log);
        hardlinkOrCopyFlatFile(old_storage, dest_storage, "partition_dict.bin", log);

        ++ctx.stage2_cursor;
        return ctx.stage2_cursor < ctx.new_materialized_index_parts.size();
    }

    void cancel() noexcept override {}

    GlobalRuntimeContextPtr global_ctx;
};


struct RemapTask::RewriteMutableSegmentsStage : public IStage
{
    void setRuntimeContext(StageRuntimeContextPtr, StageRuntimeContextPtr global) override
    {
        global_ctx = std::static_pointer_cast<GlobalRuntimeContext>(global);
    }

    StageRuntimeContextPtr getContextForNextStage() override { return global_ctx; }
    ProfileEvents::Event getTotalTimeProfileEvent() const override { return ProfileEvents::end(); }

    bool execute() override
    {
        auto & ctx = *global_ctx;

        if (ctx.new_materialized_index_parts.empty())
            return false;

        /// Advance the part/segment cursor.
        if (cursor_initialised)
        {
            ++segment_cursor;
        }
        else
        {
            cursor_initialised = true;
            part_cursor = 0;
            segment_cursor = 0;
        }

        while (true)
        {
            if (part_cursor >= ctx.new_materialized_index_parts.size())
                return false;
            if (ctx.is_cancelled.load(std::memory_order_relaxed))
                return false;

            const auto & affected = ctx.affected_seg_ids_per_new_part[part_cursor];
            while (segment_cursor < ctx.segment_count_per_new_part[part_cursor]
                && !affected.contains(segment_cursor))
                ++segment_cursor;

            if (segment_cursor >= ctx.segment_count_per_new_part[part_cursor])
            {
                ++part_cursor;
                segment_cursor = 0;
                continue;
            }
            break;
        }

        /// At this point (part_cursor, segment_cursor) points to an affected
        /// segment that needs rewriting.
        const size_t i = part_cursor;
        const auto & new_part = ctx.new_materialized_index_parts[i];
        if (!new_part)
        {
            /// Force advancement to the next part on the next invocation.
            ++part_cursor;
            segment_cursor = 0;
            cursor_initialised = false;
            return part_cursor < ctx.new_materialized_index_parts.size();
        }

        const size_t old_idx = ctx.old_index_per_new_part[i];
        const auto & old_part = ctx.affected_materialized_index_parts[old_idx];
        const auto & old_storage = old_part->getDataPartStorage();
        auto & dest_storage = new_part->getDataPartStorage();

        auto log = getLogger("RemapTask");

        /// Stable layer schema is `(part_uuid_dict_id, partition_dict_id,
        /// _block_number, _block_offset)`. We consult the old dictionary once
        /// up-front to classify each entry's source UUID as "in the outgoing
        /// delta" (-> reserved tombstone id) or "still live" (-> preserve the source
        /// `_part_offset` recorded at Build time by copying the corresponding
        /// row of the old `mutable_mapping` segment verbatim).
        const std::vector<UUID> dict = readPartUuidDict(old_storage);
        std::unordered_set<UUID> delta_out_set(
            ctx.delta_out_source_uuids.begin(), ctx.delta_out_source_uuids.end());

        const String stable_rel = fmt::format("stable_mapping_{}.bin", segment_cursor);
        const String mutable_rel = fmt::format("mutable_mapping_{}.bin", segment_cursor);

        if (!old_storage.existsFile(stable_rel))
        {
            /// Nothing to rewrite; advance and continue on next call.
            return true;
        }

        auto stable_reader = old_storage.readFile(stable_rel, ReadSettings{}, std::nullopt);
        auto mutable_reader = old_storage.readFile(mutable_rel, ReadSettings{}, std::nullopt);
        auto writer = dest_storage.writeFile(mutable_rel, 4096, WriteSettings{});

        /// `internal_id` is implicit: rows are appended in exactly the same
        /// order stable_mapping stores them, which is the canonical build-time
        /// ordering within a segment.
        UInt64 internal_id = 0;
        size_t tombstones = 0;
        size_t survivors = 0;
        while (!stable_reader->eof())
        {
            if ((internal_id & 0xFF) == 0
                && ctx.is_cancelled.load(std::memory_order_relaxed))
            {
                /// Cooperative cancel: finalize whatever has been written so
                /// the caller can observe partial progress via iterate().
                writer->finalize();
                return false;
            }

            UInt32 part_uuid_dict_id = 0;
            UInt32 partition_dict_id = 0;
            UInt64 block_number = 0;
            UInt64 block_offset = 0;
            readBinaryLittleEndian(part_uuid_dict_id, *stable_reader);
            readBinaryLittleEndian(partition_dict_id, *stable_reader);
            readBinaryLittleEndian(block_number, *stable_reader);
            readBinaryLittleEndian(block_offset, *stable_reader);

            auto old_locator = MaterializedIndexPartReverseLookup::readLocatorEntry(*mutable_reader);

            const bool outgoing = part_uuid_dict_id < dict.size()
                && delta_out_set.contains(dict[part_uuid_dict_id]);

            MaterializedIndexPartReverseLookup::LocatorEntry out_locator;
            if (outgoing)
            {
                out_locator = MaterializedIndexPartReverseLookup::tombstoneLocatorEntry();
                ++tombstones;
            }
            else
            {
                out_locator = old_locator;
                ++survivors;
            }

            MaterializedIndexPartReverseLookup::writeLocatorEntry(out_locator, *writer);

            ++internal_id;
        }
        writer->finalize();

        if (tombstones == 0 && survivors == internal_id)
            LOG_TRACE(log, "Rewrote segment {} for part {}: {} live rows, 0 tombstones",
                segment_cursor, new_part->name, survivors);
        else
            LOG_DEBUG(log, "Rewrote segment {} for part {}: {} live rows, {} tombstones",
                segment_cursor, new_part->name, survivors, tombstones);

        return true;
    }

    void cancel() noexcept override {}

    GlobalRuntimeContextPtr global_ctx;
    bool cursor_initialised{false};
    size_t part_cursor{0};
    size_t segment_cursor{0};
};


struct RemapTask::FinalizeMetadataStage : public IStage
{
    void setRuntimeContext(StageRuntimeContextPtr, StageRuntimeContextPtr global) override
    {
        global_ctx = std::static_pointer_cast<GlobalRuntimeContext>(global);
    }

    StageRuntimeContextPtr getContextForNextStage() override { return global_ctx; }
    ProfileEvents::Event getTotalTimeProfileEvent() const override { return ProfileEvents::end(); }

    bool execute() override
    {
        auto & ctx = *global_ctx;
        if (ctx.new_materialized_index_parts.empty())
            return false;

        std::unordered_set<UUID> delta_out_set(
            ctx.delta_out_source_uuids.begin(), ctx.delta_out_source_uuids.end());

        for (size_t i = 0; i < ctx.new_materialized_index_parts.size(); ++i)
        {
            const auto & new_part = ctx.new_materialized_index_parts[i];
            if (!new_part)
                continue;

            const size_t old_idx = ctx.old_index_per_new_part[i];
            const auto & old_part = ctx.affected_materialized_index_parts[old_idx];
            const auto & old_storage = old_part->getDataPartStorage();
            auto & dest_storage = new_part->getDataPartStorage();

            SyncGuardPtr sync_guard = dest_storage.getDirectorySyncGuard();

            writeHeaderJson(old_storage, dest_storage, old_part->uuid, delta_out_set);
            writeCoverageJson(old_storage, dest_storage, delta_out_set);
            writeChecksumTxt(dest_storage, ctx.segment_count_per_new_part[i]);
            writeTxnVersionTxt(dest_storage);

            /// sync_guard dtor here fsyncs the directory on scope exit.
        }

        return false;
    }

    void cancel() noexcept override {}

    static void writeHeaderJson(
        const IDataPartStorage & old_storage,
        IDataPartStorage & dest_storage,
        const UUID & derive_from,
        const std::unordered_set<UUID> & delta_out_set)
    {
        /// Read the old header, copy forward the stable fields, and
        /// recompute `coverage_source_part_count` by counting `coverage.json`
        /// entries that are NOT in the outgoing delta set. Absence of an old
        /// header reduces the new header to an empty-defaults document; the
        /// Build-time schema version is preserved.
        Poco::JSON::Object header_json;
        header_json.set("version", 1);

        String algorithm_family;
        String algorithm_impl;
        UInt64 total_rows = 0;
        size_t segment_count = 0;
        Poco::JSON::Array segment_boundaries_arr;

        if (old_storage.existsFile("header.json"))
        {
            auto header_reader = old_storage.readFile("header.json", ReadSettings{}, std::nullopt);
            String header_text;
            readStringUntilEOF(header_text, *header_reader);
            try
            {
                Poco::JSON::Parser parser;
                auto parsed = parser.parse(header_text);
                auto obj = parsed.extract<Poco::JSON::Object::Ptr>();
                if (obj)
                {
                    if (obj->has("algorithm_family"))
                        algorithm_family = obj->getValue<std::string>("algorithm_family");
                    if (obj->has("algorithm_impl"))
                        algorithm_impl = obj->getValue<std::string>("algorithm_impl");
                    if (obj->has("total_rows"))
                        total_rows = obj->getValue<UInt64>("total_rows");
                    if (obj->has("segment_count"))
                        segment_count = obj->getValue<size_t>("segment_count");
                    if (obj->has("segment_boundaries"))
                    {
                        auto arr = obj->getArray("segment_boundaries");
                        if (arr)
                        {
                            for (size_t j = 0; j < arr->size(); ++j)
                                segment_boundaries_arr.add(arr->getElement<UInt64>(static_cast<unsigned int>(j)));
                        }
                    }
                }
            }
            catch (...)
            {
                tryLogCurrentException(__PRETTY_FUNCTION__);
            }
        }

        header_json.set("algorithm_family", algorithm_family);
        header_json.set("algorithm_impl", algorithm_impl);
        header_json.set("total_rows", total_rows);
        MaterializedIndexPartReverseLookup::addLocatorHeaderFields(header_json);
        header_json.set("segment_count", segment_count);
        header_json.set("segment_boundaries", segment_boundaries_arr);

        /// `coverage_source_part_count` is the old count minus outgoing.
        /// delta_in source parts are intentionally NOT added to coverage
        /// because a new materialized-index-part is only "full-covered" for source parts it
        /// indexes in their entirety; delta_in rows only update mutable_mapping.
        size_t new_coverage = 0;
        if (old_storage.existsFile("coverage.json"))
        {
            auto cov_reader = old_storage.readFile("coverage.json", ReadSettings{}, std::nullopt);
            String body;
            readStringUntilEOF(body, *cov_reader);
            try
            {
                Poco::JSON::Parser cov_parser;
                auto parsed_cov = cov_parser.parse(body);
                auto cov_obj = parsed_cov.extract<Poco::JSON::Object::Ptr>();
                if (cov_obj)
                {
                    auto covered_arr = cov_obj->getArray("covered");
                    if (covered_arr)
                    {
                        for (size_t j = 0; j < covered_arr->size(); ++j)
                        {
                            auto item = covered_arr->getObject(static_cast<unsigned int>(j));
                            if (!item || !item->has("source_part_uuid"))
                                continue;
                            UUID u;
                            if (tryParse(u, item->getValue<std::string>("source_part_uuid"))
                                && !delta_out_set.contains(u))
                                ++new_coverage;
                        }
                    }
                }
            }
            catch (...)
            {
                /// Malformed JSON — keep going; a corrupt old coverage file
                /// should not abort the whole Remap.
                tryLogCurrentException(__PRETTY_FUNCTION__);
            }
        }
        header_json.set("coverage_source_part_count", new_coverage);
        header_json.set("created_timestamp_seconds", static_cast<Int64>(std::time(nullptr)));
        header_json.set("derive_from", toString(derive_from));

        auto writer = dest_storage.writeFile("header.json", 4096, WriteSettings{});
        std::ostringstream oss;
        Poco::JSON::Stringifier::stringify(header_json, oss);
        const std::string body = oss.str();
        writer->write(body.data(), body.size());
        writer->finalize();
    }

    static void writeCoverageJson(
        const IDataPartStorage & old_storage,
        IDataPartStorage & dest_storage,
        const std::unordered_set<UUID> & delta_out_set)
    {
        Poco::JSON::Object coverage_json;
        coverage_json.set("format_version", 1);

        Poco::JSON::Array covered_arr;
        if (old_storage.existsFile("coverage.json"))
        {
            auto cov_reader = old_storage.readFile("coverage.json", ReadSettings{}, std::nullopt);
            String body;
            readStringUntilEOF(body, *cov_reader);
            try
            {
                Poco::JSON::Parser parser;
                auto parsed = parser.parse(body);
                auto root = parsed.extract<Poco::JSON::Object::Ptr>();
                if (root)
                {
                    auto old_covered = root->getArray("covered");
                    if (old_covered)
                    {
                        for (size_t j = 0; j < old_covered->size(); ++j)
                        {
                            auto item = old_covered->getObject(static_cast<unsigned int>(j));
                            if (!item || !item->has("source_part_uuid"))
                                continue;
                            UUID u;
                            if (!tryParse(u, item->getValue<std::string>("source_part_uuid")))
                                continue;
                            if (delta_out_set.contains(u))
                                continue;
                            covered_arr.add(item);
                        }
                    }
                }
            }
            catch (...)
            {
                /// Malformed old JSON: drop coverage rather than abort the
                /// whole Remap. Symmetric with the header-recompute path.
                tryLogCurrentException(__PRETTY_FUNCTION__);
            }
        }
        coverage_json.set("covered", covered_arr);

        auto writer = dest_storage.writeFile("coverage.json", 4096, WriteSettings{});
        std::ostringstream oss;
        Poco::JSON::Stringifier::stringify(coverage_json, oss);
        const std::string body = oss.str();
        writer->write(body.data(), body.size());
        writer->finalize();
    }

    static void writeChecksumTxt(IDataPartStorage & dest_storage, size_t segment_count)
    {
        std::vector<String> data_files;
        for (size_t s = 0; s < segment_count; ++s)
        {
            data_files.push_back(fmt::format("stable_mapping_{}.bin", s));
            data_files.push_back(fmt::format("mutable_mapping_{}.bin", s));
        }
        data_files.push_back("part_uuid_dict.bin");
        data_files.push_back("partition_dict.bin");
        std::sort(data_files.begin(), data_files.end());

        auto writer = dest_storage.writeFile("checksum.txt", 4096, WriteSettings{});
        for (const auto & rel_path : data_files)
        {
            if (!dest_storage.existsFile(rel_path))
                continue;

            SipHash hasher;
            auto reader = dest_storage.readFile(rel_path, ReadSettings{}, std::nullopt);
            constexpr size_t chunk_size = 4096;
            std::array<char, chunk_size> buffer{};
            while (!reader->eof())
            {
                const size_t n = reader->readBig(buffer.data(), buffer.size());
                if (n == 0)
                    break;
                hasher.update(buffer.data(), n);
            }
            const UInt128 digest = hasher.get128();
            const UInt64 lo = digest.items[UInt128::_impl::little(0)];
            const UInt64 hi = digest.items[UInt128::_impl::little(1)];
            const String line = fmt::format("{:016x}{:016x} {}\n", lo, hi, rel_path);
            writer->write(line.data(), line.size());
        }
        writer->finalize();
    }

    static void writeTxnVersionTxt(IDataPartStorage & dest_storage)
    {
        auto writer = dest_storage.writeFile("txn_version.txt", 4096, WriteSettings{});
        const std::string_view payload{"0\n"};
        writer->write(payload.data(), payload.size());
        writer->finalize();
    }

    GlobalRuntimeContextPtr global_ctx;
};


RemapTask::Stages RemapTask::makeStages()
{
    return {
        std::make_shared<PlanAffectedSegmentsStage>(),
        std::make_shared<DeriveHardlinksStage>(),
        std::make_shared<RewriteMutableSegmentsStage>(),
        std::make_shared<FinalizeMetadataStage>(),
    };
}


RemapTask::RemapTask(
    MergeTreeData::DataPartsVector affected_materialized_index_parts_,
    MergeTreeData::DataPartsVector delta_in_source_parts_,
    std::vector<UUID> delta_out_source_uuids_,
    StorageMaterializedIndex * storage_,
    const MergeTreeData * source_storage_,
    StorageSnapshotPtr source_snapshot_,
    ContextPtr context_,
    UInt64 memory_budget_bytes_)
    : global_ctx(std::make_shared<GlobalRuntimeContext>())
    , stages(makeStages())
{
    global_ctx->affected_materialized_index_parts = std::move(affected_materialized_index_parts_);
    global_ctx->delta_in_source_parts = std::move(delta_in_source_parts_);
    global_ctx->delta_out_source_uuids = std::move(delta_out_source_uuids_);
    global_ctx->storage = storage_;
    global_ctx->source_storage = source_storage_;
    global_ctx->source_snapshot = std::move(source_snapshot_);
    global_ctx->context = std::move(context_);
    global_ctx->memory_budget_bytes = memory_budget_bytes_;

    stages_iterator = stages.begin();

    auto prepare_stage_ctx = std::make_shared<IStageRuntimeContext>();
    (*stages_iterator)->setRuntimeContext(std::move(prepare_stage_ctx), global_ctx);
}


RemapTask::~RemapTask()
{
    /// Best-effort: if the task is destroyed before the promise was fulfilled
    /// (cancellation, exception during stage construction) wake up any waiter
    /// on getFuture().get() so the caller does not block indefinitely.
    if (promise_fulfilled)
        return;

    try
    {
        global_ctx->promise.set_exception(std::make_exception_ptr(
            Exception(ErrorCodes::LOGICAL_ERROR, "RemapTask destroyed before completion")));
    }
    catch (const std::future_error &)
    {
        /// Promise may have been satisfied on a racing code path; swallow to
        /// keep the destructor noexcept-friendly.
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}


bool RemapTask::execute()
try
{
    chassert(stages_iterator != stages.end());
    const auto & current_stage = *stages_iterator;

    if (current_stage->execute())
        return true;

    auto next_stage_context = current_stage->getContextForNextStage();

    ++stages_iterator;
    if (stages_iterator == stages.end())
    {
        /// All stages have completed. Fulfil the promise exactly once with
        /// the vector produced by stage 4 (empty while stages 1-4 are still
        /// skeletons; that is intentional for early change packs).
        if (!promise_fulfilled)
        {
            global_ctx->promise.set_value(std::move(global_ctx->new_materialized_index_parts));
            promise_fulfilled = true;
        }
        return false;
    }

    (*stages_iterator)->setRuntimeContext(std::move(next_stage_context), global_ctx);
    return true;
}
catch (...)
{
    /// Propagate the exception to any thread waiting on getFuture().get()
    /// and rethrow so the top-level scheduler can record the failure.
    if (!promise_fulfilled)
    {
        global_ctx->promise.set_exception(std::current_exception());
        promise_fulfilled = true;
    }
    throw;
}


void RemapTask::cancel() noexcept
{
    global_ctx->is_cancelled.store(true, std::memory_order_relaxed);
    if (stages_iterator != stages.end())
        (*stages_iterator)->cancel();
}


std::future<std::vector<MergeTreeData::MutableDataPartPtr>> RemapTask::getFuture()
{
    return global_ctx->promise.get_future();
}


std::vector<MergeTreeData::MutableDataPartPtr> RemapTask::getUnfinishedParts()
{
    return global_ctx->new_materialized_index_parts;
}

}
