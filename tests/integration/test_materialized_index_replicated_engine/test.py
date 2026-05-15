"""
Integration tests for the `ReplicatedMaterializedIndex` engine itself
(as opposed to a MaterializedIndex inside a `Replicated` database, which
is covered by `test_materialized_index_replicated`).

The scenarios here exist only in a true multi-node cluster:

  * Single-builder semantics: only one replica holds the leader lease,
    so only one replica produces a `Build` event; the other replica
    receives the produced inner part via the `GET_PART` log entry that
    `commitReplacingPartFromBackgroundTask` enqueues.

  * Leader failure mid-build: the leader's session expires (or it
    crashes) while it holds the lease and is paused before commit;
    the surviving replica must take over the build through the
    `materialized_index_build_pause_in_finish` PAUSEABLE_ONCE failpoint.

The single-node DDL shape is covered by 04187; the single-node build
correctness is covered by 04188; the single-node commit-retry path is
covered by 04189. None of those can exercise the two cases above.
"""

import logging
import time
import uuid as uuidlib

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

CONFIG = ["configs/config.xml"]
USER_CONFIG = ["configs/settings.xml"]

node1 = cluster.add_instance(
    "node1",
    main_configs=CONFIG,
    user_configs=USER_CONFIG,
    with_zookeeper=True,
    stay_alive=True,
    macros={"shard": "01", "replica": "r1"},
)
node2 = cluster.add_instance(
    "node2",
    main_configs=CONFIG,
    user_configs=USER_CONFIG,
    with_zookeeper=True,
    stay_alive=True,
    macros={"shard": "01", "replica": "r2"},
)

all_nodes = [node1, node2]


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def _unique(prefix):
    """Return a short unique suffix so concurrent test runs do not collide
    on ZK paths across pytest workers."""
    return f"{prefix}_{uuidlib.uuid4().hex[:8]}"


def _drop_tables(suffix):
    for node in all_nodes:
        node.query(f"DROP TABLE IF EXISTS mi_{suffix} SYNC")
        node.query(f"DROP TABLE IF EXISTS src_{suffix} SYNC")


def _create_source(suffix):
    for node in all_nodes:
        node.query(
            f"CREATE TABLE src_{suffix} (k UInt64, embedding Array(Float32)) "
            f"ENGINE = ReplicatedMergeTree("
            f"  '/clickhouse/tables/{suffix}/src', '{{replica}}') "
            f"ORDER BY k "
            f"SETTINGS assign_part_uuids = 1, enable_block_number_column = 1, "
            f"enable_block_offset_column = 1"
        )


def _create_mi(suffix, sync_timeout=60):
    for node in all_nodes:
        node.query(
            f"CREATE MATERIALIZED INDEX mi_{suffix} "
            f"ON src_{suffix} (embedding) "
            f"TYPE ann('diskann', metric = 'L2', dim = 4) "
            f"ENGINE = ReplicatedMaterializedIndex("
            f"  '/clickhouse/tables/{suffix}/mi', '{{replica}}') "
            f"SETTINGS materialized_index_sync_timeout = {sync_timeout}"
        )


def _build_finish_count(node, suffix):
    """Count `BuildFinish` events in `system.materialized_index_log` for
    the named materialized index on this replica only. Each replica has
    its own log, so this is the local build count."""
    node.query("SYSTEM FLUSH LOGS materialized_index_log")
    return int(
        node.query(
            f"SELECT count() FROM system.materialized_index_log "
            f"WHERE index_name = 'mi_{suffix}' AND type = 'BuildFinish'"
        ).strip()
        or "0"
    )


def test_two_replica_lease_and_fetch(started_cluster):
    """One replica wins the materialized-index leader lease and produces the
    inner part; the other replica receives the part via the GET_PART log
    entry that `commitReplacingPartFromBackgroundTask` writes. Both replicas
    must end up with the same `total_rows` and answer self-queries identically.
    """
    suffix = _unique("two_replica")
    try:
        _create_source(suffix)
        _create_mi(suffix)

        node1.query(
            f"INSERT INTO src_{suffix} "
            f"SELECT number, [number * 1.0, number * 2.0, number * 3.0, number * 4.0] "
            f"FROM numbers(32)"
        )

        for node in all_nodes:
            node.query(f"SYSTEM SYNC REPLICA src_{suffix}", timeout=60)
            node.query(f"SYSTEM SYNC MATERIALIZED INDEX mi_{suffix}", timeout=90)

        # Both replicas must see the same coverage state via their local
        # `system.materialized_indexes` view.
        for node in all_nodes:
            row = node.query(
                f"SELECT materialized_index_part_count, total_rows "
                f"FROM system.materialized_indexes "
                f"WHERE name = 'mi_{suffix}' AND database = 'default'"
            ).strip().split("\t")
            assert int(row[0]) >= 1, f"{node.name}: part_count={row[0]}"
            assert int(row[1]) == 32, f"{node.name}: total_rows={row[1]}"

        # Lease invariant: at most one replica produced a `BuildFinish` event
        # (the lease holder). With 32 rows in a single source part the build
        # only runs once, so the count is exactly 1 on one replica and 0 on
        # the other. If both replicas raced past the lease check, the test
        # would observe two BuildFinish events.
        finishes = [_build_finish_count(node, suffix) for node in all_nodes]
        logging.info("BuildFinish per replica = %s", finishes)
        assert sum(finishes) == 1, f"BuildFinish events split: {finishes}"

        # Symmetric self-query: pick a row by k=7, the top-1 neighbour by
        # `L2Distance` must be itself with distance 0 on either replica.
        for node in all_nodes:
            res = node.query(
                f"WITH (SELECT embedding FROM src_{suffix} WHERE k = 7) AS q "
                f"SELECT k, round(L2Distance(embedding, q), 6) AS d "
                f"FROM src_{suffix} "
                f"ORDER BY L2Distance(embedding, q) "
                f"LIMIT 1 "
                f"SETTINGS force_materialized_index = 'mi_{suffix}'"
            ).strip()
            assert res == "7\t0", f"{node.name}: {res}"
    finally:
        _drop_tables(suffix)


def test_leader_failover_midbuild(started_cluster):
    """Leader crashes while it holds the materialized-index lease and is
    paused right before commit. The surviving replica must reacquire the
    lease, run its own build, and serve the index correctly. After the
    crashed leader is restarted, it must converge through `GET_PART`.

    Sequence:
      1. Stop builds on node2 so node1 is guaranteed to win the lease.
      2. Enable the `materialized_index_build_pause_in_finish` PAUSEABLE_ONCE
         failpoint on node1; insert source data; node1's build pauses just
         before commit (after the inner part is fully written to disk).
      3. SIGKILL node1, dropping its ZK session abruptly. The ephemeral
         lease and task-lock guards eventually expire.
      4. Resume builds on node2. node2 polls, observes the lease vacant,
         acquires it, rebuilds the part, and commits.
      5. Restart node1; it processes the GET_PART log entry node2 wrote
         and fetches the committed inner part.

    A failure to expire the lease, or a failure to rebuild from the
    surviving replica, would surface as a `SYSTEM SYNC` timeout on node2.
    """
    suffix = _unique("failover")

    try:
        _create_source(suffix)
        # Long sync timeout: we need to outlast one ZK session expiry
        # plus the rebuild on node2.
        _create_mi(suffix, sync_timeout=180)

        # Pin the lease to node1 by quiescing the scheduler on node2 before
        # any source part appears. `STOP MATERIALIZED INDEX BUILDS` only
        # gates new tasks; it does not interfere with an existing lease,
        # so node2 will sit idle until we resume it.
        node2.query(f"SYSTEM STOP MATERIALIZED INDEX BUILDS mi_{suffix}")

        node1.query(
            f"SYSTEM ENABLE FAILPOINT materialized_index_build_pause_in_finish"
        )

        node1.query(
            f"INSERT INTO src_{suffix} "
            f"SELECT number, [number * 1.0, number * 2.0, number * 3.0, number * 4.0] "
            f"FROM numbers(32)"
        )

        # The surviving replica must already have the source part on disk
        # before we kill node1 — once node1 is dead, ReplicatedMergeTree
        # cannot fetch from the only source-holding replica until node1
        # comes back, and an empty source on node2 would make SYSTEM SYNC
        # on the materialized index return trivially with 0 rows.
        node2.query(f"SYSTEM SYNC REPLICA src_{suffix}", timeout=60)

        # Block in node1 until the build reaches the pause point. If the
        # lease went to node2 instead of node1 (defeating the test setup),
        # this would time out, which is the signal we want.
        node1.query(
            f"SYSTEM WAIT FAILPOINT materialized_index_build_pause_in_finish PAUSE",
            timeout=60,
        )

        # Hard-kill node1 while the build is paused mid-finish. SIGKILL
        # leaves no chance to release the ephemeral lease cleanly, so
        # recovery exercises the session-expiry path.
        node1.stop_clickhouse(kill=True, stop_wait_sec=30)

        # Re-enable scheduling on node2; it will not produce a build until
        # the leader lease on node1 expires from ZK.
        node2.query(f"SYSTEM START MATERIALIZED INDEX BUILDS mi_{suffix}")

        # SYSTEM SYNC on node2 blocks until coverage of the source becomes
        # complete on the surviving replica. The waitForFullCoverage loop
        # refreshes from active parts every second, so it picks up the new
        # build as soon as node2 commits.
        node2.query(
            f"SYSTEM SYNC MATERIALIZED INDEX mi_{suffix}",
            timeout=240,
        )

        # The successor build must have happened on node2.
        node2_builds = _build_finish_count(node2, suffix)
        assert node2_builds >= 1, f"node2 BuildFinish={node2_builds}"

        # The index must answer correctly on node2.
        res2 = node2.query(
            f"WITH (SELECT embedding FROM src_{suffix} WHERE k = 7) AS q "
            f"SELECT k, round(L2Distance(embedding, q), 6) AS d "
            f"FROM src_{suffix} "
            f"ORDER BY L2Distance(embedding, q) "
            f"LIMIT 1 "
            f"SETTINGS force_materialized_index = 'mi_{suffix}'"
        ).strip()
        assert res2 == "7\t0", f"node2 self-query: {res2}"

        # Bring node1 back. The PAUSEABLE_ONCE failpoint state lives only
        # in the previous process, so the restarted node1 starts with a
        # clean failpoint table. It should converge by fetching the inner
        # part via GET_PART.
        node1.start_clickhouse()
        node1.query(f"SYSTEM SYNC REPLICA src_{suffix}", timeout=60)
        # Sync against the inner replicated MergeTree: GET_PART entries
        # for the materialized-index part live on the inner table's
        # replication queue. We address it via the well-known prefix
        # used by `04187` / 04188.
        inner_name = node1.query(
            f"SELECT concat('.inner_id.', toString(uuid)) "
            f"FROM system.tables "
            f"WHERE database = 'default' AND name = 'mi_{suffix}'"
        ).strip()
        node1.query(f"SYSTEM SYNC REPLICA `{inner_name}`", timeout=60)

        # node1 must serve the index just like node2 now.
        res1 = node1.query(
            f"WITH (SELECT embedding FROM src_{suffix} WHERE k = 7) AS q "
            f"SELECT k, round(L2Distance(embedding, q), 6) AS d "
            f"FROM src_{suffix} "
            f"ORDER BY L2Distance(embedding, q) "
            f"LIMIT 1 "
            f"SETTINGS force_materialized_index = 'mi_{suffix}'"
        ).strip()
        assert res1 == "7\t0", f"node1 self-query after restart: {res1}"
    finally:
        # If the test failed before we restarted node1, bring it back so
        # the DROPs on both replicas can complete.
        try:
            node1.start_clickhouse()
        except Exception:
            logging.exception("failed to (re)start node1 in cleanup")
        for node in all_nodes:
            try:
                node.query(
                    f"SYSTEM DISABLE FAILPOINT materialized_index_build_pause_in_finish"
                )
            except Exception:
                logging.exception(
                    "failed to disable failpoint on %s in cleanup", node.name
                )
        _drop_tables(suffix)
