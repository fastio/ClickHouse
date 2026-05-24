import logging

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node1 = cluster.add_instance(
    "node1",
    main_configs=["configs/config.xml"],
    user_configs=["configs/settings.xml"],
    with_zookeeper=True,
    stay_alive=True,
    macros={"shard": 1, "replica": 1},
)
node2 = cluster.add_instance(
    "node2",
    main_configs=["configs/config.xml"],
    user_configs=["configs/settings.xml"],
    with_zookeeper=True,
    stay_alive=True,
    macros={"shard": 1, "replica": 2},
)
node3 = cluster.add_instance(
    "node3",
    main_configs=["configs/config.xml"],
    user_configs=["configs/settings.xml"],
    with_zookeeper=True,
    stay_alive=True,
    macros={"shard": 1, "replica": 3},
)

all_nodes = [node1, node2, node3]


def attach_replicated_database(db_name, zk_path):
    """Create a 3-replica `Replicated` database; one replica per node."""
    for i, node in enumerate(all_nodes, start=1):
        node.query(
            f"CREATE DATABASE {db_name} ENGINE = Replicated('{zk_path}', 'shard1', 'replica{i}')"
        )


def drop_database_everywhere(db_name):
    for node in all_nodes:
        node.query(f"DROP DATABASE IF EXISTS {db_name} SYNC")


def sync_replicas(db_name):
    for node in all_nodes:
        node.query(f"SYSTEM SYNC DATABASE REPLICA {db_name}")


def map_names(node, db_name):
    """Return the sorted list of MaterializedAccessPath names visible to `node`.

    MaterializedAccessPath rows are filtered out of `system.tables` by D-06; they are
    only enumerable via `system.auxiliary_indexes`."""
    rows = node.query(
        f"SELECT name FROM system.auxiliary_indexes "
        f"WHERE database = '{db_name}' "
        f"ORDER BY name"
    ).strip()
    return rows.splitlines() if rows else []


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_create_drop_replication(started_cluster):
    """T-7.1 + T-7.2: a CREATE / DROP AUXILIARY INDEX issued on one replica is
    propagated through the Replicated database DDL log to every other replica."""
    db = "rdb_create_drop"
    attach_replicated_database(db, f"/test/{db}")
    try:
        node1.query(
            f"CREATE TABLE {db}.src (id UInt64, vec Array(Float32)) "
            f"ENGINE = ReplicatedMergeTree ORDER BY id"
        )
        node1.query(f"CREATE AUXILIARY INDEX m1 ON {db}.src (vec) USING diskann()")
        node1.query(f"CREATE AUXILIARY INDEX m2 ON {db}.src (vec) USING diskann()")

        sync_replicas(db)
        for node in all_nodes:
            assert map_names(node, db) == ["m1", "m2"], f"{node.name}: {map_names(node, db)}"

        node2.query(f"DROP AUXILIARY INDEX {db}.m1")
        sync_replicas(db)
        for node in all_nodes:
            assert map_names(node, db) == ["m2"], f"{node.name}: {map_names(node, db)}"
    finally:
        drop_database_everywhere(db)


def test_cascade_across_replicas(started_cluster):
    """T-7.3: DROP TABLE source on one replica cascades to every dependent
    MaterializedAccessPath on every replica via the source -> path edge."""
    db = "rdb_cascade"
    attach_replicated_database(db, f"/test/{db}")
    try:
        node1.query(
            f"CREATE TABLE {db}.src (id UInt64, vec Array(Float32)) "
            f"ENGINE = ReplicatedMergeTree ORDER BY id"
        )
        node1.query(f"CREATE AUXILIARY INDEX m1 ON {db}.src (vec) USING diskann()")
        node1.query(f"CREATE AUXILIARY INDEX m2 ON {db}.src (vec) USING diskann()")
        sync_replicas(db)

        node1.query(f"DROP TABLE {db}.src SYNC")
        sync_replicas(db)
        for node in all_nodes:
            assert map_names(node, db) == [], f"{node.name}: {map_names(node, db)}"
    finally:
        drop_database_everywhere(db)


def test_restart_recovery(started_cluster):
    """T-2.1: a Replicated-engine MaterializedAccessPath survives a clickhouse-server
    restart on its host and is rebuilt by the startup-recovery loader."""
    db = "rdb_restart"
    attach_replicated_database(db, f"/test/{db}")
    try:
        node1.query(
            f"CREATE TABLE {db}.src (id UInt64, vec Array(Float32)) "
            f"ENGINE = ReplicatedMergeTree ORDER BY id"
        )
        node1.query(f"CREATE AUXILIARY INDEX m1 ON {db}.src (vec) USING diskann()")
        sync_replicas(db)

        node1.restart_clickhouse()
        assert map_names(node1, db) == ["m1"]

        # The other replicas keep working through the restart.
        for node in (node2, node3):
            assert map_names(node, db) == ["m1"]
    finally:
        drop_database_everywhere(db)


def test_rename_preserves_binding(started_cluster):
    """T-rename.1: renaming the source table does not break the MAP -> source binding,
    because the catalog stores the binding by source UUID, not by name."""
    db = "rdb_rename"
    attach_replicated_database(db, f"/test/{db}")
    try:
        node1.query(
            f"CREATE TABLE {db}.src (id UInt64, vec Array(Float32)) "
            f"ENGINE = ReplicatedMergeTree ORDER BY id"
        )
        node1.query(f"CREATE AUXILIARY INDEX m1 ON {db}.src (vec) USING diskann()")
        sync_replicas(db)

        node1.query(f"RENAME TABLE {db}.src TO {db}.src_renamed")
        sync_replicas(db)

        # The MAP is still listed and DROP TABLE on the renamed source still cascades.
        for node in all_nodes:
            assert map_names(node, db) == ["m1"], f"{node.name}: {map_names(node, db)}"
        node1.query(f"DROP TABLE {db}.src_renamed SYNC")
        sync_replicas(db)
        for node in all_nodes:
            assert map_names(node, db) == [], f"{node.name}: {map_names(node, db)}"
    finally:
        drop_database_everywhere(db)


def test_is_replicated_consistent_across_replicas(started_cluster):
    """T-11.1: every replica's metadata.sql for the same MAP carries
    `_meta_is_replicated = 1` because the MAP lives in a Replicated database."""
    db = "rdb_is_replicated"
    attach_replicated_database(db, f"/test/{db}")
    try:
        node1.query(
            f"CREATE TABLE {db}.src (id UInt64, vec Array(Float32)) "
            f"ENGINE = ReplicatedMergeTree ORDER BY id"
        )
        node1.query(f"CREATE AUXILIARY INDEX m1 ON {db}.src (vec) USING diskann()")
        sync_replicas(db)

        for node in all_nodes:
            # `metadata_path` mirrors `system.tables`: a path relative to the server data
            # directory, so prepend `/var/lib/clickhouse/` for the in-container `cat`.
            metadata_path = node.query(
                f"SELECT metadata_path FROM system.auxiliary_indexes "
                f"WHERE database = '{db}' AND name = 'm1'"
            ).strip()
            content = node.exec_in_container(["cat", f"/var/lib/clickhouse/{metadata_path}"])
            assert "_meta_is_replicated = 1" in content, f"{node.name}: {content}"
    finally:
        drop_database_everywhere(db)
