// SIFT-1M Recall@K / QPS sweep driver, rewritten in Go.
//
// Reproduces the behaviour of the previous bash harness (run.sh + lib/*.sh)
// but talks to ClickHouse over the HTTP interface so it does not run into
// the `var=$(clickhouse client ...)` rc=1 reproducer that intermittently
// breaks command substitution in the bash version.
//
// Output layout (matches the bash harness):
//   $RUN_DIR/server_meta.txt
//   $RUN_DIR/sanity.txt
//   $RUN_DIR/build/<scenario>_<cfg>_sls=...kv
//   $RUN_DIR/queries/queries_q<N>_k<K>.sql
//   $RUN_DIR/bench_logs/<cell>.json   (raw timings dump)
//   $RUN_DIR/sweep.tsv                (one row per measurement cell)

package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// ---------- Config ----------

type config struct {
	httpURL          string // http://host:port — what the Go driver actually uses
	clickhouseBinary string // metadata only, recorded in server_meta.txt
	tcpPort          int    // metadata only, recorded in server_meta.txt
	db               string
	dir              string // ann_sift1m root
	dataDir          string
	hdf5             string
	runID            string
	runDir           string
	gitCommit        string
	k                int
	queriesPerCell   int
	warmupQueries    int
	runs             int
	slsList          []int
	beamWidth        int
	searchIOLimit    int
	scenarios        []string
	buildCfgs        []string
	concurrencies    []int
	sanityQueries    int
	sanityMissBudget int
}

// loadConfig parses command-line flags. Inputs are split on commas (e.g.
// `--scenarios=single_group,multi_group`, `--sls-list=10,30,50`).
func loadConfig() (*config, error) {
	exe, err := os.Executable()
	if err != nil {
		return nil, err
	}
	defaultDir := filepath.Clean(filepath.Join(filepath.Dir(exe), "..", ".."))

	host := flag.String("host", "127.0.0.1", "ClickHouse HTTP host")
	httpPort := flag.Int("http-port", 8123, "ClickHouse HTTP port")
	tcpPort := flag.Int("tcp-port", 9000, "ClickHouse TCP port (recorded in server_meta only)")
	binary := flag.String("clickhouse-binary", "", "path to the `clickhouse` binary (recorded in server_meta only; not invoked)")
	db := flag.String("db", "sift", "target database")
	dir := flag.String("dir", defaultDir, "ann_sift1m harness root (configs/, scenarios/, data/)")
	hdf5 := flag.String("hdf5", "", "path to the SIFT-1M HDF5 file (default: <dir>/data/sift-128-euclidean.hdf5)")
	resultsDir := flag.String("results-dir", "", "where to write the run directory (default: <dir>/results)")

	k := flag.Int("k", 10, "Recall@K")
	queriesPerCell := flag.Int("queries-per-cell", 1000, "queries used for both recall and QPS, per cell")
	warmupQueries := flag.Int("warmup-queries", 200, "warm-up iterations before each measurement")
	runs := flag.Int("runs", 3, "repetitions per cell")

	slsList := flag.String("sls-list", "10,30,50,100,200", "comma-separated search_list_size sweep")
	beamWidth := flag.Int("beam-width", 4, "beam_width (constant per cell)")
	searchIOLimit := flag.Int("search-io-limit", 4, "search_io_limit (constant per cell)")
	scenarios := flag.String("scenarios", "single_group,multi_group", "comma-separated scenario keys (matches scenarios/<key>.env)")
	buildCfgs := flag.String("build-cfgs", "paper", "comma-separated build cfg keys (matches configs/build_<key>.env)")
	concurrencies := flag.String("concurrencies", "", "comma-separated concurrency levels (default: 1,<nproc>)")

	sanityQueries := flag.Int("sanity-queries", 5, "random qids to probe before the sweep")
	sanityMissBudget := flag.Int("sanity-miss-budget", 1, "per-query miss tolerance for the brute-force sanity check")

	flag.Parse()

	cfg := &config{
		httpURL:          fmt.Sprintf("http://%s:%d", *host, *httpPort),
		clickhouseBinary: *binary,
		tcpPort:          *tcpPort,
		db:               *db,
		dir:              *dir,
		k:                *k,
		queriesPerCell:   *queriesPerCell,
		warmupQueries:    *warmupQueries,
		runs:             *runs,
		beamWidth:        *beamWidth,
		searchIOLimit:    *searchIOLimit,
		sanityQueries:    *sanityQueries,
		sanityMissBudget: *sanityMissBudget,
	}

	cfg.slsList, err = parseIntList(*slsList)
	if err != nil {
		return nil, fmt.Errorf("--sls-list: %w", err)
	}
	cfg.scenarios = parseStringList(*scenarios)
	cfg.buildCfgs = parseStringList(*buildCfgs)
	if *concurrencies == "" {
		cfg.concurrencies = []int{1, runtime.NumCPU()}
	} else {
		cfg.concurrencies, err = parseIntList(*concurrencies)
		if err != nil {
			return nil, fmt.Errorf("--concurrencies: %w", err)
		}
	}
	if len(cfg.scenarios) == 0 {
		return nil, fmt.Errorf("--scenarios is empty")
	}
	if len(cfg.buildCfgs) == 0 {
		return nil, fmt.Errorf("--build-cfgs is empty")
	}
	if len(cfg.slsList) == 0 {
		return nil, fmt.Errorf("--sls-list is empty")
	}
	if len(cfg.concurrencies) == 0 {
		return nil, fmt.Errorf("--concurrencies is empty")
	}

	cfg.dataDir = filepath.Join(cfg.dir, "data")
	if *hdf5 != "" {
		cfg.hdf5 = *hdf5
	} else {
		cfg.hdf5 = filepath.Join(cfg.dataDir, "sift-128-euclidean.hdf5")
	}
	if _, err := os.Stat(cfg.hdf5); err != nil {
		return nil, fmt.Errorf("missing %s - run download.sh first", cfg.hdf5)
	}

	resultsRoot := *resultsDir
	if resultsRoot == "" {
		resultsRoot = filepath.Join(cfg.dir, "results")
	}
	cfg.runID = time.Now().UTC().Format("20060102T150405Z")
	cfg.runDir = filepath.Join(resultsRoot, cfg.runID)
	if err := os.MkdirAll(cfg.runDir, 0o755); err != nil {
		return nil, err
	}

	cfg.gitCommit = gitCommit(cfg.dir)
	return cfg, nil
}

func parseIntList(s string) ([]int, error) {
	var out []int
	for _, f := range strings.Split(s, ",") {
		f = strings.TrimSpace(f)
		if f == "" {
			continue
		}
		n, err := strconv.Atoi(f)
		if err != nil {
			return nil, fmt.Errorf("not an int: %q", f)
		}
		out = append(out, n)
	}
	return out, nil
}

func parseStringList(s string) []string {
	var out []string
	for _, f := range strings.Split(s, ",") {
		f = strings.TrimSpace(f)
		if f != "" {
			out = append(out, f)
		}
	}
	return out
}

func gitCommit(dir string) string {
	cmd := exec.Command("git", "-C", dir, "rev-parse", "HEAD")
	out, err := cmd.Output()
	if err != nil {
		return "unknown"
	}
	return strings.TrimSpace(string(out))
}

// ---------- ClickHouse HTTP client ----------

// CH talks to ClickHouse via the HTTP interface. The default-database is
// pinned per-request via the ?database= query parameter. Per-query settings
// (e.g. log_comment) are passed the same way.
type CH struct {
	url    string
	db     string
	client *http.Client
}

func newCH(rawURL, db string) *CH {
	return &CH{
		url: rawURL,
		db:  db,
		client: &http.Client{
			Timeout: 30 * time.Minute,
		},
	}
}

// Exec runs a statement, discarding the response body.
func (c *CH) Exec(ctx context.Context, sql string) error {
	_, err := c.do(ctx, sql, nil, nil, true)
	return err
}

// Query runs a SELECT and returns the response body (ClickHouse default
// format is TabSeparated). Trailing newline is stripped.
func (c *CH) Query(ctx context.Context, sql string) (string, error) {
	body, err := c.do(ctx, sql, nil, nil, true)
	if err != nil {
		return "", err
	}
	return strings.TrimRight(string(body), "\n"), nil
}

// QueryTagged behaves like Query, but tags the resulting query_log row with
// `log_comment = tag` (used by per-cell ProfileEvents extraction).
func (c *CH) QueryTagged(ctx context.Context, tag, sql string) (string, error) {
	settings := url.Values{}
	settings.Set("log_comment", tag)
	body, err := c.do(ctx, sql, settings, nil, true)
	if err != nil {
		return "", err
	}
	return strings.TrimRight(string(body), "\n"), nil
}

// QueryNoLog runs a SELECT but disables logging into query_log for this
// query — useful for the recall computation loop, which would otherwise
// flood query_log.
func (c *CH) QueryNoLog(ctx context.Context, sql string) (string, error) {
	settings := url.Values{}
	settings.Set("log_queries", "0")
	body, err := c.do(ctx, sql, settings, nil, true)
	if err != nil {
		return "", err
	}
	return strings.TrimRight(string(body), "\n"), nil
}

// Insert streams a body (e.g. RowBinary) into a table.
func (c *CH) Insert(ctx context.Context, sql string, body io.Reader) error {
	// For INSERTs, ClickHouse expects the SQL in the `query` query-param
	// and the row payload in the request body.
	settings := url.Values{}
	settings.Set("query", sql)
	_, err := c.do(ctx, "", settings, body, false)
	return err
}

// do is the single network entry point. `extraSettings` is merged into the
// query string. When `body` is nil the SQL is sent in the request body
// (TSV / multi-row results read fine that way). When `body != nil` the SQL
// must be supplied via `extraSettings["query"]` and `body` carries the row
// payload (RowBinary / etc).
func (c *CH) do(ctx context.Context, sql string, extraSettings url.Values, body io.Reader, useDefaultFormat bool) ([]byte, error) {
	q := url.Values{}
	if c.db != "" {
		q.Set("database", c.db)
	}
	if useDefaultFormat {
		// TabSeparated is the default; explicit for readability.
		q.Set("default_format", "TabSeparated")
	}
	for k, vs := range extraSettings {
		for _, v := range vs {
			q.Set(k, v)
		}
	}
	target := c.url + "/?" + q.Encode()

	var reqBody io.Reader
	if body != nil {
		reqBody = body
	} else if sql != "" {
		reqBody = strings.NewReader(sql)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, target, reqBody)
	if err != nil {
		return nil, err
	}
	resp, err := c.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("clickhouse %d: %s", resp.StatusCode, strings.TrimSpace(string(respBody)))
	}
	return respBody, nil
}

// ---------- Logging ----------

func logf(format string, args ...any) {
	ts := time.Now().UTC().Format("15:04:05Z")
	fmt.Fprintf(os.Stderr, "[%s] %s\n", ts, fmt.Sprintf(format, args...))
}

func warnf(format string, args ...any) {
	ts := time.Now().UTC().Format("15:04:05Z")
	fmt.Fprintf(os.Stderr, "[%s] WARN: %s\n", ts, fmt.Sprintf(format, args...))
}

// ---------- Scenarios / build configs ----------

type scenarioCfg struct {
	name             string
	annGroupMinRows  int
	annGroupMaxRows  int
	annGroupMaxParts int
}

type buildCfg struct {
	name                string
	maxDegree           int
	buildSearchListSize int
	alpha               float64
	pqChunks            int
	numThreads          int
	buildRAMLimitGB     int
	hashSeed            int
}

// loadEnvFile parses a shell-style KEY=VALUE file.
func loadEnvFile(path string) (map[string]string, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	out := map[string]string{}
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		eq := strings.IndexByte(line, '=')
		if eq < 0 {
			continue
		}
		k := strings.TrimSpace(line[:eq])
		v := strings.TrimSpace(line[eq+1:])
		v = strings.Trim(v, "\"'")
		out[k] = v
	}
	return out, sc.Err()
}

func loadScenario(dir, name string) (*scenarioCfg, error) {
	m, err := loadEnvFile(filepath.Join(dir, "scenarios", name+".env"))
	if err != nil {
		return nil, fmt.Errorf("scenario %q: %w", name, err)
	}
	must := func(k string) (int, error) {
		v, ok := m[k]
		if !ok {
			return 0, fmt.Errorf("scenario %q: missing %s", name, k)
		}
		return strconv.Atoi(v)
	}
	s := &scenarioCfg{name: name}
	var err1, err2, err3 error
	s.annGroupMinRows, err1 = must("ANN_GROUP_MIN_ROWS")
	s.annGroupMaxRows, err2 = must("ANN_GROUP_MAX_ROWS")
	s.annGroupMaxParts, err3 = must("ANN_GROUP_MAX_PARTS")
	return s, errors.Join(err1, err2, err3)
}

func loadBuildCfg(dir, name string) (*buildCfg, error) {
	m, err := loadEnvFile(filepath.Join(dir, "configs", "build_"+name+".env"))
	if err != nil {
		return nil, fmt.Errorf("build cfg %q: %w", name, err)
	}
	mustI := func(k string) (int, error) {
		v, ok := m[k]
		if !ok {
			return 0, fmt.Errorf("build cfg %q: missing %s", name, k)
		}
		return strconv.Atoi(v)
	}
	b := &buildCfg{name: name}
	var errs []error
	collect := func(err error) {
		if err != nil {
			errs = append(errs, err)
		}
	}
	var err1 error
	b.maxDegree, err1 = mustI("MAX_DEGREE")
	collect(err1)
	b.buildSearchListSize, err1 = mustI("BUILD_SEARCH_LIST_SIZE")
	collect(err1)
	if v, ok := m["ALPHA"]; ok {
		b.alpha, err1 = strconv.ParseFloat(v, 64)
		collect(err1)
	} else {
		errs = append(errs, fmt.Errorf("build cfg %q: missing ALPHA", name))
	}
	b.pqChunks, _ = strconv.Atoi(m["PQ_CHUNKS"])
	b.numThreads, err1 = mustI("NUM_THREADS")
	collect(err1)
	b.buildRAMLimitGB, err1 = mustI("BUILD_RAM_LIMIT_GB")
	collect(err1)
	b.hashSeed, err1 = mustI("HASH_SEED")
	collect(err1)
	return b, errors.Join(errs...)
}

// ---------- Server meta ----------

func writeServerMeta(ctx context.Context, ch *CH, cfg *config) error {
	path := filepath.Join(cfg.runDir, "server_meta.txt")
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	get := func(sql string) string {
		v, err := ch.Query(ctx, sql)
		if err != nil {
			return "unknown"
		}
		return v
	}

	loadAvg := "unknown"
	if b, err := os.ReadFile("/proc/loadavg"); err == nil {
		loadAvg = strings.TrimSpace(string(b))
	}
	memTotal := "unknown"
	if b, err := os.ReadFile("/proc/meminfo"); err == nil {
		for _, line := range strings.Split(string(b), "\n") {
			if strings.HasPrefix(line, "MemTotal:") {
				fields := strings.Fields(line)
				if len(fields) >= 2 {
					memTotal = fields[1]
				}
				break
			}
		}
	}
	uname := "unknown"
	if out, err := exec.Command("uname", "-sr").Output(); err == nil {
		uname = strings.TrimSpace(string(out))
	}

	fmt.Fprintf(f, "run_id: %s\n", cfg.runID)
	fmt.Fprintf(f, "git_commit: %s\n", cfg.gitCommit)
	fmt.Fprintf(f, "server_version: %s\n", get("SELECT version()"))
	fmt.Fprintf(f, "build_id:       %s\n", get("SELECT buildId()"))
	fmt.Fprintf(f, "http_url: %s\n", cfg.httpURL)
	fmt.Fprintf(f, "tcp_port: %d\n", cfg.tcpPort)
	if cfg.clickhouseBinary != "" {
		fmt.Fprintf(f, "clickhouse_binary: %s\n", cfg.clickhouseBinary)
	}
	fmt.Fprintf(f, "data_dir: %s\n", cfg.dataDir)
	fmt.Fprintf(f, "k: %d\n", cfg.k)
	fmt.Fprintf(f, "queries_per_cell: %d\n", cfg.queriesPerCell)
	fmt.Fprintf(f, "warmup_queries: %d\n", cfg.warmupQueries)
	fmt.Fprintf(f, "runs: %d\n", cfg.runs)
	fmt.Fprintf(f, "sls_list: %s\n", joinInts(cfg.slsList))
	fmt.Fprintf(f, "beam_width: %d\n", cfg.beamWidth)
	fmt.Fprintf(f, "search_io_limit: %d\n", cfg.searchIOLimit)
	fmt.Fprintf(f, "scenarios: %s\n", strings.Join(cfg.scenarios, " "))
	fmt.Fprintf(f, "build_cfgs: %s\n", strings.Join(cfg.buildCfgs, " "))
	fmt.Fprintf(f, "concurrencies: %s\n", joinInts(cfg.concurrencies))
	fmt.Fprintf(f, "nproc: %d\n", runtime.NumCPU())
	fmt.Fprintf(f, "host_kernel: %s\n", uname)
	fmt.Fprintf(f, "host_memtotal_kb: %s\n", memTotal)
	fmt.Fprintf(f, "host_load_at_start: %s\n", loadAvg)
	fmt.Fprintf(f, "merge_tree_clear_retired_ann_groups_interval_seconds: %s\n",
		get("SELECT value FROM system.merge_tree_settings WHERE name = 'merge_tree_clear_retired_ann_groups_interval_seconds'"))
	fmt.Fprintf(f, "max_threads: %s\n",
		get("SELECT value FROM system.settings WHERE name = 'max_threads'"))
	logf("wrote %s", path)
	return nil
}

func joinInts(xs []int) string {
	parts := make([]string, len(xs))
	for i, x := range xs {
		parts[i] = strconv.Itoa(x)
	}
	return strings.Join(parts, " ")
}

// ---------- Data load ----------

func loadHDF5Stream(ctx context.Context, cfg *config, schema string) (*exec.Cmd, io.ReadCloser, error) {
	script := filepath.Join(cfg.dir, "hdf5_to_rowbinary.py")
	cmd := exec.CommandContext(ctx, "python3", script, cfg.hdf5, "--schema", schema)
	cmd.Stderr = os.Stderr
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, nil, err
	}
	if err := cmd.Start(); err != nil {
		return nil, nil, err
	}
	return cmd, stdout, nil
}

func loadQueryAndGT(ctx context.Context, ch *CH, cfg *config) error {
	if err := ch.Exec(ctx, "CREATE DATABASE IF NOT EXISTS "+cfg.db); err != nil {
		return err
	}
	logf("loading sift_query (10000 x 128 Float32) and sift_gt (10000 x 100 UInt32)")

	for _, sql := range []string{
		"DROP TABLE IF EXISTS sift_query",
		"DROP TABLE IF EXISTS sift_gt",
		"CREATE TABLE sift_query (id UInt32, v Array(Float32)) ENGINE = MergeTree ORDER BY id",
		"CREATE TABLE sift_gt    (query_id UInt32, neighbors Array(UInt32)) ENGINE = MergeTree ORDER BY query_id",
	} {
		if err := ch.Exec(ctx, sql); err != nil {
			return err
		}
	}

	for _, p := range []struct{ schema, table string }{
		{"query", "sift_query"},
		{"gt", "sift_gt"},
	} {
		cmd, stdout, err := loadHDF5Stream(ctx, cfg, p.schema)
		if err != nil {
			return err
		}
		err = ch.Insert(ctx, "INSERT INTO "+p.table+" FORMAT RowBinary", stdout)
		if cerr := cmd.Wait(); cerr != nil && err == nil {
			err = cerr
		}
		if err != nil {
			return fmt.Errorf("INSERT %s: %w", p.table, err)
		}
	}

	queryRows, err := ch.Query(ctx, "SELECT count() FROM sift_query")
	if err != nil {
		return err
	}
	gtRows, err := ch.Query(ctx, "SELECT count() FROM sift_gt")
	if err != nil {
		return err
	}
	logf("sift_query=%s rows, sift_gt=%s rows", queryRows, gtRows)
	if queryRows != "10000" || gtRows != "10000" {
		return fmt.Errorf("unexpected row counts (got %s/%s, want 10000/10000); HDF5 may be truncated", queryRows, gtRows)
	}
	return nil
}

// ---------- Sanity ----------

func runSanity(ctx context.Context, ch *CH, cfg *config) error {
	logf("creating throwaway sift_base for sanity check (no ANN index)")
	if err := ch.Exec(ctx, "DROP TABLE IF EXISTS sift_base"); err != nil {
		return err
	}
	if err := ch.Exec(ctx, "CREATE TABLE sift_base (id UInt64, v Array(Float32)) ENGINE = MergeTree ORDER BY id"); err != nil {
		return err
	}
	cmd, stdout, err := loadHDF5Stream(ctx, cfg, "base")
	if err != nil {
		return err
	}
	err = ch.Insert(ctx, "INSERT INTO sift_base FORMAT RowBinary", stdout)
	if cerr := cmd.Wait(); cerr != nil && err == nil {
		err = cerr
	}
	if err != nil {
		return fmt.Errorf("INSERT sift_base: %w", err)
	}

	logf("brute-force sanity: %d queries, K=%d, miss budget=%d per query", cfg.sanityQueries, cfg.k, cfg.sanityMissBudget)
	qids, err := ch.Query(ctx, fmt.Sprintf(
		"SELECT id FROM sift_query ORDER BY rand() LIMIT %d", cfg.sanityQueries))
	if err != nil {
		return err
	}

	mismatches := 0
	for _, qid := range strings.Fields(qids) {
		sql := fmt.Sprintf(`
WITH
    (SELECT v FROM sift_query WHERE id = %s) AS qv,
    (SELECT arraySlice(neighbors, 1, %d) FROM sift_gt WHERE query_id = %s) AS gt
SELECT %d - length(arrayIntersect(
    gt,
    (SELECT groupArray(id) FROM (
        SELECT id FROM sift_base
        ORDER BY L2Distance(v, materialize(qv)) ASC
        LIMIT %d
        SETTINGS try_use_ann_search = 0
    ))
))`, qid, cfg.k, qid, cfg.k, cfg.k)
		out, err := ch.Query(ctx, sql)
		if err != nil {
			return err
		}
		misses, err := strconv.Atoi(strings.TrimSpace(out))
		if err != nil {
			return fmt.Errorf("sanity: parsing miss count for qid=%s: %q: %w", qid, out, err)
		}
		if misses > cfg.sanityMissBudget {
			warnf("qid=%s: brute-force vs HDF5 differ on %d ids (budget=%d)", qid, misses, cfg.sanityMissBudget)
			mismatches++
		}
	}

	sanityPath := filepath.Join(cfg.runDir, "sanity.txt")
	msg := fmt.Sprintf("sanity OK: %d/%d brute-force results within tolerance of HDF5 ground truth\n",
		cfg.sanityQueries, cfg.sanityQueries)
	if mismatches > 0 {
		msg = fmt.Sprintf("sanity FAILED: %d/%d queries above miss budget\n", mismatches, cfg.sanityQueries)
	}
	if err := os.WriteFile(sanityPath, []byte(msg), 0o644); err != nil {
		return err
	}
	if mismatches > 0 {
		return fmt.Errorf("sanity check failed on %d/%d queries", mismatches, cfg.sanityQueries)
	}
	logf("%s", strings.TrimRight(msg, "\n"))
	return ch.Exec(ctx, "DROP TABLE sift_base")
}

// ---------- Build phase ----------

type buildArtifact struct {
	BuildSeconds  int    `json:"build_seconds"`
	IndexSizeMB   string `json:"index_size_mb"`
	ANNGroups     string `json:"ann_groups"`
}

func runBuild(ctx context.Context, ch *CH, cfg *config, sc *scenarioCfg, b *buildCfg, sls int) (*buildArtifact, error) {
	logf("build(%s/%s) sls=%d beam=%d io_limit=%d", sc.name, b.name, sls, cfg.beamWidth, cfg.searchIOLimit)

	if err := ch.Exec(ctx, "DROP TABLE IF EXISTS sift_base"); err != nil {
		return nil, err
	}

	createSQL := fmt.Sprintf(`
CREATE TABLE sift_base
(
    id UInt64,
    v  Array(Float32),
    INDEX idx_v v TYPE ann(
        dim                    = 128,
        metric                 = 'L2',
        max_degree             = %d,
        build_search_list_size = %d,
        alpha                  = %g,
        search_list_size       = %d,
        beam_width             = %d,
        search_io_limit        = %d,
        num_threads            = %d,
        build_ram_limit_gb     = %d,
        hash_seed              = %d
    ) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS
    enable_block_number_column = 1,
    enable_block_offset_column = 1,
    ann_group_min_rows         = %d,
    ann_group_max_rows         = %d,
    ann_group_max_parts        = %d`,
		b.maxDegree, b.buildSearchListSize, b.alpha,
		sls, cfg.beamWidth, cfg.searchIOLimit,
		b.numThreads, b.buildRAMLimitGB, b.hashSeed,
		sc.annGroupMinRows, sc.annGroupMaxRows, sc.annGroupMaxParts,
	)
	if err := ch.Exec(ctx, createSQL); err != nil {
		return nil, fmt.Errorf("create sift_base: %w", err)
	}

	cmd, stdout, err := loadHDF5Stream(ctx, cfg, "base")
	if err != nil {
		return nil, err
	}
	if err := ch.Insert(ctx, "INSERT INTO sift_base FORMAT RowBinary", stdout); err != nil {
		_ = cmd.Wait()
		return nil, fmt.Errorf("insert sift_base: %w", err)
	}
	if err := cmd.Wait(); err != nil {
		return nil, err
	}

	start := time.Now()
	if err := ch.Exec(ctx, "SYSTEM BUILD ANN INDEX sift_base"); err != nil {
		return nil, fmt.Errorf("SYSTEM BUILD ANN INDEX: %w", err)
	}
	if err := waitFullCoverage(ctx, ch, cfg.db, "sift_base", 1800*time.Second); err != nil {
		return nil, err
	}
	buildSeconds := int(time.Since(start).Seconds())

	indexSizeMB, err := ch.Query(ctx, fmt.Sprintf(
		`SELECT round(sum(secondary_indices_compressed_bytes) / 1048576.0, 1)
		 FROM system.parts
		 WHERE database = '%s' AND table = 'sift_base' AND active`, cfg.db))
	if err != nil {
		return nil, err
	}
	groups, err := ch.Query(ctx, fmt.Sprintf(
		"SELECT tupleElement(tableANNCoverage('%s', 'sift_base'), 'total')", cfg.db))
	if err != nil {
		return nil, err
	}

	art := &buildArtifact{BuildSeconds: buildSeconds, IndexSizeMB: indexSizeMB, ANNGroups: groups}

	kvPath := filepath.Join(cfg.runDir, "build", fmt.Sprintf(
		"%s_%s_sls=%d_beam=%d_io=%d.kv", sc.name, b.name, sls, cfg.beamWidth, cfg.searchIOLimit))
	if err := os.MkdirAll(filepath.Dir(kvPath), 0o755); err != nil {
		return nil, err
	}
	kvF, err := os.Create(kvPath)
	if err != nil {
		return nil, err
	}
	fmt.Fprintf(kvF, "scenario=%s\n", sc.name)
	fmt.Fprintf(kvF, "build_cfg=%s\n", b.name)
	fmt.Fprintf(kvF, "sls=%d\n", sls)
	fmt.Fprintf(kvF, "beam=%d\n", cfg.beamWidth)
	fmt.Fprintf(kvF, "io_limit=%d\n", cfg.searchIOLimit)
	fmt.Fprintf(kvF, "build_seconds=%d\n", buildSeconds)
	fmt.Fprintf(kvF, "index_size_mb=%s\n", indexSizeMB)
	fmt.Fprintf(kvF, "ann_groups=%s\n", groups)
	fmt.Fprintf(kvF, "max_degree=%d\n", b.maxDegree)
	fmt.Fprintf(kvF, "build_search_list_size=%d\n", b.buildSearchListSize)
	fmt.Fprintf(kvF, "alpha=%g\n", b.alpha)
	fmt.Fprintf(kvF, "hash_seed=%d\n", b.hashSeed)
	kvF.Close()

	logf("build done: %ds, index=%s MB, groups=%s", buildSeconds, indexSizeMB, groups)
	return art, nil
}

func waitFullCoverage(ctx context.Context, ch *CH, db, table string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	start := time.Now()
	lastLog := start
	for time.Now().Before(deadline) {
		row, err := ch.Query(ctx, fmt.Sprintf(
			`SELECT
                tupleElement(tableANNCoverage('%s', '%s'), 'covered'),
                tupleElement(tableANNCoverage('%s', '%s'), 'total')`,
			db, table, db, table))
		if err != nil {
			return err
		}
		fields := strings.Split(strings.TrimSpace(row), "\t")
		if len(fields) == 2 && fields[0] == fields[1] && fields[1] != "0" {
			logf("ANN coverage on %s.%s: %s/%s after %ds", db, table, fields[0], fields[1], int(time.Since(start).Seconds()))
			return nil
		}
		if time.Since(lastLog) >= 30*time.Second {
			covered, total := "?", "?"
			if len(fields) == 2 {
				covered, total = fields[0], fields[1]
			}
			logf("ANN coverage on %s.%s: %s/%s (elapsed %ds)", db, table, covered, total, int(time.Since(start).Seconds()))
			lastLog = time.Now()
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Second):
		}
	}
	return fmt.Errorf("timeout waiting for ANN coverage on %s.%s after %s", db, table, timeout)
}

// ---------- Measurement phase ----------

type cellResult struct {
	scenario, buildCfg                                   string
	sls, beam, ioLimit, conc, runIdx                     int
	queries, k                                           int
	recall                                               float64
	qps, p50us, p95us, p99us                             float64
	searchCountP50, searchUSP50, resultsReturnedP50      string
	notes                                                string
}

func renderQueriesFile(ctx context.Context, ch *CH, cfg *config) (string, error) {
	path := filepath.Join(cfg.runDir, "queries", fmt.Sprintf("queries_q%d_k%d.sql", cfg.queriesPerCell, cfg.k))
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return "", err
	}
	if st, err := os.Stat(path); err == nil && st.Size() > 0 {
		return path, nil
	}
	logf("rendering query file: %s (%d queries)", path, cfg.queriesPerCell)
	sql := fmt.Sprintf(`
SELECT 'SELECT id FROM sift_base ORDER BY L2Distance(v, [' ||
       arrayStringConcat(arrayMap(x -> toString(x), v), ',') ||
       ']::Array(Float32)) LIMIT %d FORMAT Null;'
FROM sift_query
ORDER BY id
LIMIT %d`, cfg.k, cfg.queriesPerCell)
	out, err := ch.Query(ctx, sql)
	if err != nil {
		return "", err
	}
	return path, os.WriteFile(path, []byte(out+"\n"), 0o644)
}

// runConcurrentBench runs `iters` queries from `queriesFile` with `conc`
// concurrent workers. Each worker pulls the next query from a shared index
// (atomic counter) and times the request. Returns end-to-end latencies (s)
// in arrival order — sorted afterwards for percentiles. log_comment is set
// per query so ProfileEvents can be retrieved by the same tag.
func runConcurrentBench(ctx context.Context, ch *CH, queriesFile string, iters, conc int, logComment string) ([]float64, time.Duration, error) {
	queries, err := readQueries(queriesFile)
	if err != nil {
		return nil, 0, err
	}
	if len(queries) == 0 {
		return nil, 0, fmt.Errorf("queries file %s is empty", queriesFile)
	}

	lat := make([]float64, iters)
	var idx atomic.Int64
	idx.Store(-1)
	var wg sync.WaitGroup
	wg.Add(conc)
	wallStart := time.Now()
	errCh := make(chan error, conc)

	worker := func() {
		defer wg.Done()
		for {
			i := idx.Add(1)
			if int(i) >= iters {
				return
			}
			q := queries[int(i)%len(queries)]
			t0 := time.Now()
			settings := url.Values{}
			settings.Set("log_comment", logComment)
			_, err := ch.do(ctx, q, settings, nil, false)
			if err != nil {
				select {
				case errCh <- err:
				default:
				}
				return
			}
			lat[i] = time.Since(t0).Seconds()
		}
	}
	for i := 0; i < conc; i++ {
		go worker()
	}
	wg.Wait()
	wall := time.Since(wallStart)
	select {
	case err := <-errCh:
		return nil, 0, err
	default:
	}
	return lat, wall, nil
}

func readQueries(path string) ([]string, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var qs []string
	for _, line := range strings.Split(string(b), "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		// Strip the trailing semicolon ClickHouse expects only with --multiquery.
		line = strings.TrimSuffix(line, ";")
		qs = append(qs, line)
	}
	return qs, nil
}

func percentile(sorted []float64, p float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	rank := p * float64(len(sorted)-1)
	lo := int(rank)
	hi := lo + 1
	if hi >= len(sorted) {
		return sorted[len(sorted)-1]
	}
	frac := rank - float64(lo)
	return sorted[lo]*(1-frac) + sorted[hi]*frac
}

func runMeasure(ctx context.Context, ch *CH, cfg *config, art *buildArtifact, sc *scenarioCfg, b *buildCfg, sls, conc, runIdx int, queriesFile string) (*cellResult, error) {
	cellKey := fmt.Sprintf("%s_%s_sls=%d_beam=%d_io=%d_conc=%d_r=%d",
		sc.name, b.name, sls, cfg.beamWidth, cfg.searchIOLimit, conc, runIdx)
	tagWarmup := cellTag(cfg.runID, "warmup", sc.name, b.name, sls, cfg.beamWidth, cfg.searchIOLimit, conc, runIdx)
	tagQPS := cellTag(cfg.runID, "qps", sc.name, b.name, sls, cfg.beamWidth, cfg.searchIOLimit, conc, runIdx)
	tagRecall := cellTag(cfg.runID, "recall", sc.name, b.name, sls, cfg.beamWidth, cfg.searchIOLimit, conc, runIdx)

	logf("cell %s", cellKey)

	// --- Warm-up ---
	_, _, err := runConcurrentBench(ctx, ch, queriesFile, cfg.warmupQueries, 1, tagWarmup)
	if err != nil {
		return nil, fmt.Errorf("warmup: %w", err)
	}

	// --- QPS / latency ---
	lats, wall, err := runConcurrentBench(ctx, ch, queriesFile, cfg.queriesPerCell, conc, tagQPS)
	if err != nil {
		return nil, fmt.Errorf("qps: %w", err)
	}
	qps := float64(cfg.queriesPerCell) / wall.Seconds()
	sort.Float64s(lats)
	p50 := percentile(lats, 0.5) * 1e6
	p95 := percentile(lats, 0.95) * 1e6
	p99 := percentile(lats, 0.99) * 1e6

	// Persist raw timings for offline analysis.
	benchPath := filepath.Join(cfg.runDir, "bench_logs", cellKey+".json")
	if err := os.MkdirAll(filepath.Dir(benchPath), 0o755); err != nil {
		return nil, err
	}
	benchDoc := map[string]any{
		"queries":   cfg.queriesPerCell,
		"conc":      conc,
		"wall_s":    wall.Seconds(),
		"qps":       qps,
		"p50_us":    p50,
		"p95_us":    p95,
		"p99_us":    p99,
		"latencies": lats,
	}
	bf, err := os.Create(benchPath)
	if err != nil {
		return nil, err
	}
	if err := json.NewEncoder(bf).Encode(benchDoc); err != nil {
		bf.Close()
		return nil, err
	}
	bf.Close()

	// --- Recall on the same query stream ---
	qids, err := ch.Query(ctx, fmt.Sprintf(
		"SELECT id FROM sift_query ORDER BY id LIMIT %d", cfg.queriesPerCell))
	if err != nil {
		return nil, err
	}
	matched := 0
	for _, qid := range strings.Fields(qids) {
		sql := fmt.Sprintf(`
SELECT length(arrayIntersect(
    (SELECT groupArray(id) FROM (
        SELECT id FROM sift_base
        ORDER BY L2Distance(v, materialize((SELECT v FROM sift_query WHERE id = %s))) ASC
        LIMIT %d
    )),
    (SELECT arraySlice(neighbors, 1, %d) FROM sift_gt WHERE query_id = %s)
))`, qid, cfg.k, cfg.k, qid)
		out, err := ch.QueryTagged(ctx, tagRecall, sql)
		if err != nil {
			return nil, fmt.Errorf("recall qid=%s: %w", qid, err)
		}
		m, err := strconv.Atoi(strings.TrimSpace(out))
		if err != nil {
			return nil, fmt.Errorf("recall qid=%s: parse %q: %w", qid, out, err)
		}
		matched += m
	}
	recall := float64(matched) / float64(cfg.queriesPerCell*cfg.k)

	// --- ProfileEvents medians via log_comment ---
	if err := ch.Exec(ctx, "SYSTEM FLUSH LOGS query_log"); err != nil {
		return nil, fmt.Errorf("flush query_log: %w", err)
	}
	medianFrom := func(field string) string {
		v, err := ch.Query(ctx, fmt.Sprintf(`
SELECT quantile(0.5)(toFloat64(ProfileEvents['%s']))
FROM system.query_log
WHERE log_comment = '%s' AND type = 'QueryFinish'`, field, tagQPS))
		if err != nil {
			return "0"
		}
		return v
	}
	searchCountP50 := medianFrom("DiskANNSearchCount")
	searchUSP50 := medianFrom("DiskANNSearchMicroseconds")
	resultsReturnedP50 := medianFrom("DiskANNSearchResultsReturned")

	// Notes: surface "index didn't fire" / "low recall" without aborting.
	var notes []string
	if searchCountP50 == "" || strings.HasPrefix(searchCountP50, "0") {
		notes = append(notes, "index_did_not_fire")
	}
	if recall < 0.5 {
		notes = append(notes, "low_recall")
	}

	return &cellResult{
		scenario: sc.name, buildCfg: b.name,
		sls: sls, beam: cfg.beamWidth, ioLimit: cfg.searchIOLimit,
		conc: conc, runIdx: runIdx,
		queries: cfg.queriesPerCell, k: cfg.k,
		recall: recall, qps: qps,
		p50us: p50, p95us: p95, p99us: p99,
		searchCountP50:     searchCountP50,
		searchUSP50:        searchUSP50,
		resultsReturnedP50: resultsReturnedP50,
		notes:              strings.Join(notes, ","),
	}, nil
}

func cellTag(runID, kind, scenario, buildCfg string, sls, beam, io, conc, runIdx int) string {
	return fmt.Sprintf("sift1m/%s/%s/%s/%s/sls=%d/beam=%d/io=%d/conc=%d/r=%d",
		runID, kind, scenario, buildCfg, sls, beam, io, conc, runIdx)
}

// ---------- Sweep TSV writer ----------

type sweepWriter struct {
	path string
	mu   sync.Mutex
}

var sweepHeader = strings.Join([]string{
	"run_id", "git_commit",
	"scenario", "build_cfg",
	"sls", "beam", "io_limit",
	"concurrency", "run_idx",
	"queries", "k",
	"recall", "qps", "p50_us", "p95_us", "p99_us",
	"build_seconds", "index_size_mb", "ann_groups",
	"diskann_search_count_p50", "diskann_search_us_p50", "diskann_results_returned_p50",
	"notes",
}, "\t")

func (w *sweepWriter) appendRow(cfg *config, art *buildArtifact, r *cellResult) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	exists := false
	if st, err := os.Stat(w.path); err == nil && st.Size() > 0 {
		exists = true
	}
	f, err := os.OpenFile(w.path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()
	if !exists {
		fmt.Fprintln(f, sweepHeader)
	}
	row := strings.Join([]string{
		cfg.runID, cfg.gitCommit,
		r.scenario, r.buildCfg,
		strconv.Itoa(r.sls), strconv.Itoa(r.beam), strconv.Itoa(r.ioLimit),
		strconv.Itoa(r.conc), strconv.Itoa(r.runIdx),
		strconv.Itoa(r.queries), strconv.Itoa(r.k),
		fmt.Sprintf("%.4f", r.recall),
		fmt.Sprintf("%.2f", r.qps),
		strconv.FormatInt(int64(r.p50us), 10),
		strconv.FormatInt(int64(r.p95us), 10),
		strconv.FormatInt(int64(r.p99us), 10),
		strconv.Itoa(art.BuildSeconds), art.IndexSizeMB, art.ANNGroups,
		r.searchCountP50, r.searchUSP50, r.resultsReturnedP50,
		r.notes,
	}, "\t")
	_, err = fmt.Fprintln(f, row)
	return err
}

// ---------- Top-level driver ----------

func main() {
	if err := mainErr(); err != nil {
		fmt.Fprintf(os.Stderr, "FATAL: %s\n", err)
		os.Exit(1)
	}
}

func mainErr() error {
	cfg, err := loadConfig()
	if err != nil {
		return err
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ch := newCH(cfg.httpURL, cfg.db)

	if err := writeServerMeta(ctx, ch, cfg); err != nil {
		return err
	}
	if err := loadQueryAndGT(ctx, ch, cfg); err != nil {
		return err
	}
	if err := runSanity(ctx, ch, cfg); err != nil {
		return err
	}

	// Enumerate cells.
	var scenarios []*scenarioCfg
	for _, name := range cfg.scenarios {
		s, err := loadScenario(cfg.dir, name)
		if err != nil {
			return err
		}
		scenarios = append(scenarios, s)
	}
	var builds []*buildCfg
	for _, name := range cfg.buildCfgs {
		b, err := loadBuildCfg(cfg.dir, name)
		if err != nil {
			return err
		}
		builds = append(builds, b)
	}

	totalCells := len(scenarios) * len(builds) * len(cfg.slsList) * len(cfg.concurrencies) * cfg.runs
	totalBuilds := len(scenarios) * len(builds) * len(cfg.slsList)
	logf("sweep starts; results -> %s", cfg.runDir)
	logf("plan: %d builds, %d measurement cells", totalBuilds, totalCells)

	queriesFile, err := renderQueriesFile(ctx, ch, cfg)
	if err != nil {
		return err
	}

	sw := &sweepWriter{path: filepath.Join(cfg.runDir, "sweep.tsv")}
	cellIdx := 0
	for _, sc := range scenarios {
		for _, b := range builds {
			for _, sls := range cfg.slsList {
				art, err := runBuild(ctx, ch, cfg, sc, b, sls)
				if err != nil {
					return err
				}
				for _, conc := range cfg.concurrencies {
					for r := 1; r <= cfg.runs; r++ {
						cellIdx++
						logf("[%d/%d] scenario=%s build=%s sls=%d conc=%d run=%d",
							cellIdx, totalCells, sc.name, b.name, sls, conc, r)
						res, err := runMeasure(ctx, ch, cfg, art, sc, b, sls, conc, r, queriesFile)
						if err != nil {
							return err
						}
						if err := sw.appendRow(cfg, art, res); err != nil {
							return err
						}
						notes := res.notes
						if notes == "" {
							notes = "-"
						}
						logf("cell %s_%s_sls=%d_conc=%d_r=%d: recall=%.4f qps=%.2f p99=%.0fus notes=%s",
							sc.name, b.name, sls, conc, r, res.recall, res.qps, res.p99us, notes)
					}
				}
			}
		}
	}
	_ = ch.Exec(ctx, "DROP TABLE IF EXISTS sift_base")
	logf("sweep complete")

	// Quick view.
	if data, err := os.ReadFile(sw.path); err == nil {
		buf := &bytes.Buffer{}
		buf.WriteString("\n=== quick view ===\n")
		// Print up to first 50 lines.
		count := 0
		for _, line := range strings.Split(string(data), "\n") {
			if count >= 50 {
				break
			}
			buf.WriteString(line)
			buf.WriteByte('\n')
			count++
		}
		buf.WriteString("\nResults: " + cfg.runDir + "\n")
		fmt.Println(buf.String())
	}

	return nil
}
