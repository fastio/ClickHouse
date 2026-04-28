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

// ---------- Dataset registry ----------

// datasetSpec captures everything that varies across ANN benchmark datasets:
// dimensionality, metric (also picks the SQL distance function), input file
// layout, and a default ClickHouse database name. The harness layout (table
// shapes, build cfg, scenarios) is identical across all entries here; only
// the reader script differs (HDF5 vs Big ANN binary).
type datasetSpec struct {
	name      string // canonical name (URL stem and HDF5/file stem)
	dim       int    // expected vector dimensionality
	metric    string // ClickHouse ANN index metric (DDL `metric = '%s'`): "L2" or "Cosine"
	distFn    string // SQL distance function: "L2Distance" or "cosineDistance"
	defaultDB string // default ClickHouse database name (overridable via --db)

	// format selects the reader script:
	//   "hdf5" -> hdf5_to_rowbinary.py (one HDF5 file holds base/query/gt)
	//   "bin"  -> bin_to_rowbinary.py  (Big ANN .u8bin/.i8bin/.fbin + .ibin GT, three separate files)
	format string

	// For format="bin": files are relative to <dir>/data and dtype is the on-disk
	// element type for base/query (we always widen to Float32 in the reader because
	// the ClickHouse ANN index only operates on Array(Float32)). For format="hdf5"
	// these are unset and the reader uses <name>.hdf5 with `--schema base/query/gt`.
	baseFile  string
	queryFile string
	gtFile    string
	dtype     string

	// rowsLimit, if > 0, caps the number of base rows ingested. The dataset's GT file
	// must already match this row count or recall numbers are meaningless. Defaults
	// to 0 (use whatever the file's header says, i.e. full dataset).
	rowsLimit int

	// Download URLs (consumed by download.sh). For format="hdf5" only baseURL is set
	// (single-file download); for format="bin" all three are required.
	baseURL  string
	queryURL string
	gtURL    string
}

var datasetRegistry = map[string]datasetSpec{
	"sift-128-euclidean":    {name: "sift-128-euclidean", dim: 128, metric: "L2", distFn: "L2Distance", defaultDB: "sift", format: "hdf5"},
	"gist-960-euclidean":    {name: "gist-960-euclidean", dim: 960, metric: "L2", distFn: "L2Distance", defaultDB: "gist", format: "hdf5"},
	"deep-image-96-angular": {name: "deep-image-96-angular", dim: 96, metric: "Cosine", distFn: "cosineDistance", defaultDB: "deep", format: "hdf5"},

	// Big ANN BIGANN-1B (NeurIPS '21 billion-scale ANN benchmark).
	// 128-d L2, 1B base rows (uint8 widened to float32 on ingest), 10K queries.
	// Pair this entry with run_bigann.sh, which sets KEEP_TABLE=1 (do NOT lose
	// a multi-day build), OPTIMIZE_BEFORE_BUILD=1 (force a single ANN group),
	// HTTP timeout 24h (INSERT/OPTIMIZE on 1B exceed the default 30m).
	"bigann-1B-euclidean": {
		name:      "bigann-1B-euclidean",
		dim:       128,
		metric:    "L2",
		distFn:    "L2Distance",
		defaultDB: "bigann",
		format:    "bin",
		baseFile:  "bigann/base.1B.u8bin",
		queryFile: "bigann/query.public.10K.u8bin",
		gtFile:    "bigann/GT.public.1B.ibin",
		dtype:     "uint8",
		rowsLimit: 0, // full 1B
		baseURL:   "https://dl.fbaipublicfiles.com/billion-scale-ann-benchmarks/bigann/base.1B.u8bin",
		queryURL:  "https://dl.fbaipublicfiles.com/billion-scale-ann-benchmarks/bigann/query.public.10K.u8bin",
		gtURL:     "https://dl.fbaipublicfiles.com/billion-scale-ann-benchmarks/bigann/GT.public.1B.ibin",
	},
}

func datasetNames() []string {
	out := make([]string, 0, len(datasetRegistry))
	for k := range datasetRegistry {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

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
	keepTable           bool
	buildTimeout        time.Duration
	optimizeBeforeBuild bool
	dataset             datasetSpec
	rowsLimit           int           // overrides dataset.rowsLimit when > 0; 0 = use dataset default
	httpTimeout         time.Duration // applied to the ClickHouse HTTP client; bump for 1B-class INSERT/OPTIMIZE
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
	dataset := flag.String("dataset", "sift-128-euclidean", fmt.Sprintf("ann-benchmarks dataset name; one of %v", datasetNames()))
	db := flag.String("db", "", "target ClickHouse database (default: dataset's defaultDB, e.g. sift / gist / deep)")
	dir := flag.String("dir", defaultDir, "harness root (configs/, scenarios/, data/)")
	hdf5 := flag.String("hdf5", "", "path to the dataset HDF5 file (default: <dir>/data/<dataset>.hdf5)")
	resultsDir := flag.String("results-dir", "", "where to write the run directory (default: <dir>/results)")

	k := flag.Int("k", 10, "Recall@K")
	queriesPerCell := flag.Int("queries-per-cell", 1000, "queries used for both recall and QPS, per cell")
	warmupQueries := flag.Int("warmup-queries", 200, "warm-up iterations before each measurement")
	runs := flag.Int("runs", 3, "repetitions per cell")

	slsList := flag.String("sls-list", "50,100,200,400,800", "comma-separated search_list_size sweep")
	beamWidth := flag.Int("beam-width", 8, "beam_width (constant per cell)")
	searchIOLimit := flag.Int("search-io-limit", 500, "search_io_limit (constant per cell). Caps the total number of disk reads per ANN search call; values <= dozens kill recall regardless of search_list_size, see disk_provider.rs")
	scenarios := flag.String("scenarios", "single_group,multi_group", "comma-separated scenario keys (matches scenarios/<key>.env)")
	buildCfgs := flag.String("build-cfgs", "paper", "comma-separated build cfg keys (matches configs/build_<key>.env)")
	concurrencies := flag.String("concurrencies", "", "comma-separated concurrency levels (default: 1,<nproc>)")

	sanityQueries := flag.Int("sanity-queries", 5, "random qids to probe before the sweep")
	sanityMissBudget := flag.Int("sanity-miss-budget", 1, "per-query miss tolerance for the brute-force sanity check")
	keepTable := flag.Bool("keep-table", false, "if set, do not drop the base table at sweep end, and reuse an existing fully-covered table (skips re-INSERT and re-BUILD ANN INDEX). Caller is responsible for dropping the table when build cfg or dataset changes.")
	buildTimeout := flag.Duration("build-timeout", 30*time.Minute, "timeout for waiting on full ANN coverage after SYSTEM BUILD ANN INDEX. Bump for larger datasets (e.g. deep-image-96-angular has 9.99M rows and may need 1h+; bigann-1B-euclidean may need days).")
	optimizeBeforeBuild := flag.Bool("optimize-before-build", false, "if set, run OPTIMIZE TABLE base FINAL between INSERT and SYSTEM BUILD ANN INDEX so the table has a single part (and therefore a single ANN group). Use this with single_group_large / single_group_billion scenarios when you want exactly one ANN group on multi-million-or-billion-row datasets; otherwise ANN groups stay part-bound, not row-bound.")
	rowsLimit := flag.Int("rows-limit", 0, "for format='bin' datasets, override how many base rows are ingested (0 = use the dataset's registered default). Recall numbers are only meaningful when the GT file matches this row count.")
	httpTimeout := flag.Duration("http-timeout", 30*time.Minute, "ClickHouse HTTP client timeout. Default works for SIFT/GIST/DEEP; bump to 12h-24h for billion-scale INSERT and OPTIMIZE FINAL which the default would cut off mid-stream.")

	flag.Parse()

	spec, ok := datasetRegistry[*dataset]
	if !ok {
		return nil, fmt.Errorf("unknown --dataset %q; supported: %v", *dataset, datasetNames())
	}
	resolvedDB := *db
	if resolvedDB == "" {
		resolvedDB = spec.defaultDB
	}

	cfg := &config{
		httpURL:          fmt.Sprintf("http://%s:%d", *host, *httpPort),
		clickhouseBinary: *binary,
		tcpPort:          *tcpPort,
		db:               resolvedDB,
		dataset:          spec,
		dir:              *dir,
		k:                *k,
		queriesPerCell:   *queriesPerCell,
		warmupQueries:    *warmupQueries,
		runs:             *runs,
		beamWidth:        *beamWidth,
		searchIOLimit:    *searchIOLimit,
		sanityQueries:    *sanityQueries,
		sanityMissBudget: *sanityMissBudget,
		keepTable:           *keepTable,
		buildTimeout:        *buildTimeout,
		optimizeBeforeBuild: *optimizeBeforeBuild,
		rowsLimit:           *rowsLimit,
		httpTimeout:         *httpTimeout,
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
	switch spec.format {
	case "hdf5", "":
		// Single HDF5 file holds base/query/gt; --hdf5 overrides path.
		if *hdf5 != "" {
			cfg.hdf5 = *hdf5
		} else {
			cfg.hdf5 = filepath.Join(cfg.dataDir, spec.name+".hdf5")
		}
		if _, err := os.Stat(cfg.hdf5); err != nil {
			return nil, fmt.Errorf("missing %s - run download.sh --dataset %s first", cfg.hdf5, spec.name)
		}
	case "bin":
		// Three separate files (base/query/gt). --hdf5 is ignored.
		for _, rel := range []string{spec.baseFile, spec.queryFile, spec.gtFile} {
			full := filepath.Join(cfg.dataDir, rel)
			if _, err := os.Stat(full); err != nil {
				return nil, fmt.Errorf("missing %s - run download.sh --dataset %s first", full, spec.name)
			}
		}
		cfg.hdf5 = filepath.Join(cfg.dataDir, spec.baseFile) // surface base file in server_meta.txt
	default:
		return nil, fmt.Errorf("dataset %q: unknown format %q", spec.name, spec.format)
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

func newCH(rawURL, db string, timeout time.Duration) *CH {
	if timeout <= 0 {
		timeout = 30 * time.Minute
	}
	return &CH{
		url: rawURL,
		db:  db,
		client: &http.Client{
			Timeout: timeout,
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
	fmt.Fprintf(f, "dataset: %s\n", cfg.dataset.name)
	fmt.Fprintf(f, "dataset_format: %s\n", cfg.dataset.format)
	if cfg.dataset.format == "bin" {
		fmt.Fprintf(f, "dataset_dtype: %s\n", cfg.dataset.dtype)
		fmt.Fprintf(f, "dataset_base_file: %s\n", cfg.dataset.baseFile)
		fmt.Fprintf(f, "dataset_query_file: %s\n", cfg.dataset.queryFile)
		fmt.Fprintf(f, "dataset_gt_file: %s\n", cfg.dataset.gtFile)
	}
	effectiveRowsLimit := cfg.dataset.rowsLimit
	if cfg.rowsLimit > 0 {
		effectiveRowsLimit = cfg.rowsLimit
	}
	fmt.Fprintf(f, "rows_limit: %d\n", effectiveRowsLimit)
	fmt.Fprintf(f, "http_timeout: %s\n", cfg.httpTimeout)
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

// loadDatasetStream spawns the appropriate reader script (HDF5 or Big ANN binary)
// for the given schema and pipes its RowBinary output back through stdout. The
// caller is responsible for streaming the pipe to ClickHouse and Wait()ing on
// the returned cmd.
func loadDatasetStream(ctx context.Context, cfg *config, schema string) (*exec.Cmd, io.ReadCloser, error) {
	switch cfg.dataset.format {
	case "hdf5", "":
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

	case "bin":
		script := filepath.Join(cfg.dir, "bin_to_rowbinary.py")
		var srcRel string
		switch schema {
		case "base":
			srcRel = cfg.dataset.baseFile
		case "query":
			srcRel = cfg.dataset.queryFile
		case "gt":
			srcRel = cfg.dataset.gtFile
		default:
			return nil, nil, fmt.Errorf("loadDatasetStream(bin): unknown schema %q", schema)
		}
		args := []string{script, filepath.Join(cfg.dataDir, srcRel), "--schema", schema}
		if schema != "gt" {
			args = append(args, "--dtype", cfg.dataset.dtype)
		}
		// rowsLimit only applies to base; query/gt use whatever the file says.
		// CLI flag takes precedence over the dataset's registered default.
		if schema == "base" {
			lim := cfg.dataset.rowsLimit
			if cfg.rowsLimit > 0 {
				lim = cfg.rowsLimit
			}
			if lim > 0 {
				args = append(args, "--rows-limit", strconv.Itoa(lim))
			}
		}
		cmd := exec.CommandContext(ctx, "python3", args...)
		cmd.Stderr = os.Stderr
		stdout, err := cmd.StdoutPipe()
		if err != nil {
			return nil, nil, err
		}
		if err := cmd.Start(); err != nil {
			return nil, nil, err
		}
		return cmd, stdout, nil

	default:
		return nil, nil, fmt.Errorf("loadDatasetStream: unknown format %q", cfg.dataset.format)
	}
}

func loadQueryAndGT(ctx context.Context, ch *CH, cfg *config) error {
	if err := ch.Exec(ctx, "CREATE DATABASE IF NOT EXISTS "+cfg.db); err != nil {
		return err
	}
	logf("loading queries and gt for dataset %s (dim=%d, metric=%s)", cfg.dataset.name, cfg.dataset.dim, cfg.dataset.metric)

	for _, sql := range []string{
		"DROP TABLE IF EXISTS queries",
		"DROP TABLE IF EXISTS gt",
		"CREATE TABLE queries (id UInt32, v Array(Float32)) ENGINE = MergeTree ORDER BY id",
		"CREATE TABLE gt    (query_id UInt32, neighbors Array(UInt32)) ENGINE = MergeTree ORDER BY query_id",
	} {
		if err := ch.Exec(ctx, sql); err != nil {
			return err
		}
	}

	for _, p := range []struct{ schema, table string }{
		{"query", "queries"},
		{"gt", "gt"},
	} {
		cmd, stdout, err := loadDatasetStream(ctx, cfg, p.schema)
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

	queryRows, err := ch.Query(ctx, "SELECT count() FROM queries")
	if err != nil {
		return err
	}
	gtRows, err := ch.Query(ctx, "SELECT count() FROM gt")
	if err != nil {
		return err
	}
	logf("queries=%s rows, gt=%s rows", queryRows, gtRows)
	if queryRows != gtRows || queryRows == "0" {
		return fmt.Errorf("queries (%s) / gt (%s) row counts mismatch or empty; HDF5 may be truncated", queryRows, gtRows)
	}
	return nil
}

// ---------- Sanity ----------

func runSanity(ctx context.Context, ch *CH, cfg *config) error {
	// --keep-table: if a populated base already exists, run sanity against it instead of
	// re-loading 500MB. Brute-force search is forced via vector_search_force_brute_force=1 so
	// any pre-existing ANN index on the table is bypassed.
	reuse := false
	if cfg.keepTable {
		rows, err := ch.QueryNoLog(ctx, fmt.Sprintf(
			"SELECT count() FROM system.tables WHERE database = '%s' AND name = 'base'", cfg.db))
		if err == nil && rows == "1" {
			reuse = true
		}
	}

	if reuse {
		logf("sanity: reusing existing base (--keep-table)")
	} else {
		logf("creating throwaway base for sanity check (no ANN index)")
		if err := ch.Exec(ctx, "DROP TABLE IF EXISTS base"); err != nil {
			return err
		}
		if err := ch.Exec(ctx, "CREATE TABLE base (id UInt64, v Array(Float32)) ENGINE = MergeTree ORDER BY id"); err != nil {
			return err
		}
		cmd, stdout, err := loadDatasetStream(ctx, cfg, "base")
		if err != nil {
			return err
		}
		err = ch.Insert(ctx, "INSERT INTO base FORMAT RowBinary", stdout)
		if cerr := cmd.Wait(); cerr != nil && err == nil {
			err = cerr
		}
		if err != nil {
			return fmt.Errorf("INSERT base: %w", err)
		}
	}

	logf("brute-force sanity: %d queries, K=%d, miss budget=%d per query", cfg.sanityQueries, cfg.k, cfg.sanityMissBudget)
	qids, err := ch.Query(ctx, fmt.Sprintf(
		"SELECT id FROM queries ORDER BY rand() LIMIT %d", cfg.sanityQueries))
	if err != nil {
		return err
	}

	mismatches := 0
	for _, qid := range strings.Fields(qids) {
		// Sanity: brute-force top-K ≡ HDF5 ground truth (within `sanityMissBudget`).
		// `vector_search_force_brute_force = 1` is on the outer SELECT so it propagates
		// to the inner read; settings on nested subqueries are silently ignored by the
		// optimizer.
		sql := fmt.Sprintf(`
WITH
    (SELECT v FROM queries WHERE id = %s) AS qv,
    (SELECT arraySlice(neighbors, 1, %d) FROM gt WHERE query_id = %s) AS gt
SELECT %d - length(arrayIntersect(
    gt,
    (SELECT groupArray(id) FROM (
        SELECT id FROM base
        ORDER BY %s(v, qv) ASC
        LIMIT %d
    ))
))
SETTINGS vector_search_force_brute_force = 1`, qid, cfg.k, qid, cfg.k, cfg.dataset.distFn, cfg.k)
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
	if reuse {
		return nil
	}
	return ch.Exec(ctx, "DROP TABLE base")
}

// ---------- Build phase ----------

type buildArtifact struct {
	BuildSeconds  int    `json:"build_seconds"`
	IndexSizeMB   string `json:"index_size_mb"`
	ANNGroups     string `json:"ann_groups"`
}

// `ddlSearchListSize` is the value baked into the DDL's `search_list_size`. It is the fallback
// the searcher uses when a query does not override `ann_search_list_size`. The sweep itself
// always overrides per cell, so this only matters for ad-hoc queries hitting the table after
// the sweep.
const ddlSearchListSize = 100

func runBuild(ctx context.Context, ch *CH, cfg *config, sc *scenarioCfg, b *buildCfg) (*buildArtifact, error) {
	logf("build(%s/%s) ddl_sls=%d beam=%d io_limit=%d", sc.name, b.name, ddlSearchListSize, cfg.beamWidth, cfg.searchIOLimit)

	// --keep-table: if base already exists with full ANN coverage, reuse it as-is.
	// Trade-off: this trusts the caller not to silently change build cfg between runs;
	// when in doubt, drop the table manually before re-running.
	if cfg.keepTable {
		exists, err := ch.QueryNoLog(ctx, fmt.Sprintf(
			"SELECT count() FROM system.tables WHERE database = '%s' AND name = 'base'", cfg.db))
		if err == nil && exists == "1" {
			cov, covErr := ch.QueryNoLog(ctx, fmt.Sprintf(
				`SELECT
                    tupleElement(tableANNCoverage('%s', 'base'), 'covered') = tupleElement(tableANNCoverage('%s', 'base'), 'total')
                    AND tupleElement(tableANNCoverage('%s', 'base'), 'total') > 0`,
				cfg.db, cfg.db, cfg.db))
			if covErr == nil && cov == "1" {
				logf("build(%s/%s) reusing existing base (full ANN coverage)", sc.name, b.name)
				return collectBuildArtifact(ctx, ch, cfg, sc, b, 0)
			}
			logf("build(%s/%s) existing base lacks full coverage, rebuilding", sc.name, b.name)
		}
	}

	if err := ch.Exec(ctx, "DROP TABLE IF EXISTS base"); err != nil {
		return nil, err
	}

	createSQL := fmt.Sprintf(`
CREATE TABLE base
(
    id UInt64,
    v  Array(Float32),
    INDEX idx_v v TYPE ann(
        dim                    = %d,
        metric                 = '%s',
        max_degree             = %d,
        build_search_list_size = %d,
        alpha                  = %g,
        pq_chunks              = %d,
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
		cfg.dataset.dim, cfg.dataset.metric,
		b.maxDegree, b.buildSearchListSize, b.alpha, b.pqChunks,
		ddlSearchListSize, cfg.beamWidth, cfg.searchIOLimit,
		b.numThreads, b.buildRAMLimitGB, b.hashSeed,
		sc.annGroupMinRows, sc.annGroupMaxRows, sc.annGroupMaxParts,
	)
	if err := ch.Exec(ctx, createSQL); err != nil {
		return nil, fmt.Errorf("create base: %w", err)
	}

	cmd, stdout, err := loadDatasetStream(ctx, cfg, "base")
	if err != nil {
		return nil, err
	}
	if err := ch.Insert(ctx, "INSERT INTO base FORMAT RowBinary", stdout); err != nil {
		_ = cmd.Wait()
		return nil, fmt.Errorf("insert base: %w", err)
	}
	if err := cmd.Wait(); err != nil {
		return nil, err
	}

	// Force a single part before BUILD ANN INDEX. ANN groups are part-bound: each
	// active part becomes its own ANN group regardless of ann_group_max_rows. With
	// streamed INSERTs producing multiple parts, a 10M-row dataset like
	// deep-image-96-angular ends up with 5 groups; single_group_large.env's higher
	// row cap does not by itself merge them. OPTIMIZE FINAL coalesces parts so the
	// subsequent BUILD ANN INDEX produces exactly one group.
	if cfg.optimizeBeforeBuild {
		logf("OPTIMIZE TABLE base FINAL (forcing single part before BUILD)")
		optStart := time.Now()
		if err := ch.Exec(ctx, "OPTIMIZE TABLE base FINAL"); err != nil {
			return nil, fmt.Errorf("OPTIMIZE TABLE base FINAL: %w", err)
		}
		logf("OPTIMIZE done in %ds", int(time.Since(optStart).Seconds()))
	}

	start := time.Now()
	if err := ch.Exec(ctx, "SYSTEM BUILD ANN INDEX base"); err != nil {
		return nil, fmt.Errorf("SYSTEM BUILD ANN INDEX: %w", err)
	}
	if err := waitFullCoverage(ctx, ch, cfg.db, "base", cfg.buildTimeout); err != nil {
		return nil, err
	}
	buildSeconds := int(time.Since(start).Seconds())

	return collectBuildArtifact(ctx, ch, cfg, sc, b, buildSeconds)
}

// collectBuildArtifact gathers the per-build metrics and writes the .kv provenance file.
// Shared between a fresh build and a --keep-table reuse path.
func collectBuildArtifact(ctx context.Context, ch *CH, cfg *config, sc *scenarioCfg, b *buildCfg, buildSeconds int) (*buildArtifact, error) {
	indexSizeMB, err := ch.Query(ctx, fmt.Sprintf(
		`SELECT round(sum(secondary_indices_compressed_bytes) / 1048576.0, 1)
		 FROM system.parts
		 WHERE database = '%s' AND table = 'base' AND active`, cfg.db))
	if err != nil {
		return nil, err
	}
	groups, err := ch.Query(ctx, fmt.Sprintf(
		"SELECT tupleElement(tableANNCoverage('%s', 'base'), 'total')", cfg.db))
	if err != nil {
		return nil, err
	}

	art := &buildArtifact{BuildSeconds: buildSeconds, IndexSizeMB: indexSizeMB, ANNGroups: groups}

	kvPath := filepath.Join(cfg.runDir, "build", fmt.Sprintf(
		"%s_%s.kv", sc.name, b.name))
	if err := os.MkdirAll(filepath.Dir(kvPath), 0o755); err != nil {
		return nil, err
	}
	kvF, err := os.Create(kvPath)
	if err != nil {
		return nil, err
	}
	fmt.Fprintf(kvF, "scenario=%s\n", sc.name)
	fmt.Fprintf(kvF, "build_cfg=%s\n", b.name)
	fmt.Fprintf(kvF, "build_seconds=%d\n", buildSeconds)
	fmt.Fprintf(kvF, "index_size_mb=%s\n", indexSizeMB)
	fmt.Fprintf(kvF, "ann_groups=%s\n", groups)
	fmt.Fprintf(kvF, "max_degree=%d\n", b.maxDegree)
	fmt.Fprintf(kvF, "build_search_list_size=%d\n", b.buildSearchListSize)
	fmt.Fprintf(kvF, "alpha=%g\n", b.alpha)
	fmt.Fprintf(kvF, "pq_chunks=%d\n", b.pqChunks)
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
SELECT 'SELECT id FROM base ORDER BY %s(v, [' ||
       arrayStringConcat(arrayMap(x -> toString(x), v), ',') ||
       ']::Array(Float32)) LIMIT %d FORMAT Null;'
FROM queries
ORDER BY id
LIMIT %d`, cfg.dataset.distFn, cfg.k, cfg.queriesPerCell)
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
// per query so ProfileEvents can be retrieved by the same tag. `extraSettings`
// (e.g. `ann_search_list_size`, `ann_beam_width`) are merged into the HTTP
// query parameters of every request so per-cell search-time tuning does not
// require regenerating the queries file.
func runConcurrentBench(ctx context.Context, ch *CH, queriesFile string, iters, conc int, logComment string, extraSettings url.Values) ([]float64, time.Duration, error) {
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
			for k, vs := range extraSettings {
				for _, v := range vs {
					settings.Add(k, v)
				}
			}
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
	tagWarmup := cellTag(cfg.dataset.name, cfg.runID, "warmup", sc.name, b.name, sls, cfg.beamWidth, cfg.searchIOLimit, conc, runIdx)
	tagQPS := cellTag(cfg.dataset.name, cfg.runID, "qps", sc.name, b.name, sls, cfg.beamWidth, cfg.searchIOLimit, conc, runIdx)
	tagRecall := cellTag(cfg.dataset.name, cfg.runID, "recall", sc.name, b.name, sls, cfg.beamWidth, cfg.searchIOLimit, conc, runIdx)

	logf("cell %s", cellKey)

	// Per-cell ANN search settings. The DDL's `search_list_size` / `beam_width` are now
	// fixed defaults; per-cell tuning is injected here so a single built index can be
	// swept across the whole `slsList` without re-builds.
	annSettings := url.Values{}
	annSettings.Set("try_use_ann_search", "1")
	annSettings.Set("ann_search_list_size", strconv.Itoa(sls))
	annSettings.Set("ann_beam_width", strconv.Itoa(cfg.beamWidth))
	annSettings.Set("vector_search_force_brute_force", "0")

	// --- Warm-up ---
	_, _, err := runConcurrentBench(ctx, ch, queriesFile, cfg.warmupQueries, 1, tagWarmup, annSettings)
	if err != nil {
		return nil, fmt.Errorf("warmup: %w", err)
	}

	// --- QPS / latency ---
	lats, wall, err := runConcurrentBench(ctx, ch, queriesFile, cfg.queriesPerCell, conc, tagQPS, annSettings)
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
		"SELECT id FROM queries ORDER BY id LIMIT %d", cfg.queriesPerCell))
	if err != nil {
		return nil, err
	}
	matched := 0
	for _, qid := range strings.Fields(qids) {
		// `SETTINGS` go on the outer SELECT so they propagate to the inner ANN search.
		// `materialize(...)` was previously wrapped around the reference vector to "force
		// scalar subquery evaluation", but the optimizer in `useANNSearch.cpp` only matches
		// `L2Distance(v, <constant array column>)` and treats `materialize(<scalar>)` as a
		// runtime function call that disqualifies the rewrite — silently falling back to a
		// brute-force scan and reporting brute-force recall as if it were ANN's. Keep the
		// reference vector as a plain scalar subquery so the optimizer folds it to a
		// `ColumnConst` and routes through the ANN index.
		sql := fmt.Sprintf(`
SELECT length(arrayIntersect(
    (SELECT groupArray(id) FROM (
        SELECT id FROM base
        ORDER BY %s(v, (SELECT v FROM queries WHERE id = %s)) ASC
        LIMIT %d
    )),
    (SELECT arraySlice(neighbors, 1, %d) FROM gt WHERE query_id = %s)
))
SETTINGS try_use_ann_search = 1,
         ann_search_list_size = %d,
         ann_beam_width = %d,
         vector_search_force_brute_force = 0`,
			cfg.dataset.distFn, qid, cfg.k, cfg.k, qid, sls, cfg.beamWidth)
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

func cellTag(dataset, runID, kind, scenario, buildCfg string, sls, beam, io, conc, runIdx int) string {
	return fmt.Sprintf("%s/%s/%s/%s/%s/sls=%d/beam=%d/io=%d/conc=%d/r=%d",
		dataset, runID, kind, scenario, buildCfg, sls, beam, io, conc, runIdx)
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

	// Bootstrap the target DB before pinning ?database=<db> on the main client.
	// ClickHouse rejects every query (including SELECT version()) when the default
	// database does not exist, so we cannot rely on a CREATE DATABASE issued through
	// the pinned client.
	chBoot := newCH(cfg.httpURL, "", cfg.httpTimeout)
	if err := chBoot.Exec(ctx, "CREATE DATABASE IF NOT EXISTS "+cfg.db); err != nil {
		return fmt.Errorf("create database %s: %w", cfg.db, err)
	}

	ch := newCH(cfg.httpURL, cfg.db, cfg.httpTimeout)

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
	totalBuilds := len(scenarios) * len(builds)
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
			// Build the index once per (scenario, build_cfg). The DDL's `search_list_size`
			// is fixed; per-cell `sls` is injected at query time via `ann_search_list_size`.
			// (`max_degree`, `build_search_list_size`, `alpha`, `pq_chunks` are the only
			// inputs to `params_hash` — sls / beam / io_limit do not invalidate the graph.)
			art, err := runBuild(ctx, ch, cfg, sc, b)
			if err != nil {
				return err
			}
			for _, sls := range cfg.slsList {
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
	if !cfg.keepTable {
		_ = ch.Exec(ctx, "DROP TABLE IF EXISTS base")
	}
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
