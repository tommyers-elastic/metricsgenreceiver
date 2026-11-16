package metricsgenreceiver

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/confmap/confmaptest"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/receiver/receivertest"
)

// updateGoldenEnv regenerates the golden entries for the current GOARCH when set to "1".
const updateGoldenEnv = "UPDATE_GOLDEN"

var (
	goldenDir         = filepath.Join("testdata", "golden")
	goldenSummaryPath = filepath.Join(goldenDir, "summary.json")
)

// goldenEntry summarises the complete output of one receiver run.
type goldenEntry struct {
	SHA256     string `json:"sha256"`
	Metrics    int    `json:"metrics"`
	DataPoints int    `json:"datapoints"`
}

// goldenSummary maps case name -> GOARCH -> entry. Output is stored per architecture because
// floating point results can differ in the last bit between arm64 (which fuses multiply-add)
// and amd64.
type goldenSummary map[string]map[string]goldenEntry

type goldenCase struct {
	name   string
	config func(t *testing.T) *Config
	// fullOutputFile, when set, also stores the complete serialised output (per GOARCH) so that a
	// mismatch produces a readable diff.
	fullOutputFile string
	// orderInsensitive hashes the serialised batches in sorted order, for cases whose emission
	// order is not deterministic.
	orderInsensitive bool
}

// TestGoldenOutput pins the generated output of existing configurations byte for byte.
// All cases use real_time false. With concurrency 0 or 1 the emission order is deterministic and
// pinned exactly. With concurrency > 1 each goroutine draws from its own RNG, so every emitted
// batch is deterministic but batches from different goroutines interleave in a nondeterministic
// order; those cases are pinned order-insensitively, over the sorted serialised batches.
func TestGoldenOutput(t *testing.T) {
	if !goldenSupportedArch() {
		t.Skipf("golden output is only recorded for arm64 and amd64, not %s", runtime.GOARCH)
	}
	update := os.Getenv(updateGoldenEnv) == "1"
	summary := readGoldenSummary(t, goldenSummaryPath)

	for _, gc := range goldenCases() {
		t.Run(gc.name, func(t *testing.T) {
			cfg := gc.config(t)
			require.NoError(t, cfg.Validate())
			allMetrics := runReceiverToCompletion(t, cfg)
			output := marshalMetricsOutput(t, allMetrics)
			if gc.orderInsensitive {
				output = sortLines(output)
			}
			got := summarizeOutput(allMetrics, output)
			fullOutputPath := ""
			if gc.fullOutputFile != "" {
				fullOutputPath = filepath.Join(goldenDir, gc.fullOutputFile+"."+runtime.GOARCH+".ndjson")
			}

			if update {
				if summary[gc.name] == nil {
					summary[gc.name] = map[string]goldenEntry{}
				}
				summary[gc.name][runtime.GOARCH] = got
				if fullOutputPath != "" {
					require.NoError(t, os.WriteFile(fullOutputPath, output.Bytes(), 0o644))
				}
				return
			}

			want, ok := summary[gc.name][runtime.GOARCH]
			require.True(t, ok, "no golden entry for %s on %s; run with %s=1 to record it", gc.name, runtime.GOARCH, updateGoldenEnv)
			assert.Equal(t, want, got)
			if fullOutputPath != "" {
				wantOutput, err := os.ReadFile(fullOutputPath)
				require.NoError(t, err)
				assert.Equal(t, string(wantOutput), output.String())
			}
		})
	}

	if update {
		writeGoldenSummary(t, goldenSummaryPath, summary)
	}
}

func goldenSupportedArch() bool {
	return runtime.GOARCH == "arm64" || runtime.GOARCH == "amd64"
}

func goldenCases() []goldenCase {
	cases := []goldenCase{
		{
			name:           "config_yaml",
			config:         loadTestdataConfigYaml,
			fullOutputFile: "config_yaml",
		},
		{
			name: "readme_hostmetrics",
			config: func(_ *testing.T) *Config {
				cfg := NewFactory().CreateDefaultConfig().(*Config)
				cfg.StartTime = time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
				cfg.EndTime = cfg.StartTime.Add(2 * time.Minute)
				cfg.Interval = 10 * time.Second
				cfg.Seed = 123
				cfg.Scenarios = []ScenarioCfg{{Path: "builtin/hostmetrics", Scale: 100}}
				return cfg
			},
		},
		{
			name: "two_scenarios",
			config: func(_ *testing.T) *Config {
				cfg := goldenMatrixBaseConfig()
				cfg.Scenarios = []ScenarioCfg{
					{Path: "builtin/hostmetrics", Scale: 3},
					{Path: "testdata/generation-hints-template", Scale: 2},
				}
				return cfg
			},
		},
	}

	for _, tmpl := range goldenConcurrencyTemplates() {
		for _, concurrency := range []int{1, 2} {
			for _, jitter := range []time.Duration{0, 5 * time.Millisecond} {
				name := fmt.Sprintf("concurrency:%s:%d", tmpl.name, concurrency)
				if jitter > 0 {
					name += ":jitter"
				}
				cases = append(cases, goldenCase{
					name: name,
					config: func(_ *testing.T) *Config {
						cfg := goldenMatrixBaseConfig()
						cfg.IntervalJitterStdDev = jitter
						cfg.Scenarios = []ScenarioCfg{{Path: tmpl.path, Scale: 4, Concurrency: concurrency, TemplateVars: tmpl.templateVars}}
						return cfg
					},
					orderInsensitive: concurrency > 1,
				})
			}
		}
	}

	for _, tmpl := range goldenMatrixTemplates() {
		for _, variant := range goldenMatrixVariants() {
			cases = append(cases, goldenCase{
				name: "matrix:" + tmpl.name + ":" + variant.name,
				config: func(_ *testing.T) *Config {
					cfg := goldenMatrixBaseConfig()
					cfg.Scenarios = []ScenarioCfg{{Path: tmpl.path, Scale: 4, TemplateVars: tmpl.templateVars}}
					variant.apply(cfg)
					return cfg
				},
			})
		}
	}
	return cases
}

type goldenTemplate struct {
	name         string
	path         string
	templateVars map[string]any
}

func goldenMatrixTemplates() []goldenTemplate {
	return []goldenTemplate{
		{name: "hostmetrics", path: "builtin/hostmetrics"},
		{name: "kubeletstats-node", path: "builtin/kubeletstats-node"},
		{name: "kubeletstats-pod", path: "builtin/kubeletstats-pod"},
		{name: "tsbs-devops", path: "builtin/tsbs-devops"},
		{name: "elasticapm-service-metrics", path: "builtin/elasticapm-service-metrics"},
		{
			name:         "elasticapm-span-destination-metrics",
			path:         "builtin/elasticapm-span-destination-metrics",
			templateVars: map[string]any{"destinations": 10},
		},
		{
			name:         "elasticapm-transaction-metrics",
			path:         "builtin/elasticapm-transaction-metrics",
			templateVars: map[string]any{"services": 2, "transactions": 10},
		},
		{
			name:         "simple",
			path:         "builtin/simple",
			templateVars: map[string]any{"gauge_pct": 1, "gauge_int": 1, "counter": 1},
		},
		{name: "nginx", path: "builtin/nginx"},
		{name: "node_exporter", path: "builtin/node_exporter"},
		{name: "metricstemplate", path: "testdata/metricstemplate"},
		{name: "histogram-template", path: "testdata/histogram-template"},
		{name: "generation-hints-template", path: "testdata/generation-hints-template"},
	}
}

// goldenConcurrencyTemplates are the templates pinned with concurrency > 0. histogram-template
// covers explicit-bucket histograms, which draw from the per-goroutine RNG on emission.
func goldenConcurrencyTemplates() []goldenTemplate {
	return []goldenTemplate{
		{name: "hostmetrics", path: "builtin/hostmetrics"},
		{name: "tsbs-devops", path: "builtin/tsbs-devops"},
		{name: "histogram-template", path: "testdata/histogram-template"},
	}
}

type goldenVariant struct {
	name  string
	apply func(cfg *Config)
}

func goldenMatrixVariants() []goldenVariant {
	return []goldenVariant{
		{name: "plain", apply: func(_ *Config) {}},
		{name: "jitter", apply: func(cfg *Config) { cfg.IntervalJitterStdDev = 5 * time.Millisecond }},
		{name: "churn-samples", apply: func(cfg *Config) { cfg.Scenarios[0].Churn = &ChurnCfg{SamplesPerSeries: 3} }},
		{name: "churn-lifetime", apply: func(cfg *Config) { cfg.Scenarios[0].Churn = &ChurnCfg{InstanceLifetime: 150 * time.Second} }},
		{name: "instance-offset", apply: func(cfg *Config) { cfg.InstanceOffset = 7 }},
		{name: "temporality-delta", apply: func(cfg *Config) { cfg.Scenarios[0].TemporalityOverride = "delta" }},
		{name: "temporality-cumulative", apply: func(cfg *Config) { cfg.Scenarios[0].TemporalityOverride = "cumulative" }},
		{name: "histogram-exponential", apply: func(cfg *Config) { cfg.Scenarios[0].HistogramOverride = "exponential" }},
		{name: "distribution", apply: func(cfg *Config) {
			cfg.Distribution.MedianMonotonicSum = 7
			cfg.Distribution.StdDevGaugePct = 0.2
			cfg.Distribution.StdDev = 3.5
		}},
	}
}

// goldenMatrixBaseConfig is a 5 minute window at 30s intervals with the factory defaults.
func goldenMatrixBaseConfig() *Config {
	cfg := NewFactory().CreateDefaultConfig().(*Config)
	cfg.StartTime = time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	cfg.EndTime = cfg.StartTime.Add(5 * time.Minute)
	cfg.Interval = 30 * time.Second
	cfg.Seed = 42
	return cfg
}

// loadTestdataConfigYaml loads testdata/config.yaml the same way TestLoadConfig does.
func loadTestdataConfigYaml(t *testing.T) *Config {
	cm, err := confmaptest.LoadConf(filepath.Join("testdata", "config.yaml"))
	require.NoError(t, err)
	cfg := NewFactory().CreateDefaultConfig().(*Config)
	sub, err := cm.Sub("metricsgen")
	require.NoError(t, err)
	require.NoError(t, sub.Unmarshal(cfg))
	return cfg
}

// runReceiverToCompletion runs the receiver until it has generated all intervals and returns
// every emitted pmetric.Metrics in emission order.
func runReceiverToCompletion(t *testing.T, cfg *Config) []pmetric.Metrics {
	t.Helper()
	sink := new(consumertest.MetricsSink)
	rcv, err := NewFactory().CreateMetrics(context.Background(), receivertest.NewNopSettings(typ), cfg, sink)
	require.NoError(t, err)
	require.NoError(t, rcv.Start(context.Background(), nil))
	waitForCompletion(rcv)
	require.NoError(t, rcv.Shutdown(context.Background()))
	return sink.AllMetrics()
}

// marshalMetricsOutput serialises each pmetric.Metrics as one line of OTLP JSON.
func marshalMetricsOutput(t *testing.T, allMetrics []pmetric.Metrics) *bytes.Buffer {
	t.Helper()
	marshaler := &pmetric.JSONMarshaler{}
	buf := new(bytes.Buffer)
	for _, md := range allMetrics {
		b, err := marshaler.MarshalMetrics(md)
		require.NoError(t, err)
		buf.Write(b)
		buf.WriteByte('\n')
	}
	return buf
}

// sortLines returns the lines of output sorted, each still terminated by a newline.
func sortLines(output *bytes.Buffer) *bytes.Buffer {
	lines := bytes.SplitAfter(output.Bytes(), []byte("\n"))
	if len(lines) > 0 && len(lines[len(lines)-1]) == 0 {
		lines = lines[:len(lines)-1]
	}
	sort.Slice(lines, func(i, j int) bool { return bytes.Compare(lines[i], lines[j]) < 0 })
	return bytes.NewBuffer(bytes.Join(lines, nil))
}

func summarizeOutput(allMetrics []pmetric.Metrics, output *bytes.Buffer) goldenEntry {
	sum := sha256.Sum256(output.Bytes())
	entry := goldenEntry{SHA256: hex.EncodeToString(sum[:]), Metrics: len(allMetrics)}
	for _, md := range allMetrics {
		entry.DataPoints += md.DataPointCount()
	}
	return entry
}

func readGoldenSummary(t *testing.T, path string) goldenSummary {
	t.Helper()
	summary := goldenSummary{}
	b, err := os.ReadFile(path)
	if os.IsNotExist(err) && os.Getenv(updateGoldenEnv) == "1" {
		return summary
	}
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(b, &summary))
	return summary
}

func writeGoldenSummary(t *testing.T, path string, summary goldenSummary) {
	t.Helper()
	b, err := json.MarshalIndent(summary, "", "  ")
	require.NoError(t, err)
	require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o755))
	require.NoError(t, os.WriteFile(path, append(b, '\n'), 0o644))
}
