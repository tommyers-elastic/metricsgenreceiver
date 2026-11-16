package metricsgenreceiver

import (
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/elastic/metricsgenreceiver/metricsgenreceiver/internal/dp"
	"github.com/elastic/metricsgenreceiver/metricsgenreceiver/internal/metricstmpl"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/confmap"
	"go.opentelemetry.io/collector/confmap/confmaptest"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/receiver/receivertest"
)

const (
	instancesTemplatePath = "testdata/instances-template"
	instancesFilePath     = "testdata/instances.ndjson"
	// explicitInstancesGoldenSHA256 is the SHA-256 of the output of explicitInstancesFileConfig.
	// The fixture only uses values that are computed identically on arm64 and amd64, so the
	// hash is the same on both.
	explicitInstancesGoldenSHA256 = "51a2be5fe9a71ac4e90c3db0c0bd2a8a26644fca28748de435dc5e057f930406"
)

// explicitInstancesFileConfig runs the instances fixture for three intervals.
func explicitInstancesFileConfig() *Config {
	cfg := NewFactory().CreateDefaultConfig().(*Config)
	cfg.StartTime = time.Date(2024, 12, 17, 0, 0, 0, 0, time.UTC)
	cfg.EndTime = cfg.StartTime.Add(90 * time.Second)
	cfg.Interval = 30 * time.Second
	cfg.Seed = 7
	cfg.Scenarios = []ScenarioCfg{{Path: instancesTemplatePath, InstancesPath: instancesFilePath}}
	return cfg
}

func loadInstancesTestConfig(t *testing.T) *Config {
	t.Helper()
	cm, err := confmaptest.LoadConf(filepath.Join("testdata", "config-instances.yaml"))
	require.NoError(t, err)
	cfg := NewFactory().CreateDefaultConfig().(*Config)
	sub, err := cm.Sub("metricsgen")
	require.NoError(t, err)
	require.NoError(t, sub.Unmarshal(cfg))
	return cfg
}

func TestLoadConfigInstances(t *testing.T) {
	cfg := loadInstancesTestConfig(t)
	require.NoError(t, confmap.Validate(cfg))
	require.Len(t, cfg.Scenarios, 2)

	inline := cfg.Scenarios[0]
	assert.Empty(t, inline.InstancesPath)
	require.Len(t, inline.Instances, 4)
	assert.Equal(t, InstanceCfg{
		ResourceAttributes: map[string]any{
			"host.name":      "db-1",
			"host.ip":        []any{"192.0.2.20"},
			"host.cpu.count": 16,
			"cloud.provider": "example",
		},
		TemplateVars: map[string]any{
			"interfaces":   []any{"lo", "eth0", "eth1"},
			"disks":        []any{"nvme0n1", "nvme1n1"},
			"memory_total": 68719476736,
		},
	}, inline.Instances[2])

	fromFile := cfg.Scenarios[1]
	assert.Equal(t, instancesFilePath, fromFile.InstancesPath)
	assert.Empty(t, fromFile.Instances)
	assert.Equal(t, 2, fromFile.Scale)
	assert.Equal(t, map[string]any{"disks": []any{"sdb"}}, fromFile.TemplateVars)
}

func TestLoadInstancesFileMatchesInlineConfig(t *testing.T) {
	cfg := loadInstancesTestConfig(t)
	fromFile, err := loadInstancesFile(instancesFilePath, 30*time.Second)
	require.NoError(t, err)
	assert.Equal(t, cfg.Scenarios[0].Instances, instanceCfgs(fromFile))
	assert.Equal(t, "testdata/instances.ndjson:4", fromFile[2].source)
}

func TestLoadInstancesFileMatchesInlineConfigWithDelimiterKeys(t *testing.T) {
	// confmap uses "::" as its key delimiter; keys containing it must not be split into nested maps.
	dir := t.TempDir()
	yamlPath := filepath.Join(dir, "config.yaml")
	require.NoError(t, os.WriteFile(yamlPath, []byte(`metricsgen:
  scenarios:
  - path: testdata/instances-template
    instances:
    - resource_attributes:
        "a::b": x
        labels:
          "c::d": y
          nested: {"e::f": 1}
        items: [{"g::h": true}]
      template_vars:
        "i::j": 2.5
`), 0o600))
	ndjsonPath := filepath.Join(dir, "instances.ndjson")
	require.NoError(t, os.WriteFile(ndjsonPath, []byte(
		`{"resource_attributes":{"a::b":"x","labels":{"c::d":"y","nested":{"e::f":1}},"items":[{"g::h":true}]},"template_vars":{"i::j":2.5}}`+"\n"), 0o600))

	cm, err := confmaptest.LoadConf(yamlPath)
	require.NoError(t, err)
	cfg := NewFactory().CreateDefaultConfig().(*Config)
	sub, err := cm.Sub("metricsgen")
	require.NoError(t, err)
	require.NoError(t, sub.Unmarshal(cfg))

	fromFile, err := loadInstancesFile(ndjsonPath, 30*time.Second)
	require.NoError(t, err)
	want := InstanceCfg{
		ResourceAttributes: map[string]any{
			"a::b":   "x",
			"labels": map[string]any{"c::d": "y", "nested": map[string]any{"e::f": 1}},
			"items":  []any{map[string]any{"g::h": true}},
		},
		TemplateVars: map[string]any{"i::j": 2.5},
	}
	assert.Equal(t, []InstanceCfg{want}, cfg.Scenarios[0].Instances)
	assert.Equal(t, []InstanceCfg{want}, instanceCfgs(fromFile))
}

func TestResourceAttributesNestedMapsAreSorted(t *testing.T) {
	nested := map[string]any{}
	for _, k := range []string{"k", "c", "x", "a", "q", "m", "b", "z", "e", "o"} {
		nested[k] = map[string]any{"z" + k: 1, "a" + k: []any{map[string]any{"y": 1, "b": 2, "m": 3}}}
	}
	inst := InstanceCfg{ResourceAttributes: map[string]any{"z": 1, "labels": nested, "a": "x"}}

	var first string
	for i := 0; i < 50; i++ {
		attrs, err := inst.resourceAttributes()
		require.NoError(t, err)
		got := attributeKeyOrder(attrs)
		if i == 0 {
			first = got
			continue
		}
		require.Equal(t, first, got, "run %d", i)
	}
	assert.True(t, strings.HasPrefix(first, "a,labels{a{aa[{b,m,y}],za},b{ab[{b,m,y}],zb},c{"), first)
}

// attributeKeyOrder renders the keys of attrs, recursively, in insertion order.
func attributeKeyOrder(attrs pcommon.Map) string {
	var b strings.Builder
	first := true
	attrs.Range(func(k string, v pcommon.Value) bool {
		if !first {
			b.WriteByte(',')
		}
		first = false
		b.WriteString(k)
		b.WriteString(valueKeyOrder(v))
		return true
	})
	return b.String()
}

func valueKeyOrder(v pcommon.Value) string {
	switch v.Type() {
	case pcommon.ValueTypeMap:
		return "{" + attributeKeyOrder(v.Map()) + "}"
	case pcommon.ValueTypeSlice:
		parts := make([]string, 0, v.Slice().Len())
		for i := 0; i < v.Slice().Len(); i++ {
			parts = append(parts, valueKeyOrder(v.Slice().At(i)))
		}
		return "[" + strings.Join(parts, ",") + "]"
	default:
		return ""
	}
}

func TestExplicitInstancesGroupingIsTypeExact(t *testing.T) {
	cfg := explicitInstancesFileConfig()
	cfg.Scenarios[0].InstancesPath = ""
	cfg.Scenarios[0].TemplateVars = map[string]any{"interfaces": []any{"lo"}, "disks": []any{"sda"}}
	// 1 and 1.0 encode to the same JSON but are different template variables.
	cfg.Scenarios[0].Instances = []InstanceCfg{
		{ResourceAttributes: map[string]any{"host.name": "a"}, TemplateVars: map[string]any{"memory_total": 1}},
		{ResourceAttributes: map[string]any{"host.name": "b"}, TemplateVars: map[string]any{"memory_total": 1.0}},
		{ResourceAttributes: map[string]any{"host.name": "c"}, TemplateVars: map[string]any{"memory_total": 1}},
	}
	require.NoError(t, cfg.Validate())
	rcv, err := newMetricsGenReceiver(cfg, receivertest.NewNopSettings(typ))
	require.NoError(t, err)
	require.Len(t, rcv.scenarios, 2)
	assert.Len(t, rcv.scenarios[0].instances, 2)
	assert.Len(t, rcv.scenarios[1].instances, 1)
	assert.Equal(t, 1, rcv.scenarios[1].instances[0].id)
}

func TestResourceAttributesTemplateExists(t *testing.T) {
	exists, err := metricstmpl.ResourceAttributesTemplateExists("builtin/hostmetrics")
	require.NoError(t, err)
	assert.True(t, exists)

	exists, err = metricstmpl.ResourceAttributesTemplateExists(instancesTemplatePath)
	require.NoError(t, err)
	assert.False(t, exists)

	// A path below a regular file cannot be checked; that is an error, not a missing template.
	file := filepath.Join(t.TempDir(), "file")
	require.NoError(t, os.WriteFile(file, nil, 0o600))
	_, err = metricstmpl.ResourceAttributesTemplateExists(filepath.Join(file, "template"))
	require.Error(t, err)
	assert.NotErrorIs(t, err, os.ErrNotExist)
}

func TestExplicitInstancesEmitDeclaredInstances(t *testing.T) {
	allMetrics := runReceiverToCompletion(t, explicitInstancesFileConfig())
	// 4 instances x 3 intervals
	require.Len(t, allMetrics, 12)

	// Instances with equal template variables share a template and are emitted together;
	// groups are emitted in order of first appearance.
	wantOrder := []string{"web-1", "web-2", "db-1", "cache-1"}
	for i, md := range allMetrics {
		assert.Equal(t, wantOrder[i%4], resourceHostName(t, md), "emission %d", i)
	}

	wantAttributes := map[string]map[string]any{
		"web-1": {
			"host.name": "web-1", "host.ip": []any{"192.0.2.10", "2001:db8::10"}, "host.cpu.count": int64(4), "os.type": "linux",
		},
		"web-2": {
			"host.name": "web-2", "host.ip": []any{"192.0.2.11", "2001:db8::11"}, "host.cpu.count": int64(4), "os.type": "linux",
		},
		"db-1": {
			"host.name": "db-1", "host.ip": []any{"192.0.2.20"}, "host.cpu.count": int64(16), "cloud.provider": "example", "os.type": "linux",
		},
		"cache-1": {
			"host.name": "cache-1", "host.ip": []any{"192.0.2.30"}, "host.cpu.count": int64(2), "host.virtual": true, "os.type": "linux",
		},
	}
	wantDevices := map[string]map[string][]string{
		"web-1":   {"system.network.io": {"lo", "lo", "eth0", "eth0"}, "system.disk.operations": {"sda"}},
		"web-2":   {"system.network.io": {"lo", "lo", "eth0", "eth0"}, "system.disk.operations": {"sda"}},
		"db-1":    {"system.network.io": {"lo", "lo", "eth0", "eth0", "eth1", "eth1"}, "system.disk.operations": {"nvme0n1", "nvme1n1"}},
		"cache-1": {"system.network.io": {"lo", "lo", "ens5", "ens5"}, "system.disk.operations": {"xvda"}},
	}
	wantDataPoints := map[string]int{"web-1": 7, "web-2": 7, "db-1": 10, "cache-1": 7}
	wantMemory := map[string]int64{"web-1": 8589934592, "web-2": 8589934592, "db-1": 68719476736, "cache-1": 4294967296}

	for _, md := range allMetrics {
		host := resourceHostName(t, md)
		require.Equal(t, 1, md.ResourceMetrics().Len())
		attrs := md.ResourceMetrics().At(0).Resource().Attributes()
		assert.Equal(t, wantAttributes[host], attrs.AsRaw(), host)
		assert.Equal(t, wantDevices[host], dataPointDevices(md), host)
		assert.Equal(t, wantDataPoints[host], md.DataPointCount(), host)
		assert.Equal(t, wantMemory[host], metricIntValue(t, md, "system.memory.limit"), host)
	}
}

func TestExplicitInstancesShareTemplatesByVariables(t *testing.T) {
	cfg := explicitInstancesFileConfig()
	cfg.InstanceOffset = 10
	rcv, err := newMetricsGenReceiver(cfg, receivertest.NewNopSettings(typ))
	require.NoError(t, err)

	// web-1 and web-2 have equal template_vars and share one template.
	require.Len(t, rcv.scenarios, 3)
	var ids [][]int
	for _, scn := range rcv.scenarios {
		assert.True(t, scn.explicitInstances)
		var groupIDs []int
		for _, inst := range scn.instances {
			groupIDs = append(groupIDs, inst.id)
		}
		ids = append(ids, groupIDs)
	}
	assert.Equal(t, [][]int{{10, 11}, {12}, {13}}, ids)
	assert.Equal(t, 7, rcv.scenarios[0].metricsTemplate.DataPointCount())
	assert.Equal(t, 10, rcv.scenarios[1].metricsTemplate.DataPointCount())
}

func TestExplicitInstancesScenarioTemplateVarsAreMerged(t *testing.T) {
	cfg := explicitInstancesFileConfig()
	cfg.Scenarios[0].TemplateVars = map[string]any{"disks": []any{"sdb"}, "interfaces": []any{"lo"}}
	// Instance variables win over scenario variables.
	cfg.Scenarios[0].InstancesPath = ""
	cfg.Scenarios[0].Instances = []InstanceCfg{
		{ResourceAttributes: map[string]any{"host.name": "a"}, TemplateVars: map[string]any{"memory_total": 1024}},
		{ResourceAttributes: map[string]any{"host.name": "b"}, TemplateVars: map[string]any{"memory_total": 1024, "disks": []any{"sdc", "sdd"}}},
		{ResourceAttributes: map[string]any{"host.name": "c"}, TemplateVars: map[string]any{"memory_total": 1024}},
	}
	require.NoError(t, cfg.Validate())
	rcv, err := newMetricsGenReceiver(cfg, receivertest.NewNopSettings(typ))
	require.NoError(t, err)
	require.Len(t, rcv.scenarios, 2)
	assert.Len(t, rcv.scenarios[0].instances, 2)

	cfg = explicitInstancesFileConfig()
	cfg.Scenarios[0].TemplateVars = map[string]any{"disks": []any{"sdb"}, "interfaces": []any{"lo"}}
	cfg.Scenarios[0].InstancesPath = ""
	cfg.Scenarios[0].Instances = []InstanceCfg{
		{ResourceAttributes: map[string]any{"host.name": "a"}, TemplateVars: map[string]any{"memory_total": 1024}},
		{ResourceAttributes: map[string]any{"host.name": "b"}, TemplateVars: map[string]any{"memory_total": 1024, "disks": []any{"sdc", "sdd"}}},
	}
	allMetrics := runReceiverToCompletion(t, cfg)
	require.Len(t, allMetrics, 6)
	assert.Equal(t, map[string][]string{"system.network.io": {"lo", "lo"}, "system.disk.operations": {"sdb"}}, dataPointDevices(allMetrics[0]))
	assert.Equal(t, map[string][]string{"system.network.io": {"lo", "lo"}, "system.disk.operations": {"sdc", "sdd"}}, dataPointDevices(allMetrics[1]))
}

func TestExplicitInstancesInlineMatchesFile(t *testing.T) {
	inlineCfg := explicitInstancesFileConfig()
	inlineCfg.Scenarios[0] = loadInstancesTestConfig(t).Scenarios[0]
	require.NoError(t, inlineCfg.Validate())
	inlineOutput := marshalMetricsOutput(t, runReceiverToCompletion(t, inlineCfg))

	fileOutput := marshalMetricsOutput(t, runReceiverToCompletion(t, explicitInstancesFileConfig()))
	assert.Equal(t, fileOutput.String(), inlineOutput.String())
}

func TestExplicitInstancesScaleSelectsFirstInstances(t *testing.T) {
	cfg := explicitInstancesFileConfig()
	cfg.Scenarios[0].Scale = 2
	require.NoError(t, cfg.Validate())
	allMetrics := runReceiverToCompletion(t, cfg)
	require.Len(t, allMetrics, 6)
	assert.Equal(t, []string{"web-1", "web-2"}, collectHostNames(allMetrics))
}

func TestExplicitInstancesDeterministic(t *testing.T) {
	first := marshalMetricsOutput(t, runReceiverToCompletion(t, explicitInstancesFileConfig()))
	second := marshalMetricsOutput(t, runReceiverToCompletion(t, explicitInstancesFileConfig()))
	require.Equal(t, first.String(), second.String())

	sum := sha256.Sum256(first.Bytes())
	assert.Equal(t, explicitInstancesGoldenSHA256, hex.EncodeToString(sum[:]))
}

func TestExplicitInstancesWithResourceAttributesTemplate(t *testing.T) {
	cfg := explicitInstancesFileConfig()
	cfg.Scenarios = []ScenarioCfg{{
		Path: "builtin/hostmetrics",
		Instances: []InstanceCfg{
			{ResourceAttributes: map[string]any{"host.name": "web-1", "deployment.environment.name": "test"}},
			{ResourceAttributes: map[string]any{"host.name": "web-2", "deployment.environment.name": "test"}},
		},
	}}
	require.NoError(t, cfg.Validate())
	allMetrics := runReceiverToCompletion(t, cfg)
	require.Len(t, allMetrics, 6)

	hostIPs := map[string]string{}
	for i, md := range allMetrics {
		wantHost := []string{"web-1", "web-2"}[i%2]
		for j := 0; j < md.ResourceMetrics().Len(); j++ {
			attrs := md.ResourceMetrics().At(j).Resource().Attributes()
			hostName, ok := attrs.Get("host.name")
			require.True(t, ok)
			assert.Equal(t, wantHost, hostName.Str())
			env, ok := attrs.Get("deployment.environment.name")
			require.True(t, ok, "explicit attributes are added to every resource")
			assert.Equal(t, "test", env.Str())
			// host.ip still comes from the rendered resource attributes template.
			if ip, ok := attrs.Get("host.ip"); ok {
				require.Equal(t, pcommon.ValueTypeSlice, ip.Type())
				if prev, seen := hostIPs[wantHost]; seen {
					assert.Equal(t, prev, ip.AsString())
				}
				hostIPs[wantHost] = ip.AsString()
			}
		}
	}
	require.Len(t, hostIPs, 2)
	assert.NotEqual(t, hostIPs["web-1"], hostIPs["web-2"])
}

func TestExplicitInstancesValidate(t *testing.T) {
	instances := []InstanceCfg{{ResourceAttributes: map[string]any{"host.name": "web-1"}}}
	tests := []struct {
		name      string
		scenario  ScenarioCfg
		wantError string
	}{
		{
			name:     "inline instances",
			scenario: ScenarioCfg{Instances: instances},
		},
		{
			name:     "instances path",
			scenario: ScenarioCfg{InstancesPath: instancesFilePath, Scale: 100},
		},
		{
			name: "all supported value types",
			scenario: ScenarioCfg{Instances: []InstanceCfg{{ResourceAttributes: map[string]any{
				"string": "a", "bool": true, "int": 1, "float": 1.5, "list": []any{"a", 1}, "map": map[string]any{"k": "v"},
			}}}},
		},
		{
			name:      "instances and instances path",
			scenario:  ScenarioCfg{Instances: instances, InstancesPath: instancesFilePath},
			wantError: "instances and instances_path are mutually exclusive",
		},
		{
			name:      "concurrency",
			scenario:  ScenarioCfg{Instances: instances, Concurrency: 1},
			wantError: "concurrency is not supported with instances or instances_path",
		},
		{
			name:      "churn",
			scenario:  ScenarioCfg{InstancesPath: instancesFilePath, Churn: &ChurnCfg{SamplesPerSeries: 1}},
			wantError: "churn is not supported with instances or instances_path",
		},
		{
			name:      "scale larger than instances",
			scenario:  ScenarioCfg{Instances: instances, Scale: 2},
			wantError: "scale (2) must not exceed the number of instances (1)",
		},
		{
			name:      "negative scale",
			scenario:  ScenarioCfg{Instances: instances, Scale: -1},
			wantError: "scale must not be negative",
		},
		{
			name:      "null value",
			scenario:  ScenarioCfg{Instances: []InstanceCfg{{ResourceAttributes: map[string]any{"host.name": nil}}}},
			wantError: `instances[0]: resource_attributes "host.name": value must not be null`,
		},
		{
			name: "nested null value",
			scenario: ScenarioCfg{Instances: []InstanceCfg{{ResourceAttributes: map[string]any{
				"labels": map[string]any{"team": "a", "items": []any{"x", map[string]any{"owner": nil}}},
			}}}},
			wantError: `instances[0]: resource_attributes "labels"["items"][1]["owner"]: value must not be null`,
		},
		{
			name:      "null list element",
			scenario:  ScenarioCfg{Instances: []InstanceCfg{{ResourceAttributes: map[string]any{"host.ip": []any{"192.0.2.10", nil}}}}},
			wantError: `instances[0]: resource_attributes "host.ip"[1]: value must not be null`,
		},
		{
			name:      "empty key",
			scenario:  ScenarioCfg{Instances: []InstanceCfg{{ResourceAttributes: map[string]any{"": "x"}}}},
			wantError: `instances[0]: resource_attributes "": key must not be empty`,
		},
		{
			name:      "nested empty key",
			scenario:  ScenarioCfg{Instances: []InstanceCfg{{ResourceAttributes: map[string]any{"labels": map[string]any{"": "x"}}}}},
			wantError: `instances[0]: resource_attributes "labels"[""]: key must not be empty`,
		},
		{
			name:      "unsupported value type",
			scenario:  ScenarioCfg{Instances: []InstanceCfg{{}, {ResourceAttributes: map[string]any{"host.boot": time.Time{}}}}},
			wantError: `instances[1]: resource_attributes "host.boot": unsupported value (<Invalid value type time.Time>)`,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg := &Config{Interval: time.Second, Scenarios: []ScenarioCfg{tc.scenario}}
			err := cfg.Validate()
			if tc.wantError == "" {
				require.NoError(t, err)
			} else {
				require.EqualError(t, err, tc.wantError)
			}
		})
	}
}

func TestExplicitInstancesFileErrors(t *testing.T) {
	tests := []struct {
		name      string
		content   string
		scale     int
		wantError string
		// partial matches wantError as a substring, for messages worded by dependencies.
		partial bool
	}{
		{
			name:      "invalid json",
			content:   `{"resource_attributes":{"host.name":"web-1"}}` + "\n\n" + `{"resource_attributes":`,
			wantError: "instances.ndjson:3: unexpected EOF",
		},
		{
			name:      "not an object",
			content:   `null`,
			wantError: "instances.ndjson:1: expected a JSON object",
		},
		{
			name:      "trailing data",
			content:   `{} {}`,
			wantError: "instances.ndjson:1: unexpected data after the JSON object",
		},
		{
			name:      "unknown key",
			content:   `{"resource_attributes":{"host.name":"web-1"}}` + "\n" + `{"resource_attribute":{"host.name":"web-2"}}`,
			wantError: "has invalid keys: resource_attribute",
			partial:   true,
		},
		{
			name:      "null value",
			content:   `{"resource_attributes":{"host.name":null}}`,
			wantError: `instances.ndjson:1: resource_attributes "host.name": value must not be null`,
		},
		{
			name:      "empty file",
			content:   "\n\n",
			wantError: "instances.ndjson: no instances defined",
		},
		{
			name:      "scale larger than instances",
			content:   `{"resource_attributes":{"host.name":"web-1"}}`,
			scale:     2,
			wantError: "scale (2) must not exceed the number of instances (1)",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "instances.ndjson")
			require.NoError(t, os.WriteFile(path, []byte(tc.content), 0o600))
			cfg := explicitInstancesFileConfig()
			cfg.Scenarios[0].InstancesPath = path
			cfg.Scenarios[0].Scale = tc.scale
			require.NoError(t, cfg.Validate())

			_, err := newMetricsGenReceiver(cfg, receivertest.NewNopSettings(typ))
			require.Error(t, err)
			msg := strings.TrimPrefix(err.Error(), filepath.Dir(path)+string(filepath.Separator))
			if tc.partial {
				assert.True(t, strings.HasPrefix(msg, "instances.ndjson:2: "), msg)
				assert.Contains(t, msg, tc.wantError)
			} else {
				assert.Equal(t, tc.wantError, msg)
			}
		})
	}

	t.Run("missing file", func(t *testing.T) {
		cfg := explicitInstancesFileConfig()
		cfg.Scenarios[0].InstancesPath = filepath.Join(t.TempDir(), "missing.ndjson")
		_, err := newMetricsGenReceiver(cfg, receivertest.NewNopSettings(typ))
		require.ErrorIs(t, err, os.ErrNotExist)
	})
}

func instanceCfgs(declared []declaredInstance) []InstanceCfg {
	cfgs := make([]InstanceCfg, len(declared))
	for i, inst := range declared {
		cfgs[i] = inst.cfg
	}
	return cfgs
}

func resourceHostName(t *testing.T, md pmetric.Metrics) string {
	t.Helper()
	require.Positive(t, md.ResourceMetrics().Len())
	hostName, ok := md.ResourceMetrics().At(0).Resource().Attributes().Get("host.name")
	require.True(t, ok)
	return hostName.Str()
}

// dataPointDevices returns the device attribute of every data point that has one, per metric
// name, in emission order.
func dataPointDevices(md pmetric.Metrics) map[string][]string {
	devices := map[string][]string{}
	dp.ForEachDataPoint(&md, func(_ int, _ pcommon.Resource, _ pcommon.InstrumentationScope, m pmetric.Metric, dataPoint dp.DataPoint) {
		if device, ok := dataPoint.Attributes().Get("device"); ok {
			devices[m.Name()] = append(devices[m.Name()], device.Str())
		}
	})
	return devices
}
