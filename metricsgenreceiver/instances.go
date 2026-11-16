package metricsgenreceiver

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"os"
	"reflect"
	"sort"
	"strconv"
	"time"

	"go.opentelemetry.io/collector/confmap"
	"go.opentelemetry.io/collector/pdata/pcommon"
)

// resourceAttributes converts the declared resource attributes into a pcommon.Map.
// Keys are inserted in sorted order at every level, so the result does not depend on map
// iteration order.
func (c InstanceCfg) resourceAttributes() (pcommon.Map, error) {
	attrs := pcommon.NewMap()
	if err := putSortedMap(attrs, c.ResourceAttributes, ""); err != nil {
		return pcommon.Map{}, fmt.Errorf("resource_attributes %w", err)
	}
	return attrs, nil
}

// putSortedMap inserts the entries of raw into dst in key order, converting nested maps and lists
// recursively. path is the location of raw within the top-level map, used in errors.
func putSortedMap(dst pcommon.Map, raw map[string]any, path string) error {
	keys := make([]string, 0, len(raw))
	for k := range raw {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	dst.EnsureCapacity(len(keys))
	for _, k := range keys {
		keyPath := fmt.Sprintf("%s[%q]", path, k)
		if path == "" {
			keyPath = strconv.Quote(k)
		}
		if k == "" {
			return fmt.Errorf("%s: key must not be empty", keyPath)
		}
		if err := putRawValue(dst.PutEmpty(k), raw[k], keyPath); err != nil {
			return err
		}
	}
	return nil
}

// putRawValue sets dst to v. Maps and lists are converted recursively so that nested maps are
// also inserted in key order; other values are converted with pcommon.Value.FromRaw.
func putRawValue(dst pcommon.Value, v any, path string) error {
	switch tv := v.(type) {
	case nil:
		return fmt.Errorf("%s: value must not be null", path)
	case map[string]any:
		return putSortedMap(dst.SetEmptyMap(), tv, path)
	case []any:
		slice := dst.SetEmptySlice()
		slice.EnsureCapacity(len(tv))
		for i, elem := range tv {
			if err := putRawValue(slice.AppendEmpty(), elem, fmt.Sprintf("%s[%d]", path, i)); err != nil {
				return err
			}
		}
		return nil
	default:
		if err := dst.FromRaw(v); err != nil {
			return fmt.Errorf("%s: unsupported value (%w)", path, err)
		}
		return nil
	}
}

// declaredInstance is an explicitly declared instance together with the location of its
// definition, which prefixes errors about the instance.
type declaredInstance struct {
	cfg InstanceCfg
	// source is instances[<index>] for inline instances and <path>:<line> for file instances.
	source string
}

func inlineInstanceSource(i int) string {
	return fmt.Sprintf("instances[%d]", i)
}

// scenarioInstances returns the explicit instances of a scenario, loading them from
// instances_path if set, limited to the first scale instances when scale is positive.
func scenarioInstances(scn ScenarioCfg, interval time.Duration) ([]declaredInstance, error) {
	var instances []declaredInstance
	if scn.InstancesPath != "" {
		var err error
		instances, err = loadInstancesFile(scn.InstancesPath, interval)
		if err != nil {
			return nil, err
		}
		if len(instances) == 0 {
			return nil, fmt.Errorf("%s: no instances defined", scn.InstancesPath)
		}
	} else {
		instances = make([]declaredInstance, len(scn.Instances))
		for i, inst := range scn.Instances {
			instances[i] = declaredInstance{cfg: inst, source: inlineInstanceSource(i)}
		}
	}
	if err := checkInstanceScale(scn.Scale, len(instances)); err != nil {
		return nil, err
	}
	if scn.Scale > 0 {
		instances = instances[:scn.Scale]
	}
	return instances, nil
}

// loadInstancesFile reads an NDJSON file with one instance definition per line.
// Blank lines are ignored. Lines are decoded through confmap like inline instances,
// so unknown keys are rejected.
func loadInstancesFile(path string, interval time.Duration) ([]declaredInstance, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var instances []declaredInstance
	reader := bufio.NewReader(f)
	for lineNumber := 1; ; lineNumber++ {
		source := fmt.Sprintf("%s:%d", path, lineNumber)
		line, readErr := reader.ReadBytes('\n')
		if readErr != nil && !errors.Is(readErr, io.EOF) {
			return nil, fmt.Errorf("%s: %w", source, readErr)
		}
		if line = bytes.TrimSpace(line); len(line) > 0 {
			inst, err := decodeInstanceLine(line, interval)
			if err != nil {
				return nil, fmt.Errorf("%s: %w", source, err)
			}
			instances = append(instances, declaredInstance{cfg: inst, source: source})
		}
		if readErr != nil {
			return instances, nil
		}
	}
}

// instancesDocument is the shape a file line is decoded into. Wrapping the line in a list,
// like inline instances, keeps confmap from splitting keys that contain its "::" delimiter:
// confmap only splits keys of maps that are not inside a list.
type instancesDocument struct {
	Instances []InstanceCfg `mapstructure:"instances"`
}

func decodeInstanceLine(line []byte, interval time.Duration) (InstanceCfg, error) {
	decoder := json.NewDecoder(bytes.NewReader(line))
	decoder.UseNumber()
	var raw map[string]any
	if err := decoder.Decode(&raw); err != nil {
		return InstanceCfg{}, err
	}
	if raw == nil {
		return InstanceCfg{}, errors.New("expected a JSON object")
	}
	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		return InstanceCfg{}, errors.New("unexpected data after the JSON object")
	}
	var doc instancesDocument
	conf := confmap.NewFromStringMap(map[string]any{"instances": []any{normalizeJSONNumbers(raw)}})
	if err := conf.Unmarshal(&doc); err != nil {
		return InstanceCfg{}, err
	}
	if len(doc.Instances) != 1 {
		return InstanceCfg{}, errors.New("expected one instance definition")
	}
	inst := doc.Instances[0]
	if err := inst.validate(interval); err != nil {
		return InstanceCfg{}, err
	}
	return inst, nil
}

// validate checks the parts of an instance definition that do not depend on the receiver's
// start time, which may only be known when the receiver is created.
func (c InstanceCfg) validate(interval time.Duration) error {
	if _, err := c.resourceAttributes(); err != nil {
		return err
	}
	if c.IntervalOffset != nil {
		if *c.IntervalOffset < 0 {
			return fmt.Errorf("interval_offset (%s) must not be negative", *c.IntervalOffset)
		}
		if *c.IntervalOffset >= interval {
			return fmt.Errorf("interval_offset (%s) must be less than interval (%s)", *c.IntervalOffset, interval)
		}
	}
	return c.validateLifetime()
}

// validateLifetime checks start_time, end_time, restarts and their relation to start_timestamp.
func (c InstanceCfg) validateLifetime() error {
	if c.StartTime != nil && c.EndTime != nil && !c.StartTime.Before(*c.EndTime) {
		return fmt.Errorf("start_time (%s) must be before end_time (%s)", formatTime(*c.StartTime), formatTime(*c.EndTime))
	}
	if c.StartTimestamp != nil && c.StartTime != nil && c.StartTimestamp.After(*c.StartTime) {
		return fmt.Errorf("start_timestamp (%s) must not be after start_time (%s)", formatTime(*c.StartTimestamp), formatTime(*c.StartTime))
	}
	for i, restart := range c.Restarts {
		if i > 0 && !restart.After(c.Restarts[i-1]) {
			return fmt.Errorf("restarts[%d] (%s) must be after restarts[%d] (%s)", i, formatTime(restart), i-1, formatTime(c.Restarts[i-1]))
		}
		if c.StartTimestamp != nil && !restart.After(*c.StartTimestamp) {
			return fmt.Errorf("restarts[%d] (%s) must be after start_timestamp (%s)", i, formatTime(restart), formatTime(*c.StartTimestamp))
		}
		if c.StartTime != nil && !restart.After(*c.StartTime) {
			return fmt.Errorf("restarts[%d] (%s) must be after start_time (%s)", i, formatTime(restart), formatTime(*c.StartTime))
		}
		if c.EndTime != nil && !restart.Before(*c.EndTime) {
			return fmt.Errorf("restarts[%d] (%s) must be before end_time (%s)", i, formatTime(restart), formatTime(*c.EndTime))
		}
	}
	return nil
}

func formatTime(t time.Time) string {
	return t.Format(time.RFC3339Nano)
}

// initialStartTimestamp returns start_timestamp, defaulting to start_time; nil if neither is set.
func (c InstanceCfg) initialStartTimestamp() *time.Time {
	if c.StartTimestamp != nil {
		return c.StartTimestamp
	}
	return c.StartTime
}

// hasLifecycle reports whether the instance has a bounded lifetime or restarts.
func (c InstanceCfg) hasLifecycle() bool {
	return c.StartTime != nil || c.EndTime != nil || len(c.Restarts) > 0
}

// validateStart checks the instance definition against the receiver's start time. A
// start_timestamp may lie inside the generated window only when the instance starts later.
func (c InstanceCfg) validateStart(startTime time.Time) error {
	if c.StartTime == nil && c.StartTimestamp != nil && c.StartTimestamp.After(startTime) {
		return fmt.Errorf("start_timestamp (%s) must not be after the receiver's start_time (%s)",
			formatTime(*c.StartTimestamp), formatTime(startTime))
	}
	return nil
}

// intervalOffset returns the instance's offset within the interval: the declared
// interval_offset if set, otherwise the given stagger offset.
func (c InstanceCfg) intervalOffset(stagger time.Duration) time.Duration {
	if c.IntervalOffset != nil {
		return *c.IntervalOffset
	}
	return stagger
}

// normalizeJSONNumbers converts json.Number values to int when they are integral and to
// float64 otherwise, so values match what YAML decoding of an inline config produces.
func normalizeJSONNumbers(v any) any {
	switch tv := v.(type) {
	case json.Number:
		if i, err := tv.Int64(); err == nil {
			return int(i)
		}
		if f, err := tv.Float64(); err == nil {
			return f
		}
		return tv.String()
	case map[string]any:
		for k, elem := range tv {
			tv[k] = normalizeJSONNumbers(elem)
		}
		return tv
	case []any:
		for i, elem := range tv {
			tv[i] = normalizeJSONNumbers(elem)
		}
		return tv
	default:
		return v
	}
}

// instanceGroup is a set of explicit instances that render the metrics template with equal
// variables and therefore share one template.
type instanceGroup struct {
	templateVars map[string]any
	// varsDigest is the FNV-64a hash of the canonical JSON encoding of templateVars.
	varsDigest uint64
	// indexes are the positions of the group's instances in the scenario's instance list.
	indexes []int
}

// groupInstancesByTemplateVars merges each instance's template_vars over the scenario's and
// groups instances with equal merged variables. Groups are ordered by first appearance and
// instances keep list order within a group.
func groupInstancesByTemplateVars(scenarioVars map[string]any, instances []declaredInstance) ([]instanceGroup, error) {
	var groups []instanceGroup
	// groupsByDigest maps the digest of the JSON encoding of merged variables to the groups with
	// that digest. Only the digest is kept, not the encoding, which can be large. JSON does not
	// distinguish some types (for example 1 and 1.0), and templates can render them differently,
	// so a group is only joined if its variables are deeply equal.
	groupsByDigest := make(map[uint64][]int)
	for i, inst := range instances {
		merged := make(map[string]any, len(scenarioVars)+len(inst.cfg.TemplateVars))
		for k, v := range scenarioVars {
			merged[k] = v
		}
		for k, v := range inst.cfg.TemplateVars {
			merged[k] = v
		}
		// encoding/json sorts map keys, so equal variables produce equal encodings.
		encoded, err := json.Marshal(merged)
		if err != nil {
			return nil, fmt.Errorf("%s: template_vars: %w", inst.source, err)
		}
		h := fnv.New64a()
		_, _ = h.Write(encoded)
		digest := h.Sum64()
		idx := -1
		for _, candidate := range groupsByDigest[digest] {
			if reflect.DeepEqual(groups[candidate].templateVars, merged) {
				idx = candidate
				break
			}
		}
		if idx < 0 {
			idx = len(groups)
			groupsByDigest[digest] = append(groupsByDigest[digest], idx)
			groups = append(groups, instanceGroup{templateVars: merged, varsDigest: digest})
		}
		groups[idx].indexes = append(groups[idx].indexes, i)
	}
	return groups, nil
}
