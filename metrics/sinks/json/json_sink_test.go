package json

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"k8s.io/heapster/metrics/core"
)

func TestSimpleWrite(t *testing.T) {
	now := time.Now()
	batch := core.DataBatch{
		Timestamp:  now,
		MetricSets: make(map[string]*core.MetricSet),
	}
	batch.MetricSets["pod1"] = &core.MetricSet{
		Labels: map[string]string{"bzium": "hocuspocus"},
		MetricValues: map[string]core.MetricValue{
			"m1": {
				ValueType:  core.ValueInt64,
				MetricType: core.MetricGauge,
				IntValue:   31415,
			},
			"m2": {
				ValueType:  core.ValueFloat,
				MetricType: core.MetricGauge,
				FloatValue:   31.456,
			},
		},
		LabeledMetrics: []core.LabeledMetric{
			{
				Name: "lm",
				MetricValue: core.MetricValue{
					MetricType: core.MetricGauge,
					ValueType:  core.ValueInt64,
					IntValue:   279,
				},
				Labels: map[string]string{
					"disk": "hard",
				},

			},
		},
		ScrapeTime:     now,
		CreateTime:     now,
	}
	log, _ := batchToStringList(&batch)

	assert.True(t, strings.Contains(log[0], "31415"))
	assert.True(t, strings.Contains(log[0], "m1"))
	assert.True(t, strings.Contains(log[0], "m2"))
	assert.True(t, strings.Contains(log[0], "3.1456E+01"))
	assert.True(t, strings.Contains(log[0], "bzium"))
	assert.True(t, strings.Contains(log[0], "hocuspocus"))
	assert.True(t, strings.Contains(log[0], "pod1"))
	assert.True(t, strings.Contains(log[0], "279"))
	assert.True(t, strings.Contains(log[0], "disk"))
	assert.True(t, strings.Contains(log[0], "hard"))
	assert.True(t, strings.Contains(log[0], fmt.Sprintf("%s", now)))
}

func TestEmptyWrite(t *testing.T) {
	// check fields get initialized correctly when various metric fields are empty
	now := time.Now()
	batch := core.DataBatch{
		Timestamp:  now,
		MetricSets: make(map[string]*core.MetricSet),
	}
	batch.MetricSets["pod1"] = &core.MetricSet{
		Labels: map[string]string{},
		MetricValues: map[string]core.MetricValue{},
		LabeledMetrics: []core.LabeledMetric{},
		ScrapeTime:     now,
		CreateTime:     now,
	}
	log, _ := batchToStringList(&batch)
	assert.True(t, strings.Contains(log[0], "\"Labels\":{}"))
	assert.True(t, strings.Contains(log[0], "\"Metrics\":{}"))
	assert.True(t, strings.Contains(log[0], "\"LabeledMetrics\":{}"))
}