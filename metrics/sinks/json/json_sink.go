package json

import (
	"encoding/json"
	"fmt"
	"github.com/golang/glog"
	"k8s.io/heapster/metrics/core"
	"strconv"
)

type JSONSink struct {
}

func (this *JSONSink) Name() string {
	return "JSON Sink"
}

func (this *JSONSink) Stop() {
	// Do nothing.
}

type MetricSet struct {
	DataBatchTimestamp string
	MetricSet          string
	ScrapeTime         string
	CreateTime         string
	Labels             map[string]string
	Metrics            map[string]Metric
	LabeledMetrics     map[string][]LabeledMetric
}

type Metric struct {
	Value string
}

type LabeledMetric struct {
	Value  string
	Labels map[string]string
}

func batchToStringList(batch *core.DataBatch) (result []string, err error) {

	for key, ms := range batch.MetricSets {
		outMs := MetricSet{}
		outMs.MetricSet = key

		outMs.DataBatchTimestamp = batch.Timestamp.String()
		// duplicating times with unixtime format as per log sink seems redundant here
		outMs.ScrapeTime = ms.ScrapeTime.String()
		outMs.CreateTime = ms.CreateTime.String()
		outMs.Labels = ms.Labels
		outMs.Metrics = make(map[string]Metric)
		outMs.LabeledMetrics = make(map[string][]LabeledMetric)

		for metricName, metricValue := range ms.MetricValues {
			var outMetric Metric
			if core.ValueInt64 == metricValue.ValueType {
				outMetric = Metric{
					Value: strconv.FormatInt(metricValue.IntValue, 10),
				}

			} else if core.ValueFloat == metricValue.ValueType {
				outMetric = Metric{
					// exponent format not always the most intuitive, but is consistent...
					Value: strconv.FormatFloat(float64(metricValue.FloatValue), 'E', -1, 32),
				}
			} else {
				outMetric = Metric{
					Value: "?",
				}
			}
			if _, ok := outMs.Metrics[metricName]; ok {
				glog.Error("Skipping duplicate metric: " + metricName + " with value: " + outMetric.Value + ", existing value: " + outMs.Metrics[metricName].Value)
			} else {
				outMs.Metrics[metricName] = outMetric
			}
		}

		for _, metric := range ms.LabeledMetrics {
			var outMetric LabeledMetric
			if core.ValueInt64 == metric.ValueType {
				outMetric = LabeledMetric{
					Value: strconv.FormatInt(metric.IntValue, 10),
				}

			} else if core.ValueFloat == metric.ValueType {
				outMetric = LabeledMetric{
					Value: strconv.FormatFloat(float64(metric.FloatValue), 'E', -1, 32),
				}
			} else {
				outMetric = LabeledMetric{
					Value: "?",
				}
			}
			outMetric.Labels = metric.Labels
			if _, ok := outMs.LabeledMetrics[metric.Name]; ok {
				// labels can be used to subcategorize metrics
				// KV structure might be easier to filter but not obvious how best to do this with labels
				outMs.LabeledMetrics[metric.Name] = append(outMs.LabeledMetrics[metric.Name], outMetric)
			} else {
				outMs.LabeledMetrics[metric.Name] = []LabeledMetric{outMetric}
			}
		}

		b, err := json.Marshal(outMs)
		if err != nil {
			return nil, err
		}
		result = append(result, string(b))
	}
	return result, nil
}

func (this *JSONSink) ExportData(batch *core.DataBatch) {
	dataBatchJSON, _ := batchToStringList(batch)
	for _, metricSetJSON := range dataBatchJSON {
		fmt.Println(metricSetJSON)
	}
}

func NewJSONSink() *JSONSink {
	return &JSONSink{}
}
