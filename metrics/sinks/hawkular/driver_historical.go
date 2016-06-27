// Copyright 2016 Google Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package hawkular

import (
	"fmt"
	"sync"
	"time"

	"github.com/hawkular/hawkular-client-go/metrics"
	"k8s.io/heapster/metrics/core"
)

// TODO Move internal functions to client.go

/*
  https://github.com/kubernetes/heapster/blob/master/docs/proposals/old-timer.md

  Notes:
    - Note that queries by pod name will return metrics from the latest pod with the given name.
*/

// Historical returns the historical data access interface for this sink
func (h *hawkularSink) Historical() core.HistoricalSource {
	return h
}

// GetMetric retrieves the given metric for one or more objects (specified by metricKeys) of
// the same type, within the given time interval
func (h *hawkularSink) GetMetric(metricName string, metricKeys []core.HistoricalKey, start, end time.Time) (map[core.HistoricalKey][]core.TimestampedMetricValue, error) {
	typ := h.metricNameToHawkularType(metricName)

	resLock := &sync.Mutex{}
	res := make(map[core.HistoricalKey][]core.TimestampedMetricValue, len(metricKeys))

	errChan := make(chan error, len(metricKeys))
	// resultChan := make(chan []core.TimestampedMetricValue, len(metricKeys))

	wg := &sync.WaitGroup{}

	// TODO Hawkular-Metrics 0.18.0 supports fetching with a single command for raw as well
	for _, key := range metricKeys {
		wg.Add(1)
		go func(key core.HistoricalKey, errChan chan (error)) {
			tags := h.keyToTags(key)
			tags[descriptorTag] = metricName

			o := []metrics.Modifier{metrics.Filters(metrics.TagsFilter(tags))}
			if h.isNamespaceTenant() && key.NamespaceName != "" && key.ObjectType != core.MetricSetTypeCluster {
				o = append(o, metrics.Tenant(key.NamespaceName))
			}

			// Remove in favor of using ReadRaw with tags filter to reduce the amount of queries to HWKMETRICS
			mds, err := h.client.Definitions(o...)
			if err != nil {
				errChan <- err
				return
			}

			if len(mds) < 0 || len(mds) > 1 {
				errChan <- fmt.Errorf("Given metric query options did not result in unique metricId, %q, %q", metricName, key)
				return
			}

			dps, err := h.client.ReadRaw(mds[0].Type, mds[0].ID, metrics.Filters(metrics.StartTimeFilter(start), metrics.EndTimeFilter(end)))
			if err != nil {
				errChan <- err
				return
			}

			mvs := make([]core.TimestampedMetricValue, 0, len(dps))

			for _, dp := range dps {
				mvs = append(mvs, h.datapointToMetricValue(&dp, &typ))
			}

			resLock.Lock()
			res[key] = mvs
			resLock.Unlock()

			// resultChan <- mvs

			wg.Done()
		}(key, errChan)

		// We need to append here previous results.. to build one query, unless we want to do multiple requests
		// TODO Could we use our internal driver cache for this use case also? That is, cache the matching metricIds for easier fetching
	}
	wg.Wait()
	close(errChan)
	// close(resultChan)

	// Check for errors first
	for e := range errChan {
		return nil, e
	}

	return res, nil
}

// GetAggregation fetches the given aggregations for one or more objects (specified by metricKeys) of
// the same type, within the given time interval, calculated over a series of buckets
func (h *hawkularSink) GetAggregation(metricName string, aggregations []string, metricKeys []core.HistoricalKey, start, end time.Time, bucketSize time.Duration) (map[core.HistoricalKey][]core.TimestampedAggregationValue, error) {
	typ := h.metricNameToHawkularType(metricName)

	resLock := &sync.Mutex{}
	res := make(map[core.HistoricalKey][]core.TimestampedAggregationValue, len(metricKeys))

	wg := &sync.WaitGroup{}

	// While we could read everything in a single query to Hawkular-Metrics, we could not reliably parse the results back to HistoricalKey
	for _, key := range metricKeys {
		wg.Add(1)
		go func(key core.HistoricalKey, errChan chan (error), wg *sync.WaitGroup) {
			tags := h.keyToTags(key)
			tags[descriptorTag] = metricName

			o := []metrics.Modifier{metrics.Filters(metrics.TagsFilter(tags))}
			if h.isNamespaceTenant() && key.NamespaceName != "" && key.ObjectType != core.MetricSetTypeCluster {
				o = append(o, metrics.Tenant(key.NamespaceName))
			}

			bdps, err := h.client.ReadBuckets(typ, metrics.Filters(metrics.TagsFilter(tags), metrics.BucketsDurationFilter(bucketSize), metrics.StartTimeFilter(start), metrics.EndTimeFilter(end)))

			tav := make([]core.TimestampedAggregationValue, len(bdps))

			for _, bdp := range bdps {
				tav = append(tav, h.bucketPointToAggregationValue(bdp, typ, aggregations, BucketSize))
			}

			resLock.Lock()
			res[key] = tav
			resLock.Unlock()

			wg.Done()
		}(key, errChan)
	}
	wg.Wait()

	// Check for errors first
	for e := range errChan {
		return nil, e
	}

	return res, nil
}

// GetMetricNames retrieves the available metric names for the given object
func (h *hawkularSink) GetMetricNames(metricKey core.HistoricalKey) ([]string, error) {
	// MetricType is important, is it counter or gauge? Could we get that info from the metricName?

	tags := h.keyToTags(metricKey)
	tags[descriptorTag] = "*"

	var r map[string]string
	var err error

	// Create one []Modifier and pass it always (even if empty) - easier to read
	if h.isNamespaceTenant() {
		r, err = h.client.TagValues(tags, metrics.Tenant(metricKey.NamespaceName))
	} else {
		r, err = h.client.TagValues(tags)
	}

	return r[descriptorTag], err
}

// GetNodes retrieves the list of nodes in the cluster
func (h *hawkularSink) GetNodes() ([]string, error) {
	tags := make(map[string]string)
	tags[core.LabelHostname.Key] = "*"
	tags[core.LabelMetricSetType.Key] = core.MetricSetTypeNode
	r := h.client.TagValues(tags) // Assume these are stored in the system tenant
	return r[core.LabelHostname.Key], nil
}

// GetPodsFromNamespace retrieves the list of pods in a given namespace
func (h *hawkularSink) GetPodsFromNamespace(namespace string) ([]string, error) {
	tags := make(map[string]string)
	tags[core.LabelPodName.Key] = "*"
	tags[core.LabelMetricSetType.Key] = core.MetricSetTypePod

	var r map[string]string
	var err error

	if h.isNamespaceTenant() {
		r, err = h.client.TagValues(tags, metrics.Tenant(namespace))
	} else {
		tags[core.LabelNamespaceName.Key] = namespace
		r, err = h.client.TagValues(tags)
	}

	return r[core.LabelPodName.Key], err
}

// GetSystemContainersFromNode retrieves the list of free containers for a given node
func (h *hawkularSink) GetSystemContainersFromNode(node string) ([]string, error) {
	tags := make(map[string]string)
	tags[core.LabelContainerName.Key] = "*"
	tags[core.LabelHostname.Key] = node
	tags[core.LabelMetricSetType.Key] = core.MetricSetTypeSystemContainer
	r := h.client.TagValues(tags) // I assume these are stored at the system tenant
	return r[core.LabelContainerName.Key], nil
}

// GetNamespaces retrieves the list of namespaces in the cluster
func (h *hawkularSink) GetNamespaces() ([]string, error) {
	// Fetch tenants, if the labelToTenantId is set (and includes namespace label)
	if h.isNamespaceTenant() {
		tds := h.client.Tenants()
		ns := make([]string, 0, len(tds))
		for _, td := range tds {
			ns = append(ns, td.ID)
		}
		return ns
	}

	return h.getLabelTagValues(core.LabelNamespaceName.Key)
}

// Internal functions

func (h *hawkularSink) getLabelTagValues(labelKey string, o ...metrics.Modifier) ([]string, error) {
	tags := make(map[string]string)
	tags[labelKey] = "*"
	r, err := h.client.TagValues(tags, o...)
	return r[labelKey], err
}

func (h *hawkularSink) isNamespaceTenant() bool {
	// This could be a bit problematic.. are we really storing the right info?
	if h.labelTenant != "" && (h.labelTenant == core.LabelPodNamespace.Key || h.labelTenant == core.LabelPodNamespaceUID.Key) {
		return true
	}
	return false
}

func (h *hawkularSink) bucketPointToAggregationValue(bp *metrics.Bucketpoint, mt *metrics.MetricType, aggregations *[]string, bucketSize time.Duration) *core.TimestampedAggregationValue {
	// type TimestampedAggregationValue struct {
	// 	Timestamp  time.Time
	// 	BucketSize time.Duration
	// 	AggregationValue
	// }
	// type AggregationValue struct {
	// 	Count *uint64

	// 	Aggregations map[string]MetricValue
	// }

	tav := core.TimestampedAggregationValue{
		Timestamp:  bp.End, // TODO Fix the implementation in Hawkular Client to return time.Time
		BucketSize: bucketSize,
		AggregationValue: core.AggregationValue{
			Count: &bp.Samples,
		},
	}

	// for _, a := range aggregations {
	//     switch(a) {
	//         case
	//     }
	// }
}

func (h *hawkularSink) datapointToMetricValue(dp *metrics.Datapoint, mt *metrics.MetricType) *core.TimestampedMetricValue {
	mv := core.MetricValue{
		MetricType: hawkularTypeToHeapsterType(mt),
	}

	switch mt {
	case metrics.Counter:
		mv.ValueType = core.ValueInt64
		if v, ok := dp.Value.(int64); ok {
			mv.IntValue = int64(dp.Value)
		}
	case metrics.Gauge:
		mv.ValueType = core.ValueFloat
		if v, ok := dp.Value.(float64); ok {
			mv.FloatValue = float32(float64(dp.Value))
		}
	}

	return core.TimestampedMetricValue{
		Timestamp:   dp.Timestamp,
		MetricValue: mv,
	}
}

func (h *hawkularSink) metricNameToHawkularType(metricName string) metrics.MetricType {
	for _, metric := range core.AllMetrics {
		if metric.Name == metricName {
			return heapsterTypeToHawkularType(metric.Type)
		}
	}
	return metrics.Gauge
}

func (h *hawkularSink) keyToTags(metricKey *core.HistoricalKey) map[string]string {
	tags := make(map[string]string)
	tags[core.LabelMetricSetType.Key] = metricKey.ObjectType

	switch metricKey.ObjectType {
	case core.MetricSetTypeSystemContainer:
		tags[core.LabelNodename.Key] = metricKey.NodeName
		tags[core.LabelContainerName.Key] = metricKey.ContainerName
	case core.MetricSetTypePodContainer:
		tags[core.LabelContainerName.Key] = metricKey.ContainerName

		if key.PodId != "" {
			tags[core.LabelPodId.Key] = metricKey.PodId
		} else {
			tags[core.LabelPodName.Key] = metricKey.PodName
			tags[core.LabelNamespaceName.Key] = namespace
		}
	case core.MetricSetTypePod:
		if key.PodId != "" {
			tags[core.LabelPodId.Key] = metricKey.PodId
		} else {
			tags[core.LabelNamespaceName.Key] = namespace
			tags[core.LabelPodName.Key] = metricKey.PodName
		}
	case core.MetricSetTypeNamespace:
		tags[core.LabelNamespaceName.Key] = namespace
	case core.MetricSetTypeNode:
		tags[core.LabelNodename.Key] = metricKey.NodeName
	case core.MetricSetTypeCluster:
		// System tenant?
	}
	return tags
}
