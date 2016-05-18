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
package core

import (
	"fmt"
	"time"
)

var (
	AggregationTypeAverage      = "average"
	AggregationTypeMaximum      = "max"
	AggregationTypeMinimum      = "min"
	AggregationTypeMedian       = "median"
	AggregationTypeCount        = "count"
	AggregationTypePercentile50 = "50-perc"
	AggregationTypePercentile95 = "95-perc"
	AggregationTypePercentile99 = "99-perc"
)

// MultiTypedAggregations is the list of aggregations that can be either float or int
var MultiTypedAggregations = []string{
	AggregationTypeAverage,
	AggregationTypeMaximum,
	AggregationTypeMinimum,
	AggregationTypeMedian,
	AggregationTypePercentile50,
	AggregationTypePercentile95,
	AggregationTypePercentile99,
}

// AllAggregations is the set of all supported aggregations
var AllAggregations = map[string]bool{
	AggregationTypeAverage:      true,
	AggregationTypeMaximum:      true,
	AggregationTypeMinimum:      true,
	AggregationTypeMedian:       true,
	AggregationTypePercentile50: true,
	AggregationTypePercentile95: true,
	AggregationTypePercentile99: true,
	AggregationTypeCount:        true,
}

// TimestampedMetricValue is a metric value with an associated timestamp
type TimestampedMetricValue struct {
	MetricValue
	Timestamp time.Time
}

// AggregationValue is a description of aggregated MetricValues over time
type AggregationValue struct {
	Count *uint64

	Aggregations map[string]MetricValue
}

// TimestampedAggregationValue is an aggregation value with an associated timestamp
// and bucket size
type TimestampedAggregationValue struct {
	Timestamp  time.Time
	BucketSize time.Duration
	AggregationValue
}

// HistoricalKey is an identifier pointing to a particular object.
// Is is composed of an object type (pod, namespace, container, etc) as well
// as a series of fields which identify that object.
type HistoricalKey struct {
	// ObjectType specifies which type of object this is for (pod, namespace, etc)
	// It should be one of the MetricSetType* labels.
	ObjectType string

	// NodeName is used for node and system-container metrics
	NodeName string
	// NamespaceName is used for namespace, pod, and pod-container metrics
	NamespaceName string
	// PodName is used for pod and pod-container metrics
	PodName string
	// ContainerName is used for system-container and pod-container metrics
	ContainerName string
	// PodId may be used in place of the combination of PodName and NamespaceName for pod and pod-container metrics
	PodId string
}

func (key *HistoricalKey) String() string {
	prefix := fmt.Sprintf("(%s)", key.ObjectType)

	var path string = "[unknown type]"
	switch key.ObjectType {
	case MetricSetTypeSystemContainer:
		path = fmt.Sprintf("node:%s/container:%s", key.NodeName, key.ContainerName)
	case MetricSetTypePodContainer:
		if key.PodId != "" {
			path = fmt.Sprintf("poduid:%s/container:%s", key.PodId, key.ContainerName)
		} else {
			path = fmt.Sprintf("ns:%s/pod:%s/container:%s", key.NamespaceName, key.PodName, key.ContainerName)
		}
	case MetricSetTypePod:
		if key.PodId != "" {
			path = fmt.Sprintf("poduid:%s", key.PodId)
		} else {
			path = fmt.Sprintf("ns:%s/pod:%s", key.NamespaceName, key.PodName)
		}
	case MetricSetTypeNamespace:
		path = fmt.Sprintf("ns:%s", key.NamespaceName)
	case MetricSetTypeNode:
		path = fmt.Sprintf("node:%s", key.NodeName)
	case MetricSetTypeCluster:
		path = "[cluster]"
	}

	return prefix + path
}

// HistoricalSource allows for retrieval of historical metrics and aggregations from sinks
type HistoricalSource interface {
	// GetMetric retrieves the given metric for one or more objects (specified by metricKeys) of
	// the same type, within the given time interval
	GetMetric(metricName string, metricKeys []HistoricalKey, start, end time.Time) (map[HistoricalKey][]TimestampedMetricValue, error)

	// GetAggregation fetches the given aggregations for one or more objects (specified by metricKeys) of
	// the same type, within the given time interval, calculated over a series of buckets
	GetAggregation(metricName string, aggregations []string, metricKeys []HistoricalKey, start, end time.Time, bucketSize time.Duration) (map[HistoricalKey][]TimestampedAggregationValue, error)

	// GetMetricNames retrieves the available metric names for the given object
	GetMetricNames(metricKey HistoricalKey) ([]string, error)

	// GetNodes retrieves the list of nodes in the cluster
	GetNodes() ([]string, error)
	// GetNamespaces retrieves the list of namespaces in the cluster
	GetNamespaces() ([]string, error)
	// GetPodsFromNamespace retrieves the list of pods in a given namespace
	GetPodsFromNamespace(namespace string) ([]string, error)
	// GetSystemContainersFromNode retrieves the list of free containers for a given node
	GetSystemContainersFromNode(node string) ([]string, error)
}

// AsHistoricalSource represents sinks which support a historical access interface
type AsHistoricalSource interface {
	// Historical returns the historical data access interface for this sink
	Historical() HistoricalSource
}
