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

package influxdb

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"time"

	"k8s.io/heapster/metrics/core"

	"github.com/golang/glog"
	influxdb "github.com/influxdb/influxdb/client"
	"github.com/influxdb/influxdb/influxql"
)

// Historical indicates that this sink supports being used as a HistoricalSource
func (sink *influxdbSink) Historical() core.HistoricalSource {
	return sink
}

// implementation of HistoricalSource for influxdbSink

// Kube pod and namespace names are limitted to [a-zA-Z0-9-.], while docker also allows
// underscores, so only allow these those characters.  When Influx actually supports bound
// paramaters, this will be less necessary.
var nameAllowedChars = regexp.MustCompile("^[a-zA-Z0-9_.-]+$")

// sanitizeKey errors out if invalid characters are found in the key, since InfluxDB does not widely
// support bound parameters yet (see https://github.com/influxdata/influxdb/pull/6634) and we need to
// sanitize our inputs.
func (sink *influxdbSink) checkSanitizedKey(key *core.HistoricalKey) error {
	if key.NodeName != "" && !nameAllowedChars.MatchString(key.NodeName) {
		return fmt.Errorf("Invalid node name %q", key.NodeName)
	}

	if key.NamespaceName != "" && !nameAllowedChars.MatchString(key.NamespaceName) {
		return fmt.Errorf("Invalid namespace name %q", key.NamespaceName)
	}

	if key.PodName != "" && !nameAllowedChars.MatchString(key.PodName) {
		return fmt.Errorf("Invalid pod name %q", key.PodName)
	}

	// NB: this prevents access to some of the free containers with slashes in their name
	// (e.g. system.slice/foo.bar), but the Heapster API seems to choke on the slashes anyway
	if key.ContainerName != "" && !nameAllowedChars.MatchString(key.ContainerName) {
		return fmt.Errorf("Invalid container name %q", key.ContainerName)
	}

	if key.PodId != "" && !nameAllowedChars.MatchString(key.PodId) {
		return fmt.Errorf("Invalid pod id %q", key.PodId)
	}

	return nil
}

// aggregationFunc converts an aggregation name into the equivalent call to an InfluxQL
// aggregation function
func (sink *influxdbSink) aggregationFunc(aggregationName, fieldName string) string {
	switch aggregationName {
	case core.AggregationTypeAverage:
		return fmt.Sprintf("MEAN(%q)", fieldName)
	case core.AggregationTypeMaximum:
		return fmt.Sprintf("MAX(%q)", fieldName)
	case core.AggregationTypeMinimum:
		return fmt.Sprintf("MIN(%q)", fieldName)
	case core.AggregationTypeMedian:
		return fmt.Sprintf("MEDIAN(%q)", fieldName)
	case core.AggregationTypeCount:
		return fmt.Sprintf("COUNT(%q)", fieldName)
	case core.AggregationTypePercentile50:
		return fmt.Sprintf("PERCENTILE(%q, 50)", fieldName)
	case core.AggregationTypePercentile95:
		return fmt.Sprintf("PERCENTILE(%q, 95)", fieldName)
	case core.AggregationTypePercentile99:
		return fmt.Sprintf("PERCENTILE(%q, 99)", fieldName)
	}

	// This should have been checked by the API level, so something's seriously wrong here
	panic(fmt.Sprintf("Unknown aggregation type %q", aggregationName))
}

// keyToSelector converts a HistoricalKey to a InfluxQL predicate
func (sink *influxdbSink) keyToSelector(key core.HistoricalKey) string {
	typeSel := fmt.Sprintf("type = '%s'", key.ObjectType)
	switch key.ObjectType {
	case core.MetricSetTypeNode:
		return fmt.Sprintf("%s AND %s = '%s'", typeSel, core.LabelNodename.Key, key.NodeName)
	case core.MetricSetTypeSystemContainer:
		return fmt.Sprintf("%s AND %s = '%s' AND %s = '%s'", typeSel, core.LabelContainerName.Key, key.ContainerName, core.LabelNodename.Key, key.NodeName)
	case core.MetricSetTypeCluster:
		return typeSel
	case core.MetricSetTypeNamespace:
		return fmt.Sprintf("%s AND %s = '%s'", typeSel, core.LabelNamespaceName.Key, key.NamespaceName)
	case core.MetricSetTypePod:
		if key.PodId != "" {
			return fmt.Sprintf("%s AND %s = '%s'", typeSel, core.LabelPodId.Key, key.PodId)
		} else {
			return fmt.Sprintf("%s AND %s = '%s' AND %s = '%s'", typeSel, core.LabelNamespaceName.Key, key.NamespaceName, core.LabelPodName.Key, key.PodName)
		}
	case core.MetricSetTypePodContainer:
		if key.PodId != "" {
			return fmt.Sprintf("%s AND %s = '%s' AND %s = '%s'", typeSel, core.LabelPodId.Key, key.PodId, core.LabelContainerName.Key, key.ContainerName)
		} else {
			return fmt.Sprintf("%s AND %s = '%s' AND %s = '%s' AND %s = '%s'", typeSel, core.LabelNamespaceName.Key, key.NamespaceName, core.LabelPodName.Key, key.PodName, core.LabelContainerName.Key, key.ContainerName)
		}
	}

	// These are assigned by the API, so it shouldn't be possible to reach this unless things are really broken
	panic(fmt.Sprintf("Unknown metric type %q", key.ObjectType))
}

// metricToSeriesAndField retrieves the appropriate field name and series name for a given metric
// (this varies depending on whether or not WithFields is enabled)
func (sink *influxdbSink) metricToSeriesAndField(metricName string) (string, string) {
	if sink.c.WithFields {
		seriesName := strings.SplitN(metricName, "/", 2)
		if len(seriesName) > 1 {
			return seriesName[0], seriesName[1]
		} else {
			return seriesName[0], "value"
		}
	} else {
		return metricName, "value"
	}
}

// composeRawQuery creates the InfluxQL query to fetch the given metric values
func (sink *influxdbSink) composeRawQuery(metricName string, metricKeys []core.HistoricalKey, start, end time.Time) string {
	seriesName, fieldName := sink.metricToSeriesAndField(metricName)

	queries := make([]string, len(metricKeys))
	for i, key := range metricKeys {
		pred := sink.keyToSelector(key)
		if !start.IsZero() {
			pred += fmt.Sprintf(" AND time > '%s'", start.Format(time.RFC3339))
		}
		if !end.IsZero() {
			pred += fmt.Sprintf(" AND time < '%s'", end.Format(time.RFC3339))
		}
		queries[i] = fmt.Sprintf("SELECT time, %q FROM %q WHERE %s", fieldName, seriesName, pred)
	}

	return strings.Join(queries, "; ")
}

// parseRawQueryRow parses a set of timestamped metric values from unstructured JSON output into the
// appropriate Heapster form
func (sink *influxdbSink) parseRawQueryRow(rawRow influxql.Row) ([]core.TimestampedMetricValue, error) {
	vals := make([]core.TimestampedMetricValue, len(rawRow.Values))
	wasInt := make(map[string]bool, 1)
	for i, rawVal := range rawRow.Values {
		val := core.TimestampedMetricValue{}

		if ts, err := time.Parse(time.RFC3339, rawVal[0].(string)); err != nil {
			return nil, fmt.Errorf("Unable to parse timestamp %q in series %q", rawVal[0].(string), rawRow.Name)
		} else {
			val.Timestamp = ts
		}

		if err := tryParseMetricValue("value", rawVal, &val.MetricValue, 1, wasInt); err != nil {
			glog.Errorf("Unable to parse field \"value\" in series %q: %v", rawRow.Name, err)
			return nil, fmt.Errorf("Unable to parse values in series %q", rawRow.Name)
		}

		vals[i] = val
	}

	if wasInt["value"] {
		for i := range vals {
			vals[i].MetricValue.ValueType = core.ValueInt64
		}
	} else {
		for i := range vals {
			vals[i].MetricValue.ValueType = core.ValueFloat
		}
	}

	return vals, nil
}

// GetMetric retrieves the given metric for one or more objects (specified by metricKeys) of
// the same type, within the given time interval
func (sink *influxdbSink) GetMetric(metricName string, metricKeys []core.HistoricalKey, start, end time.Time) (map[core.HistoricalKey][]core.TimestampedMetricValue, error) {
	for _, key := range metricKeys {
		if err := sink.checkSanitizedKey(&key); err != nil {
			return nil, err
		}
	}

	query := sink.composeRawQuery(metricName, metricKeys, start, end)

	sink.RLock()
	defer sink.RUnlock()

	resp, err := sink.runQuery(query)
	if err != nil {
		return nil, err
	}

	res := make(map[core.HistoricalKey][]core.TimestampedMetricValue, len(metricKeys))
	for i, key := range metricKeys {
		if len(resp[i].Series) < 1 {
			return nil, fmt.Errorf("No results for metric %q describing %q", metricName, key.String())
		}

		vals, err := sink.parseRawQueryRow(resp[i].Series[0])
		if err != nil {
			return nil, err
		}
		res[key] = vals
	}

	return res, nil
}

// composeRawQuery creates the InfluxQL query to fetch the given aggregation values
func (sink *influxdbSink) composeAggregateQuery(metricName string, aggregations []string, metricKeys []core.HistoricalKey, start, end time.Time, bucketSize time.Duration) string {
	seriesName, fieldName := sink.metricToSeriesAndField(metricName)

	var bucketSizeNanoSeconds int64 = 0
	if bucketSize != 0 {
		bucketSizeNanoSeconds = int64(bucketSize.Nanoseconds() / int64(time.Microsecond/time.Nanosecond))
	}

	queries := make([]string, len(metricKeys))
	for i, key := range metricKeys {
		pred := sink.keyToSelector(key)
		if !start.IsZero() {
			pred += fmt.Sprintf(" AND time > '%s'", start.Format(time.RFC3339))
		}
		if !end.IsZero() {
			pred += fmt.Sprintf(" AND time < '%s'", end.Format(time.RFC3339))
		}

		aggParts := make([]string, len(aggregations))
		for i, agg := range aggregations {
			aggParts[i] = sink.aggregationFunc(agg, fieldName)
		}

		queries[i] = fmt.Sprintf("SELECT %s FROM %q WHERE %s", strings.Join(aggParts, ", "), seriesName, pred)

		if bucketSize != 0 {
			// group by time requires we have at least one time bound
			if start.IsZero() && end.IsZero() {
				queries[i] += fmt.Sprintf(" AND time < now()")
			}

			// fill(none) makes sure we skip data points will null values (otherwise we'll get a *bunch* of null
			// values when we go back beyond the time where we started collecting data).
			queries[i] += fmt.Sprintf(" GROUP BY time(%vu) fill(none)", bucketSizeNanoSeconds)
		}
	}

	return strings.Join(queries, "; ")
}

// parseRawQueryRow parses a set of timestamped aggregation values from unstructured JSON output into the
// appropriate Heapster form
func (sink *influxdbSink) parseAggregateQueryRow(rawRow influxql.Row, aggregationLookup map[string]int, bucketSize time.Duration) ([]core.TimestampedAggregationValue, error) {
	vals := make([]core.TimestampedAggregationValue, len(rawRow.Values))
	wasInt := make(map[string]bool, len(aggregationLookup))

	for i, rawVal := range rawRow.Values {
		val := core.TimestampedAggregationValue{
			BucketSize: bucketSize,
			AggregationValue: core.AggregationValue{
				Aggregations: map[string]core.MetricValue{},
			},
		}

		if ts, err := time.Parse(time.RFC3339, rawVal[0].(string)); err != nil {
			return nil, fmt.Errorf("Unable to parse timestamp %q in series %q", rawVal[0].(string), rawRow.Name)
		} else {
			val.Timestamp = ts
		}

		// The Influx client decods numeric fields to json.Number (a string), so we have to try decoding to both types of numbers

		// Count is always a uint64
		if countIndex, ok := aggregationLookup[core.AggregationTypeCount]; ok {
			if err := json.Unmarshal([]byte(rawVal[countIndex].(json.Number).String()), &val.Count); err != nil {
				glog.Errorf("Unable to parse count value in series %q: %v", rawRow.Name, err)
				return nil, fmt.Errorf("Unable to parse values in series %q", rawRow.Name)
			}
		}

		// The rest of the aggregation values can be either float or int, so attempt to parse both
		if err := populateAggregations(rawRow.Name, rawVal, &val, aggregationLookup, wasInt); err != nil {
			return nil, err
		}

		vals[i] = val
	}

	// figure out whether each aggregation was full of float values, or int values
	setAggregationValueTypes(vals, wasInt)

	return vals, nil
}

// GetAggregation fetches the given aggregations for one or more objects (specified by metricKeys) of
// the same type, within the given time interval, calculated over a series of buckets
func (sink *influxdbSink) GetAggregation(metricName string, aggregations []string, metricKeys []core.HistoricalKey, start, end time.Time, bucketSize time.Duration) (map[core.HistoricalKey][]core.TimestampedAggregationValue, error) {
	for _, key := range metricKeys {
		if err := sink.checkSanitizedKey(&key); err != nil {
			return nil, err
		}
	}

	// make it easy to look up where the different aggregations are in the list
	aggregationLookup := make(map[string]int, len(aggregations))
	for i, agg := range aggregations {
		aggregationLookup[agg] = i + 1
	}

	query := sink.composeAggregateQuery(metricName, aggregations, metricKeys, start, end, bucketSize)

	sink.RLock()
	defer sink.RUnlock()

	resp, err := sink.runQuery(query)
	if err != nil {
		return nil, err
	}

	// TODO: when there are too many points (e.g. certain times when a start time is not specified), Influx will sometimes return only a single bucket
	//       instead of returning an error.  We should detect this case and return an error ourselves (or maybe just require a start time at the API level)
	res := make(map[core.HistoricalKey][]core.TimestampedAggregationValue, len(metricKeys))
	for i, key := range metricKeys {
		vals, err := sink.parseAggregateQueryRow(resp[i].Series[0], aggregationLookup, bucketSize)
		if err != nil {
			return nil, err
		}
		res[key] = vals
	}

	return res, nil
}

// setMetricValueIfPresent checks to to if the given metric value is present in the list of raw values, and if so,
// copies it to the output format
func setMetricValueIfPresent(metricName string, rawVal []interface{}, aggregations *core.AggregationValue, indexLookup map[string]int, wasInt map[string]bool) error {
	if fieldIndex, ok := indexLookup[metricName]; ok {
		targetValue := &core.MetricValue{}
		if err := tryParseMetricValue(metricName, rawVal, targetValue, fieldIndex, wasInt); err != nil {
			return err
		}

		aggregations.Aggregations[metricName] = *targetValue
	}

	return nil
}

// tryParseMetricValue attempts to parse a raw metric value into the appropriate go type.
func tryParseMetricValue(metricName string, rawVal []interface{}, targetValue *core.MetricValue, fieldIndex int, wasInt map[string]bool) error {
	// the Influx client decodes numeric fields to json.Number (a string), so we have to deal with that --
	// assume, starting off, that values may be either float or int.  Try int until we fail once, and always
	// try float.  At the end, figure out which is which.

	var rv string
	if rvN, ok := rawVal[fieldIndex].(json.Number); !ok {
		return fmt.Errorf("Value %q of metric %q was not a json.Number", rawVal[fieldIndex], metricName)
	} else {
		rv = rvN.String()
	}

	tryInt := false
	isInt, triedBefore := wasInt[metricName]
	tryInt = isInt || !triedBefore

	if tryInt {
		if err := json.Unmarshal([]byte(rv), &targetValue.IntValue); err != nil {
			wasInt[metricName] = false
		} else {
			wasInt[metricName] = true
		}
	}

	if err := json.Unmarshal([]byte(rv), &targetValue.FloatValue); err != nil {
		return err
	}

	return nil
}

// GetMetricNames retrieves the available metric names for the given object
func (sink *influxdbSink) GetMetricNames(metricKey core.HistoricalKey) ([]string, error) {
	if err := sink.checkSanitizedKey(&metricKey); err != nil {
		return nil, err
	}
	return sink.stringListQuery(fmt.Sprintf("SHOW MEASUREMENTS WHERE %s", sink.keyToSelector(metricKey)), "Unable to list available metrics")
}

// GetNodes retrieves the list of nodes in the cluster
func (sink *influxdbSink) GetNodes() ([]string, error) {
	return sink.stringListQuery(fmt.Sprintf("SHOW TAG VALUES WITH KEY = %s", core.LabelNodename.Key), "Unable to list all nodes")
}

// GetNamespaces retrieves the list of namespaces in the cluster
func (sink *influxdbSink) GetNamespaces() ([]string, error) {
	return sink.stringListQuery(fmt.Sprintf("SHOW TAG VALUES WITH KEY = %s", core.LabelNamespaceName.Key), "Unable to list all namespaces")
}

// GetPodsFromNamespace retrieves the list of pods in a given namespace
func (sink *influxdbSink) GetPodsFromNamespace(namespace string) ([]string, error) {
	if !nameAllowedChars.MatchString(namespace) {
		return nil, fmt.Errorf("Invalid namespace name %q", namespace)
	}
	// This is a bit difficult for the influx query language, so we cheat a bit here --
	// we just get all series for the uptime measurement for pods which match our namespace
	// (any measurement should work here, though)
	q := fmt.Sprintf("SHOW SERIES FROM %q WHERE %s = '%s' AND type = '%s'", core.MetricUptime.MetricDescriptor.Name, core.LabelNamespaceName.Key, namespace, core.MetricSetTypePod)
	return sink.stringListQueryCol(q, core.LabelPodName.Key, fmt.Sprintf("Unable to list pods in namespace %q", namespace))
}

// GetSystemContainersFromNode retrieves the list of free containers for a given node
func (sink *influxdbSink) GetSystemContainersFromNode(node string) ([]string, error) {
	if !nameAllowedChars.MatchString(node) {
		return nil, fmt.Errorf("Invalid node name %q", node)
	}
	// This is a bit difficult for the influx query language, so we cheat a bit here --
	// we just get all series for the uptime measurement for system containers on our node
	// (any measurement should work here, though)
	q := fmt.Sprintf("SHOW SERIES FROM %q WHERE %s = '%s' AND type = '%s'", core.MetricUptime.MetricDescriptor.Name, core.LabelNodename.Key, node, core.MetricSetTypeSystemContainer)
	return sink.stringListQueryCol(q, core.LabelContainerName.Key, fmt.Sprintf("Unable to list system containers on node %q", node))
}

// stringListQueryCol runs the given query, and returns all results from the given column as a string list
func (sink *influxdbSink) stringListQueryCol(q, colName string, errStr string) ([]string, error) {
	resp, err := sink.runQuery(q)
	if err != nil {
		return nil, fmt.Errorf(errStr)
	}

	if len(resp[0].Series) < 1 {
		return nil, fmt.Errorf(errStr)
	}

	colInd := -1
	for i, col := range resp[0].Series[0].Columns {
		if col == colName {
			colInd = i
			break
		}
	}

	if colInd == -1 {
		glog.Errorf("%s: results did not contain the %q column", errStr, core.LabelPodName.Key)
		return nil, fmt.Errorf(errStr)
	}

	res := make([]string, len(resp[0].Series[0].Values))
	for i, rv := range resp[0].Series[0].Values {
		res[i] = rv[colInd].(string)
	}
	return res, nil
}

// stringListQuery runs the given query, and returns all results from the first column as a string list
func (sink *influxdbSink) stringListQuery(q string, errStr string) ([]string, error) {
	resp, err := sink.runQuery(q)
	if err != nil {
		return nil, fmt.Errorf(errStr)
	}

	if len(resp[0].Series) < 1 {
		return nil, fmt.Errorf(errStr)
	}

	res := make([]string, len(resp[0].Series[0].Values))
	for i, rv := range resp[0].Series[0].Values {
		res[i] = rv[0].(string)
	}
	return res, nil
}

// runQuery executes the given query against InfluxDB (using the default database for this sink)
func (sink *influxdbSink) runQuery(queryStr string) ([]influxdb.Result, error) {
	q := influxdb.Query{
		Command:  queryStr,
		Database: sink.c.DbName,
	}

	glog.V(4).Infof("Executing query %q against database %q", q.Command, q.Database)

	resp, err := sink.client.Query(q)
	if err != nil {
		glog.Errorf("Unable to perform query %q against database %q: %v", q.Command, q.Database, err)
		return nil, err
	} else if resp.Error() != nil {
		glog.Errorf("Unable to perform query %q against database %q: %v", q.Command, q.Database, resp.Error())
		return nil, resp.Error()
	}

	if len(resp.Results) < 1 {
		glog.Errorf("Unable to perform query %q against database %q: no results returned", q.Command, q.Database)
		return nil, fmt.Errorf("No results returned")
	}

	return resp.Results, nil
}

// populateAggregations extracts aggregation values from a given data point
func populateAggregations(rawRowName string, rawVal []interface{}, val *core.TimestampedAggregationValue, aggregationLookup map[string]int, wasInt map[string]bool) error {
	for _, aggregation := range core.MultiTypedAggregations {
		if err := setMetricValueIfPresent(aggregation, rawVal, &val.AggregationValue, aggregationLookup, wasInt); err != nil {
			glog.Errorf("Unable to parse field %q in series %q: %v", aggregation, rawRowName, err)
			return fmt.Errorf("Unable to parse values in series %q", rawRowName)
		}
	}

	return nil
}

// setAggregationValueTypes inspects a set of aggregation values and figures out whether each aggregation value
// returned as a float column, or an int column
func setAggregationValueTypes(vals []core.TimestampedAggregationValue, wasInt map[string]bool) {
	for _, aggregation := range core.MultiTypedAggregations {
		if isInt, ok := wasInt[aggregation]; ok && isInt {
			for i := range vals {
				val := vals[i].Aggregations[aggregation]
				val.ValueType = core.ValueInt64
				vals[i].Aggregations[aggregation] = val
			}
		} else if ok {
			for i := range vals {
				val := vals[i].Aggregations[aggregation]
				val.ValueType = core.ValueFloat
				vals[i].Aggregations[aggregation] = val
			}
		}
	}
}
