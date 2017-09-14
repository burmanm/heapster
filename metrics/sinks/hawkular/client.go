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
	"bytes"
	"fmt"
	"hash/fnv"
	"math"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/golang/glog"
	"github.com/hawkular/hawkular-client-go/metrics"
	"k8s.io/heapster/metrics/core"
)

// cacheDefinitions Fetches all known definitions from all tenants (all projects in Openshift)
func (h *hawkularSink) cacheDefinitions() error {
	tds, err := h.client.Tenants()
	if err != nil {
		return err
	}

	// TagsFiltering for definitions
	tagsFilter := make(map[string]string, 1)
	tagsFilter[descriptorTag] = "*"

	m := make([]metrics.Modifier, len(h.modifiers), len(h.modifiers)+1)
	copy(m, h.modifiers)
	m = append(m, metrics.Filters(metrics.TagsFilter(tagsFilter)))

	wG := &sync.WaitGroup{}

	for _, td := range tds {
		fetchModifiers := make([]metrics.Modifier, len(m), len(m)+1)
		copy(fetchModifiers, m)
		fetchModifiers = append(m, metrics.Tenant(td.ID))

		wG.Add(1)
		go func(m ...metrics.Modifier) {
			err := h.updateDefinitions(fetchModifiers...)
			if err != nil {
				fmt.Println(err)
			}
			wG.Done()
		}()

		// Any missing fetches will be cached in the first datapoint store
	}

	wG.Wait()
	glog.V(4).Infof("PreCaching completed, cached %d definitions\n", len(h.reg))

	return nil
}

// Fetches definitions from the server and checks that they're matching the descriptors
func (h *hawkularSink) updateDefinitions(m ...metrics.Modifier) error {
	mds, err := h.client.Definitions(m...)
	if err != nil {
		return err
	}

	for _, p := range mds {
		if model, f := h.models[p.Tags[descriptorTag]]; f && !h.recent(p, model) {
			if err := h.client.UpdateTags(p.Type, p.ID, p.Tags, h.modifiers...); err != nil {
				return err
			}
		}
		h.regLock.Lock()
		h.reg[p.ID] = hash(p)
		h.regLock.Unlock()
	}

	return nil
}

func hash(md *metrics.MetricDefinition) uint64 {
	h := fnv.New64a()

	// Hash metricId first
	// h.Write([]byte(tenant))
	h.Write([]byte(md.Type))
	h.Write([]byte(md.ID))

	// Then tags to allow "recent" checking
	var buffer bytes.Buffer

	tagNames := make([]string, 0, len(md.Tags))
	tagValues := make([]string, 0, len(md.Tags))

	for k, v := range md.Tags {
		tagNames = append(tagNames, k)
		tagValues = append(tagValues, v)
	}

	sort.Strings(tagNames)
	sort.Strings(tagValues)

	for _, tn := range tagNames {
		buffer.WriteString(tn)
	}

	for _, tv := range tagValues {
		buffer.WriteString(tv)
	}

	buffer.WriteTo(h)
	return h.Sum64()
}

// Checks that stored definition is up to date with the model
func (h *hawkularSink) recent(live *metrics.MetricDefinition, model *metrics.MetricDefinition) bool {
	recent := true
	for k := range model.Tags {
		if v, found := live.Tags[k]; !found {
			// There's a label that wasn't in our stored definition
			live.Tags[k] = v
			recent = false
		}
	}

	return recent
}

// Transform the MetricDescriptor to a format used by Hawkular-Metrics
func (h *hawkularSink) descriptorToDefinition(md *core.MetricDescriptor) metrics.MetricDefinition {
	tags := make(map[string]string)
	// Postfix description tags with _description
	for _, l := range md.Labels {
		if len(l.Description) > 0 {
			tags[l.Key+descriptionTag] = l.Description
		}
	}

	if len(md.Units.String()) > 0 {
		tags[unitsTag] = md.Units.String()
	}

	tags[descriptorTag] = md.Name

	hmd := metrics.MetricDefinition{
		ID:   md.Name,
		Tags: tags,
		Type: heapsterTypeToHawkularType(md.Type),
	}

	return hmd
}

func (h *hawkularSink) groupName(ms *core.MetricSet, metricName string) string {
	n := []string{ms.Labels[core.LabelContainerName.Key], metricName}
	return strings.Join(n, separator)
}

func (h *hawkularSink) idName(ms *core.MetricSet, metricName string) string {
	n := make([]string, 0, 3)

	metricType := ms.Labels[core.LabelMetricSetType.Key]
	switch metricType {
	case core.MetricSetTypeNode:
		n = append(n, "machine")
		n = append(n, h.nodeName(ms))
	case core.MetricSetTypeSystemContainer:
		n = append(n, core.MetricSetTypeSystemContainer)
		n = append(n, ms.Labels[core.LabelContainerName.Key])
		n = append(n, ms.Labels[core.LabelPodId.Key])
	case core.MetricSetTypeCluster:
		n = append(n, core.MetricSetTypeCluster)
	case core.MetricSetTypeNamespace:
		n = append(n, core.MetricSetTypeNamespace)
		n = append(n, ms.Labels[core.LabelNamespaceName.Key])
	case core.MetricSetTypePod:
		n = append(n, core.MetricSetTypePod)
		n = append(n, ms.Labels[core.LabelPodId.Key])
	case core.MetricSetTypePodContainer:
		n = append(n, ms.Labels[core.LabelContainerName.Key])
		n = append(n, ms.Labels[core.LabelPodId.Key])
	default:
		n = append(n, ms.Labels[core.LabelContainerName.Key])
		if ms.Labels[core.LabelPodId.Key] != "" {
			n = append(n, ms.Labels[core.LabelPodId.Key])
		} else {
			n = append(n, h.nodeName(ms))
		}
	}

	n = append(n, metricName)

	return strings.Join(n, separator)
}

func (h *hawkularSink) nodeName(ms *core.MetricSet) string {
	if len(h.labelNodeId) > 0 {
		if v, found := ms.Labels[h.labelNodeId]; found {
			return v
		}
		glog.V(4).Infof("The labelNodeId was set to %s but there is no label with this value."+
			"Using the default 'nodename' label instead.", h.labelNodeId)
	}

	return ms.Labels[core.LabelNodename.Key]
}

func (h *hawkularSink) createDefinitionFromModel(ms *core.MetricSet, metric core.LabeledMetric) (*metrics.MetricDefinition, error) {
	if md, f := h.models[metric.Name]; f {
		// Copy the original map
		mdd := *md
		tags := make(map[string]string)
		for k, v := range mdd.Tags {
			tags[k] = v
		}
		mdd.Tags = tags

		// Set tag values
		for k, v := range ms.Labels {
			mdd.Tags[k] = v
			if k == core.LabelLabels.Key {
				labels := strings.Split(v, ",")
				for _, label := range labels {
					labelKeyValue := strings.Split(label, ":")
					if len(labelKeyValue) != 2 {
						glog.V(4).Infof("Could not split the label %v into its key and value pair. This label will not be added as a tag in Hawkular Metrics.", label)
					} else {
						mdd.Tags[h.labelTagPrefix+labelKeyValue[0]] = labelKeyValue[1]
					}
				}
			}
		}

		// Set the labeled values
		for k, v := range metric.Labels {
			mdd.Tags[k] = v
		}

		mdd.Tags[groupTag] = h.groupName(ms, metric.Name)
		mdd.Tags[descriptorTag] = metric.Name

		return &mdd, nil
	}
	return nil, fmt.Errorf("Could not find definition model with name %s", metric.Name)
}

func (h *hawkularSink) registerLabeledIfNecessary(ms *core.MetricSet, metric core.LabeledMetric, m ...metrics.Modifier) (uint64, error) {

	count := uint64(0)
	var key string
	if resourceID, found := metric.Labels[core.LabelResourceID.Key]; found {
		key = h.idName(ms, metric.Name+separator+resourceID)
	} else {
		key = h.idName(ms, metric.Name)
	}

	mdd, err := h.createDefinitionFromModel(ms, metric)
	if err != nil {
		return 0, err
	}

	mddHash := hash(mdd)

	h.regLock.RLock()
	if _, found := h.reg[key]; !found || h.reg[key] != mddHash {
		// I'm going to release the lock to allow concurrent processing, even if that
		// can cause dual updates (highly unlikely). The UpdateTags is idempotent in any case.
		h.regLock.RUnlock()
		m = append(m, h.modifiers...)

		// Create metric, use updateTags instead of Create because we don't care about uniqueness
		if err := h.client.UpdateTags(heapsterTypeToHawkularType(metric.MetricType), key, mdd.Tags, m...); err != nil {
			// Log error and don't add this key to the lookup table
			glog.Errorf("Could not update tags: %s", err)
			return 0, err
		}

		h.regLock.Lock()
		h.reg[key] = mddHash
		h.regLock.Unlock()
		count++
	} else {
		h.regLock.RUnlock()
	}

	return count, nil
}

func toBatches(m []metrics.MetricHeader, batchSize int) chan []metrics.MetricHeader {
	if batchSize == 0 {
		c := make(chan []metrics.MetricHeader, 1)
		c <- m
		return c
	}

	size := int(math.Ceil(float64(len(m)) / float64(batchSize)))
	c := make(chan []metrics.MetricHeader, size)

	for i := 0; i < len(m); i += batchSize {
		n := i + batchSize
		if len(m) < n {
			n = len(m)
		}
		part := m[i:n]
		c <- part
	}

	return c
}

func (h *hawkularSink) sendData(tmhs map[string][]metrics.MetricHeader, wg *sync.WaitGroup) {
	for k, v := range tmhs {
		parts := toBatches(v, h.batchSize)
		close(parts)

		for p := range parts {
			wg.Add(1)
			go func(batch []metrics.MetricHeader, tenant string) {
				defer wg.Done()

				m := make([]metrics.Modifier, len(h.modifiers), len(h.modifiers)+1)
				copy(m, h.modifiers)
				m = append(m, metrics.Tenant(tenant))
				if err := h.client.Write(batch, m...); err != nil {
					glog.Errorf(err.Error())
				}
			}(p, k)
		}
	}
}

// Converts Timeseries to metric structure used by the Hawkular
func (h *hawkularSink) pointToLabeledMetricHeader(ms *core.MetricSet, metric core.LabeledMetric, timestamp time.Time) (*metrics.MetricHeader, error) {

	name := h.idName(ms, metric.Name)
	if resourceID, found := metric.Labels[core.LabelResourceID.Key]; found {
		name = h.idName(ms, metric.Name+separator+resourceID)
	}

	var value float64
	if metric.ValueType == core.ValueInt64 {
		value = float64(metric.IntValue)
	} else {
		value = float64(metric.FloatValue)
	}

	m := metrics.Datapoint{
		Value:     value,
		Timestamp: timestamp,
	}

	mh := &metrics.MetricHeader{
		ID:   name,
		Data: []metrics.Datapoint{m},
		Type: heapsterTypeToHawkularType(metric.MetricType),
	}

	return mh, nil
}

// If Heapster gets filters, remove these..
func parseFilters(v []string) ([]Filter, error) {
	fs := make([]Filter, 0, len(v))
	for _, s := range v {
		p := strings.Index(s, "(")
		if p < 0 {
			return nil, fmt.Errorf("Incorrect syntax in filter parameters, missing (")
		}

		if strings.Index(s, ")") != len(s)-1 {
			return nil, fmt.Errorf("Incorrect syntax in filter parameters, missing )")
		}

		t := Unknown.From(s[:p])
		if t == Unknown {
			return nil, fmt.Errorf("Unknown filter type")
		}

		command := s[p+1 : len(s)-1]

		switch t {
		case Label:
			proto := strings.SplitN(command, ":", 2)
			if len(proto) < 2 {
				return nil, fmt.Errorf("Missing : from label filter")
			}
			r, err := regexp.Compile(proto[1])
			if err != nil {
				return nil, err
			}
			fs = append(fs, labelFilter(proto[0], r))
			break
		case Name:
			r, err := regexp.Compile(command)
			if err != nil {
				return nil, err
			}
			fs = append(fs, nameFilter(r))
			break
		}
	}
	return fs, nil
}

func labelFilter(label string, r *regexp.Regexp) Filter {
	return func(ms *core.MetricSet, metricName string) bool {
		for k, v := range ms.Labels {
			if k == label {
				if r.MatchString(v) {
					return false
				}
			}
		}
		return true
	}
}

func nameFilter(r *regexp.Regexp) Filter {
	return func(ms *core.MetricSet, metricName string) bool {
		return !r.MatchString(metricName)
	}
}
