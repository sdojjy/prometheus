// Copyright 2016 The Prometheus Authors
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

package push

import (
	"encoding/json"
	"io/ioutil"
	"net/http"
	"sort"
	"sync"

	"github.com/go-kit/kit/log/level"

	"github.com/go-kit/kit/log"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/storage"
)

// Manager maintains a set of scrape pools and manages start/stop cycles
// when receiving new target groups form the discovery manager.
type Manager struct {
	logger log.Logger
	append storage.Appendable
	cache  map[string]uint64
	lck    sync.RWMutex
}

// NewManager is the Manager constructor
func NewManager(logger log.Logger, app storage.Appendable) *Manager {
	if logger == nil {
		logger = log.NewNopLogger()
	}
	m := &Manager{
		append: app,
		logger: logger,
		cache:  map[string]uint64{},
		lck:    sync.RWMutex{},
	}
	return m
}

type Payload struct {
	TimeSeries []TimeSeries `json:"series"`
}

// Label is a key/value pair of strings.
type Label struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

type TimeSeries struct {
	Labels  []Label  `json:"labels"`
	Samples []Sample `json:"samples"`
}

type Sample struct {
	Timestamp int64   `json:"timestamp"`
	Value     float64 `json:"value"`
}

func (m *Manager) Append(r *http.Request) (err error) {
	var (
		b  []byte
		id uint64
	)
	ctx := r.Context()

	b, err = ioutil.ReadAll(r.Body)
	defer r.Body.Close()
	if err != nil {
		return err
	}

	pb := &Payload{}
	err = json.Unmarshal(b, pb)
	if err != nil {
		return err
	}

	appender := m.append.Appender(ctx)
	defer func() {
		if err != nil {
			level.Error(m.logger).Log("msg", "add failed", "err", err)
			appender.Rollback()
			return
		}
		err = appender.Commit()
		if err != nil {
			level.Error(m.logger).Log("msg", "commit failed", "err", err)
		}
	}()
	for _, ts := range pb.TimeSeries {
		var ls labels.Labels
		hasNameLabel := false
		for _, label := range ts.Labels {
			ls = append(ls, labels.Label{Name: label.Name, Value: label.Value})
			if label.Name == labels.MetricName {
				hasNameLabel = true
			}
		}
		if len(ls) == 0 {
			level.Error(m.logger).Log("msg", "labels is empty failed")
			continue
		}
		if !hasNameLabel {
			level.Error(m.logger).Log("msg", "no __name__ label is found")
			continue
		}
		sort.Sort(ls)
		labelStr := ls.String()

		m.lck.RLock()
		ref, ok := m.cache[labelStr]
		m.lck.RUnlock()

		for _, sample := range ts.Samples {
			if ok {
				err = appender.AddFast(ref, sample.Timestamp, sample.Value)
				if err == storage.ErrNotFound {
					ok = false
				}
			}

			if !ok {
				id, err = appender.Add(ls, sample.Timestamp, sample.Value)
				if err != nil {
					return err
				}
				m.lck.Lock()
				m.cache[labelStr] = id
				ok = true
				ref = id
				m.lck.Unlock()
			}
		}
	}
	return nil
}
