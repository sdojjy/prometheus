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
	"github.com/go-kit/kit/log/level"
	"io/ioutil"
	"math"
	"net/http"
	"sort"
	"strconv"
	"time"
	"unsafe"

	"github.com/go-kit/kit/log"
	"github.com/pkg/errors"
	"github.com/prometheus/common/model"

	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/storage"
)

// Manager maintains a set of scrape pools and manages start/stop cycles
// when receiving new target groups form the discovery manager.
type Manager struct {
	logger log.Logger
	append storage.Appendable
	//cache  *scrapeCache
}

// NewManager is the Manager constructor
func NewManager(logger log.Logger, app storage.Appendable) *Manager {
	if logger == nil {
		logger = log.NewNopLogger()
	}
	m := &Manager{
		append: app,
		logger: logger,
		//cache:  newScrapeCache(),
	}
	return m
}

//func newScrapeCache() *scrapeCache {
//	return &scrapeCache{
//		series:        map[string]*cacheEntry{},
//		droppedSeries: map[string]*uint64{},
//		seriesCur:     map[uint64]labels.Labels{},
//		seriesPrev:    map[uint64]labels.Labels{},
//		metadata:      map[string]*metaEntry{},
//	}
//}

// scrapeCache tracks mappings of exposed metric strings to label sets and
// storage references. Additionally, it tracks staleness of series between
// scrapes.
//type scrapeCache struct {
//	// How many series and metadata entries there were at the last success.
//	successfulCount int
//
//	// Parsed string to an entry with information about the actual label set
//	// and its storage reference.
//	series map[string]*cacheEntry
//
//	// Cache of dropped metric strings and their iteration. The iteration must
//	// be a pointer so we can update it without setting a new entry with an unsafe
//	// string in addDropped().
//	droppedSeries map[string]*uint64
//
//	// seriesCur and seriesPrev store the labels of series that were seen
//	// in the current and previous scrape.
//	// We hold two maps and swap them out to save allocations.
//	seriesCur  map[uint64]labels.Labels
//	seriesPrev map[uint64]labels.Labels
//
//	metaMtx  sync.Mutex
//	metadata map[string]*metaEntry
//}

//func (c *scrapeCache) iterDone(flushCache bool) {
//	c.metaMtx.Lock()
//	count := len(c.series) + len(c.droppedSeries) + len(c.metadata)
//	c.metaMtx.Unlock()
//
//	if flushCache {
//		c.successfulCount = count
//	} else if count > c.successfulCount*2+1000 {
//		// If a target had varying labels in scrapes that ultimately failed,
//		// the caches would grow indefinitely. Force a flush when this happens.
//		// We use the heuristic that this is a doubling of the cache size
//		// since the last scrape, and allow an additional 1000 in case
//		// initial scrapes all fail.
//		flushCache = true
//	}
//
//	if flushCache {
//		// All caches may grow over time through series churn
//		// or multiple string representations of the same metric. Clean up entries
//		// that haven't appeared in the last scrape.
//		for s, e := range c.series {
//			if c.iter != e.lastIter {
//				delete(c.series, s)
//			}
//		}
//		for s, iter := range c.droppedSeries {
//			if c.iter != *iter {
//				delete(c.droppedSeries, s)
//			}
//		}
//		c.metaMtx.Lock()
//		for m, e := range c.metadata {
//			// Keep metadata around for 10 scrapes after its metric disappeared.
//			if c.iter-e.lastIter > 10 {
//				delete(c.metadata, m)
//			}
//		}
//		c.metaMtx.Unlock()
//
//		c.iter++
//	}
//
//	// Swap current and previous series.
//	c.seriesPrev, c.seriesCur = c.seriesCur, c.seriesPrev
//
//	// We have to delete every single key in the map.
//	for k := range c.seriesCur {
//		delete(c.seriesCur, k)
//	}
//}

//func (c *scrapeCache) get(met string) (*cacheEntry, bool) {
//	e, ok := c.series[met]
//	if !ok {
//		return nil, false
//	}
//	e.lastIter = c.iter
//	return e, true
//}

//func (c *scrapeCache) addRef(met string, ref uint64, lset labels.Labels, hash uint64) {
//	if ref == 0 {
//		return
//	}
//	c.series[met] = &cacheEntry{ref: ref, lastIter: c.iter, lset: lset, hash: hash}
//}

//func (c *scrapeCache) addDropped(met string) {
//	iter := c.iter
//	c.droppedSeries[met] = &iter
//}

//func (c *scrapeCache) getDropped(met string) bool {
//	iterp, ok := c.droppedSeries[met]
//	if ok {
//		*iterp = c.iter
//	}
//	return ok
//}
//func (c *scrapeCache) forEachStale(f func(labels.Labels) bool) {
//	for h, lset := range c.seriesPrev {
//		if _, ok := c.seriesCur[h]; !ok {
//			if !f(lset) {
//				break
//			}
//		}
//	}
//}

//func (c *scrapeCache) setType(metric []byte, t textparse.MetricType) {
//	c.metaMtx.Lock()
//
//	e, ok := c.metadata[yoloString(metric)]
//	if !ok {
//		e = &metaEntry{typ: textparse.MetricTypeUnknown}
//		c.metadata[string(metric)] = e
//	}
//	e.typ = t
//	e.lastIter = c.iter
//
//	c.metaMtx.Unlock()
//}
//
//func (c *scrapeCache) setHelp(metric, help []byte) {
//	c.metaMtx.Lock()
//
//	e, ok := c.metadata[yoloString(metric)]
//	if !ok {
//		e = &metaEntry{typ: textparse.MetricTypeUnknown}
//		c.metadata[string(metric)] = e
//	}
//	if e.help != yoloString(help) {
//		e.help = string(help)
//	}
//	e.lastIter = c.iter
//
//	c.metaMtx.Unlock()
//}
//
//func (c *scrapeCache) setUnit(metric, unit []byte) {
//	c.metaMtx.Lock()
//
//	e, ok := c.metadata[yoloString(metric)]
//	if !ok {
//		e = &metaEntry{typ: textparse.MetricTypeUnknown}
//		c.metadata[string(metric)] = e
//	}
//	if e.unit != yoloString(unit) {
//		e.unit = string(unit)
//	}
//	e.lastIter = c.iter
//
//	c.metaMtx.Unlock()
//}
//
//func (c *scrapeCache) GetMetadata(metric string) (MetricMetadata, bool) {
//	c.metaMtx.Lock()
//	defer c.metaMtx.Unlock()
//
//	m, ok := c.metadata[metric]
//	if !ok {
//		return MetricMetadata{}, false
//	}
//	return MetricMetadata{
//		Metric: metric,
//		Type:   m.typ,
//		Help:   m.help,
//		Unit:   m.unit,
//	}, true
//}
//
//func (c *scrapeCache) ListMetadata() []MetricMetadata {
//	c.metaMtx.Lock()
//	defer c.metaMtx.Unlock()
//
//	res := make([]MetricMetadata, 0, len(c.metadata))
//
//	for m, e := range c.metadata {
//		res = append(res, MetricMetadata{
//			Metric: m,
//			Type:   e.typ,
//			Help:   e.help,
//			Unit:   e.unit,
//		})
//	}
//	return res
//}
//
//// MetadataSize returns the size of the metadata cache.
//func (c *scrapeCache) SizeMetadata() (s int) {
//	c.metaMtx.Lock()
//	defer c.metaMtx.Unlock()
//	for _, e := range c.metadata {
//		s += e.size()
//	}
//
//	return s
//}
//
//// MetadataLen returns the number of metadata entries in the cache.
//func (c *scrapeCache) LengthMetadata() int {
//	c.metaMtx.Lock()
//	defer c.metaMtx.Unlock()
//
//	return len(c.metadata)
//}
//
//// metaEntry holds meta information about a metric.
//type metaEntry struct {
//	lastIter uint64 // Last scrape iteration the entry was observed at.
//	typ      textparse.MetricType
//	help     string
//	unit     string
//}
//
//func (m *metaEntry) size() int {
//	// The attribute lastIter although part of the struct it is not metadata.
//	return len(m.help) + len(m.unit) + len(m.typ)
//}
//
type cacheEntry struct {
	ref      uint64
	lastIter uint64
	hash     uint64
	lset     labels.Labels
}

func yoloString(b []byte) string {
	return *((*string)(unsafe.Pointer(&b)))
}

//
//// MetricMetadata is a piece of metadata for a metric.
//type MetricMetadata struct {
//	Metric string
//	Type   textparse.MetricType
//	Help   string
//	Unit   string
//}

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

func parseDuration(s string) (time.Duration, error) {
	if d, err := strconv.ParseFloat(s, 64); err == nil {
		ts := d * float64(time.Second)
		if ts > float64(math.MaxInt64) || ts < float64(math.MinInt64) {
			return 0, errors.Errorf("cannot parse %q to a valid duration. It overflows int64", s)
		}
		return time.Duration(ts), nil
	}
	if d, err := model.ParseDuration(s); err == nil {
		return time.Duration(d), nil
	}
	return 0, errors.Errorf("cannot parse %q to a valid duration", s)
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
			appender.Rollback()
			return
		}
		err = appender.Commit()
		if err != nil {
			level.Error(m.logger).Log("msg", "Scrape commit failed", "err", err)
		}
	}()
	for _, ts := range pb.TimeSeries {
		var ls labels.Labels
		for _, label := range ts.Labels {
			ls = append(ls, labels.Label{Name: label.Name, Value: label.Value})
		}
		sort.Sort(ls)
		for _, sample := range ts.Samples {
			println(sample.Timestamp, sample.Value)
			id, err = appender.Add(ls, sample.Timestamp, sample.Value)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// Adds samples to the appender, checking the error, and then returns the # of samples added,
// whether the caller should continue to process more samples, and any sample limit errors.

func (sl *scrapeLoop) checkAddError(ce *cacheEntry, met []byte, tp *int64, err error, sampleLimitErr *error, appErrs *appendErrors) (bool, error) {
	switch errors.Cause(err) {
	case nil:
		if tp == nil && ce != nil {
			sl.cache.trackStaleness(ce.hash, ce.lset)
		}
		return true, nil
	case storage.ErrNotFound:
		return false, storage.ErrNotFound
	case storage.ErrOutOfOrderSample:
		appErrs.numOutOfOrder++
		level.Debug(sl.l).Log("msg", "Out of order sample", "series", string(met))
		targetScrapeSampleOutOfOrder.Inc()
		return false, nil
	case storage.ErrDuplicateSampleForTimestamp:
		appErrs.numDuplicates++
		level.Debug(sl.l).Log("msg", "Duplicate sample for timestamp", "series", string(met))
		targetScrapeSampleDuplicate.Inc()
		return false, nil
	case storage.ErrOutOfBounds:
		appErrs.numOutOfBounds++
		level.Debug(sl.l).Log("msg", "Out of bounds metric", "series", string(met))
		targetScrapeSampleOutOfBounds.Inc()
		return false, nil
	case errSampleLimit:
		// Keep on parsing output if we hit the limit, so we report the correct
		// total number of samples scraped.
		*sampleLimitErr = err
		return false, nil
	default:
		return false, err
	}
}
