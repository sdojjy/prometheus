// Copyright 2013 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package trace

import "github.com/prometheus/prometheus/pkg/labels"

var traceLabels []labels.Label

func AddTrace(ls []labels.Label) {
	traceLabels = ls
}

func IsTranceOn() bool {
	return len(traceLabels) > 0
}

func ShouldTrace(lset *labels.Labels) bool {
	tmp := traceLabels
	if tmp == nil || len(tmp) == 0 {
		return false
	}
	for _, lable := range tmp {
		if !lset.Has(lable.Name) || lset.Get(lable.Name) != lable.Value {
			return false
		}
	}
	return true
}
