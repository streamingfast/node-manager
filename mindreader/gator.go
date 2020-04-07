// Copyright 2019 dfuse Platform Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package mindreader

import (
	"time"

	"github.com/dfuse-io/bstream"
)

type Gator interface {
	pass(block *bstream.Block) bool
}

type TimeThresholdGator struct {
	passed    bool
	threshold time.Duration
}

func NewTimeThresholdGatorr(threshold time.Duration) *TimeThresholdGator {
	return &TimeThresholdGator{
		threshold: threshold,
	}
}

func (g *TimeThresholdGator) pass(block *bstream.Block) bool {
	if g.passed {
		return true
	}

	g.passed = time.Since(block.Time()) < g.threshold
	return g.passed
}

type BlockNumberGator struct {
	passed   bool
	blockNum uint64
}

func NewBlockNumberGator(blockNum uint64) *BlockNumberGator {
	return &BlockNumberGator{
		blockNum: blockNum,
	}
}

func (g *BlockNumberGator) pass(block *bstream.Block) bool {
	if g.passed {
		return true
	}

	g.passed = block.Num() >= g.blockNum
	return g.passed
}
