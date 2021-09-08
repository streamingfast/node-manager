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
	"io/ioutil"
	"log"
	"os"
	"testing"
	"time"

	"github.com/streamingfast/bstream"
	"github.com/streamingfast/dstore"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

var now = time.Now()

func testNewMergeArchiver(path string, store dstore.Store) *MergeArchiver {
	return NewMergeArchiver(store, testBlockWriteFactory, path, testLogger)
}

func genBlocks(nums ...uint64) (out []*bstream.Block) {
	for _, num := range nums {
		out = append(out, &bstream.Block{Number: num, PayloadBuffer: []byte{0x01}, Timestamp: now.Add(-time.Hour).Add(time.Second * time.Duration(num))})
	}
	return
}

func TestArchiverSelector(t *testing.T) {

	tests := []struct {
		name               string
		input              []*bstream.Block
		mergeTimeThreshold time.Duration
		expectMergedBlocks []*bstream.Block
		expectOneBlocks    []*bstream.Block
	}{
		{
			name:               "one block",
			input:              genBlocks(99),
			mergeTimeThreshold: 999 * time.Hour,
			expectMergedBlocks: nil,
			expectOneBlocks:    genBlocks(99),
		},
		{
			name:               "one old block",
			input:              genBlocks(99),
			mergeTimeThreshold: time.Minute,
			expectMergedBlocks: nil,
			expectOneBlocks:    genBlocks(99),
		},
		{
			name:               "one boundary old block",
			input:              genBlocks(100),
			mergeTimeThreshold: time.Minute,
			expectMergedBlocks: genBlocks(100),
			expectOneBlocks:    nil,
		},
		{
			name:               "multiple old blocks starting on boundary",
			input:              genBlocks(100, 101, 102, 103),
			mergeTimeThreshold: time.Minute,
			expectMergedBlocks: genBlocks(100, 101, 102, 103),
			expectOneBlocks:    nil,
		},
		{
			name:               "multiple old blocks traverse boundary",
			input:              genBlocks(98, 99, 100, 101, 102),
			mergeTimeThreshold: time.Minute,
			expectMergedBlocks: genBlocks(100, 101, 102),
			expectOneBlocks:    genBlocks(98, 99, 100),
		},
		{
			name:               "multiple young blocks traverse boundary",
			input:              genBlocks(98, 99, 100, 101, 102),
			mergeTimeThreshold: 999 * time.Hour,
			expectMergedBlocks: nil,
			expectOneBlocks:    genBlocks(98, 99, 100, 101, 102),
		},
		{
			name:               "holes in the stream",
			input:              genBlocks(98, 99, 101, 102),
			mergeTimeThreshold: time.Minute,
			expectMergedBlocks: genBlocks(101, 102),
			expectOneBlocks:    genBlocks(98, 99, 101),
		},
		{
			name:               "from merged to live young blocks",
			input:              genBlocks(98, 99, 101, 102, 199, 200, 201),
			mergeTimeThreshold: (3600 - 199) * time.Second,
			expectMergedBlocks: genBlocks(101, 102, 199, 200),
			expectOneBlocks:    genBlocks(98, 99, 101, 200, 201),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			dir, err := ioutil.TempDir("/tmp", "test-mindreader-archiver-selector")
			if err != nil {
				log.Fatal(err)
			}
			defer os.RemoveAll(dir)

			ma := &testArchiver{}
			oa := &testArchiver{}

			tracker := bstream.NewTracker(0)

			s := NewArchiverSelector(oa, ma, bstream.GetBlockReaderFactory, false, tracker, test.mergeTimeThreshold, dir, zap.NewNop())

			s.Init()

			for _, blk := range test.input {
				err := s.StoreBlock(blk)
				require.NoError(t, err)
			}

			assert.Equal(t, test.expectOneBlocks, oa.blocks)
			assert.Equal(t, test.expectMergedBlocks, ma.blocks)
		})
	}
}
