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
	"bytes"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/streamingfast/bstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var now = time.Now()

func genBlocks(nums ...uint64) (out []*bstream.Block) {
	prevId := ""
	for _, num := range nums {
		if prevId == "" {
			prevId = fmt.Sprintf("%d", num-1)
		}
		blockId := fmt.Sprintf("%d", num)
		out = append(out, &bstream.Block{Id: blockId, PreviousId: prevId, Number: num, Timestamp: now.Add(-time.Hour).Add(time.Second * time.Duration(num))})
		prevId = blockId
	}
	return
}
func genOneBlockFiles(nums ...uint64) (out []string) {
	blks := genBlocks(nums...)
	for _, blk := range blks {
		out = append(out, blockFileNameFromArgs(blk.Number, blk.Timestamp, blk.Id, blk.PreviousId, blk.LibNum, "default.dat"))
	}
	return
}

func TestArchiverSelector(t *testing.T) {
	tests := []struct {
		name                       string
		input                      []*bstream.Block
		mergeTimeThreshold         time.Duration
		expectUploadedMergedBlocks map[uint64][]uint64
		expectBufferedMergedBlocks []uint64
		expectMergedFiles          []string
		expectOneBlocks            []string
	}{
		{
			name:               "one block",
			input:              genBlocks(99),
			mergeTimeThreshold: 999 * time.Hour,
			expectOneBlocks:    genOneBlockFiles(99),
		},
		{
			name:               "one old block",
			input:              genBlocks(99),
			mergeTimeThreshold: time.Minute,
			expectOneBlocks:    genOneBlockFiles(99),
		},
		{
			name:                       "one boundary old block",
			input:                      genBlocks(100),
			mergeTimeThreshold:         time.Minute,
			expectBufferedMergedBlocks: []uint64{100},
			expectOneBlocks:            nil,
		},
		{
			name:                       "multiple old blocks starting on boundary",
			input:                      genBlocks(100, 101, 102, 103),
			mergeTimeThreshold:         time.Minute,
			expectBufferedMergedBlocks: []uint64{100, 101, 102, 103},
			expectOneBlocks:            nil,
		},
		{
			name:                       "multiple old blocks traverse boundary",
			input:                      genBlocks(98, 99, 100, 101, 102),
			mergeTimeThreshold:         time.Minute,
			expectBufferedMergedBlocks: []uint64{100, 101, 102},
			expectOneBlocks:            genOneBlockFiles(98, 99, 100),
		},
		{
			name:               "multiple old blocks traverse multiple boundaries",
			input:              genBlocks(98, 99, 101, 102, 203, 205, 301),
			mergeTimeThreshold: time.Minute,
			expectUploadedMergedBlocks: map[uint64][]uint64{
				100: {101, 102},
				200: {203, 205},
			},
			expectBufferedMergedBlocks: []uint64{301},
			expectOneBlocks:            genOneBlockFiles(98, 99, 101),
		},
		{
			name:               "multiple old blocks traverse multiple boundaries at same time",
			input:              genBlocks(98, 99, 101, 102, 301, 302, 400),
			mergeTimeThreshold: time.Minute,
			expectUploadedMergedBlocks: map[uint64][]uint64{
				100: {101, 102},
				200: nil,
				300: {301, 302},
			},
			expectBufferedMergedBlocks: []uint64{400},
			expectOneBlocks:            genOneBlockFiles(98, 99, 101),
		},
		{
			name:               "multiple old blocks from boundary traverse boundary",
			input:              genBlocks(100, 102, 203, 205),
			mergeTimeThreshold: time.Minute,
			expectUploadedMergedBlocks: map[uint64][]uint64{
				100: {100, 102},
			},
			expectBufferedMergedBlocks: []uint64{203, 205},
		},

		{
			name:               "multiple young blocks traverse boundary",
			input:              genBlocks(98, 99, 100, 101, 102),
			mergeTimeThreshold: 999 * time.Hour,
			expectOneBlocks:    genOneBlockFiles(98, 99, 100, 101, 102),
		},
		{
			name:                       "holes in the stream",
			input:                      genBlocks(98, 99, 101, 102),
			mergeTimeThreshold:         time.Minute,
			expectBufferedMergedBlocks: []uint64{101, 102},
			expectOneBlocks:            genOneBlockFiles(98, 99, 101),
		},
		{
			name:                       "from merged to live young blocks",
			input:                      genBlocks(98, 99, 101, 102, 199, 200, 201),
			mergeTimeThreshold:         (3600 - 199) * time.Second,
			expectUploadedMergedBlocks: map[uint64][]uint64{100: {101, 102, 199}},
			expectBufferedMergedBlocks: nil, // no more merged blocks
			expectOneBlocks:            genOneBlockFiles(98, 99, 101, 200, 201),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			workDir := newWorkDir(t)
			archiver := testNewArchiver(workDir, nil)
			mergeArchiver := testNewMergeArchiver(workDir, nil)
			archiverSelector := testNewArchiverSelector(t, archiver, mergeArchiver)
			archiverSelector.mergeThresholdBlockAge = test.mergeTimeThreshold

			archiverSelector.Init()

			for _, blk := range test.input {
				err := archiverSelector.StoreBlock(blk)
				require.NoError(t, err)
			}

			oneBlocks, mergedFiles, err := getProducedFiles(t, workDir)
			require.NoError(t, err)
			assert.Equal(t, test.expectOneBlocks, oneBlocks)

			uploadedMergedBlocks := readMergedFilesBlockNums(t, mergedFiles)
			fmt.Println("hey got this", test.expectUploadedMergedBlocks, uploadedMergedBlocks)
			if !assert.Equal(t, test.expectUploadedMergedBlocks, uploadedMergedBlocks, "uploaded merged blocks") {
				fmt.Println("got these", uploadedMergedBlocks)
			}

			var bufferedMergedBlocks []uint64
			data, err := io.ReadAll(mergeArchiver.buffer)
			require.NoError(t, err)
			if data != nil {
				bufferedMergedBlocks = readMergedBytesBlockNums(t, data)
			}
			assert.Equal(t, test.expectBufferedMergedBlocks, bufferedMergedBlocks, "buffered merged blocks")

		})
	}
}

func getProducedFiles(t *testing.T, workDir string) (oneBlocks, mergedFiles []string, err error) {
	err = filepath.Walk(workDir, func(path string, info os.FileInfo, err error) error {
		if info.IsDir() {
			return nil
		}
		name := info.Name()
		if strings.HasSuffix(name, "merged") {
			mergedFiles = append(mergedFiles, path)
		} else {
			oneBlocks = append(oneBlocks, name)
		}
		return err
	})
	return
}

func mergedFileBaseNum(filename string) (uint64, error) {
	base := strings.TrimSuffix(path.Base(filename), ".merged")
	return strconv.ParseUint(base, 10, 64)

}

func readMergedFilesBlockNums(t *testing.T, mergedFiles []string) map[uint64][]uint64 {
	t.Helper()
	mergedBlocks := make(map[uint64][]uint64)
	for _, mf := range mergedFiles {
		f, err := os.Open(mf)
		require.NoError(t, err)

		mfNum, err := mergedFileBaseNum(mf)
		require.NoError(t, err)

		//cnt, err := io.ReadAll(f)
		//require.NoError(t, err)
		//fmt.Println(string(cnt))
		data, err := io.ReadAll(f)
		require.NoError(t, err)

		mb := readMergedBytesBlockNums(t, data)
		mergedBlocks[mfNum] = mb
	}
	if len(mergedBlocks) > 0 {
		return mergedBlocks
	}

	return nil
}

func readMergedBytesBlockNums(t *testing.T, data []byte) (out []uint64) {
	t.Helper()
	blocks := bytes.Split(data, []byte("}{"))
	for _, blk := range blocks {
		if len(blk) == 0 {
			continue
		}
		if blk[0] != byte('{') {
			blk = append([]byte("{"), blk...)
		}
		if blk[len(blk)-1] != byte('}') {
			blk = append(blk, byte('}'))
		}

		br, err := testBlockReadFactory(bytes.NewReader(blk))
		require.NoError(t, err)
		b, err := br.Read()
		if err != nil {
			fmt.Println("got error", err)
			break
		}
		out = append(out, b.Number)
	}

	return
}
