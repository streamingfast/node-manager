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
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/streamingfast/bstream"
	"github.com/streamingfast/merger/bundle"
)

func TestArchiver_StoreBlock(t *testing.T) {
	io := &TestArchiverIO{}
	archiver := NewArchiver(5, io, false, nil, time.Hour, testLogger)

	srcOneBlockFiles := []*bundle.OneBlockFile{
		bundle.MustNewOneBlockFile("0000000001-20210728T105016.01-00000001a-00000000a-0-suffix"),
		bundle.MustNewOneBlockFile("0000000002-20210728T105016.02-00000002a-00000001a-0-suffix"),
		bundle.MustNewOneBlockFile("0000000003-20210728T105016.03-00000003a-00000002a-0-suffix"),
		bundle.MustNewOneBlockFile("0000000004-20210728T105016.06-00000004a-00000003a-2-suffix"),
		bundle.MustNewOneBlockFile("0000000006-20210728T105016.08-00000006a-00000004a-2-suffix"),
	}

	io.StoreMergeableOneBlockFileFunc = func(ctx context.Context, fileName string, block *bstream.Block) error {
		fmt.Println("StoreMergeableOneBlockFileFunc:", fileName)
		return nil
	}
	io.MergeAndStoreFunc = func(inclusiveLowerBlock uint64, oneBlockFiles []*bundle.OneBlockFile) (err error) {
		fmt.Println("merging bundle:", inclusiveLowerBlock)
		return nil
	}
	//	mergerIO.FetchOneBlockFilesFunc = func(ctx context.Context) (oneBlockFiles []*bundle.OneBlockFile, err error) {
	//		return srcOneBlockFiles, nil
	//	}
	//
	//	var mergedFiles []*bundle.OneBlockFile
	//	mergerIO.MergeAndStoreFunc = func(inclusiveLowerBlock uint64, oneBlockFiles []*bundle.OneBlockFile) (err error) {
	//		defer merger.Shutdown(nil)
	//		mergedFiles = oneBlockFiles
	//		return nil
	//	}
	//
	//	var deletedFiles []*bundle.OneBlockFile
	//	merger.deleteFilesFunc = func(oneBlockFiles []*bundle.OneBlockFile) {
	//		deletedFiles = append(deletedFiles, oneBlockFiles...)
	//	}
	//
	//	go func() {
	//		select {
	//		case <-time.After(time.Second):
	//			panic("too long")
	//		case <-merger.Terminated():
	//		}
	//	}()
	//
	ctx := context.Background()
	for _, oneBlockFile := range srcOneBlockFiles {
		err := archiver.storeBlock(ctx, oneBlockFile, oneBlockFile2Block(oneBlockFile))
		require.NoError(t, err)
	}

	//	require.NoError(t, err)
	//
	//	assert.Len(t, deletedFiles, 4)
	//	assert.Equal(t, bundle.ToSortedIDs(srcOneBlockFiles[0:4]), bundle.ToSortedIDs(deletedFiles))
	//	assert.Len(t, mergedFiles, 4)
	//	assert.Equal(t, srcOneBlockFiles[0:4], mergedFiles)
	//
}

func oneBlockFile2Block(oneBlockFile *bundle.OneBlockFile) *bstream.Block {
	return &bstream.Block{
		Id:             oneBlockFile.ID,
		Number:         oneBlockFile.Num,
		PreviousId:     oneBlockFile.PreviousID,
		Timestamp:      oneBlockFile.BlockTime,
		LibNum:         oneBlockFile.LibNum(),
		PayloadKind:    0,
		PayloadVersion: 0,
		Payload:        nil,
	}
}

type TestArchiverIO struct {
	MergeAndStoreFunc            func(inclusiveLowerBlock uint64, oneBlockFiles []*bundle.OneBlockFile) (err error)
	FetchMergedOneBlockFilesFunc func(lowBlockNum uint64) ([]*bundle.OneBlockFile, error)
	FetchOneBlockFilesFunc       func(ctx context.Context) (oneBlockFiles []*bundle.OneBlockFile, err error)
	DownloadOneBlockFileFunc     func(ctx context.Context, oneBlockFile *bundle.OneBlockFile) (data []byte, err error)

	StoreOneBlockFileFunc          func(ctx context.Context, fileName string, block *bstream.Block) error
	StoreMergeableOneBlockFileFunc func(ctx context.Context, fileName string, block *bstream.Block) error
	DeleteOneBlockFilesFunc        func(ctx context.Context, oneBlockFiles []*bundle.OneBlockFile) error
	WalkMergeableOneBlockFilesFunc func(ctx context.Context) []*bundle.OneBlockFile
}

func (io *TestArchiverIO) MergeAndStore(inclusiveLowerBlock uint64, oneBlockFiles []*bundle.OneBlockFile) (err error) {
	if io.MergeAndStoreFunc == nil {
		return nil
	}
	return io.MergeAndStoreFunc(inclusiveLowerBlock, oneBlockFiles)
}

func (io *TestArchiverIO) FetchMergedOneBlockFiles(lowBlockNum uint64) ([]*bundle.OneBlockFile, error) {
	if io.FetchMergedOneBlockFilesFunc == nil {
		return nil, nil
	}
	return io.FetchMergedOneBlockFilesFunc(lowBlockNum)
}

func (io *TestArchiverIO) FetchOneBlockFiles(ctx context.Context) (oneBlockFiles []*bundle.OneBlockFile, err error) {
	if io.FetchOneBlockFilesFunc == nil {
		return nil, nil
	}
	return io.FetchOneBlockFilesFunc(ctx)
}

func (io *TestArchiverIO) DownloadOneBlockFile(ctx context.Context, oneBlockFile *bundle.OneBlockFile) (data []byte, err error) {
	if io.DownloadOneBlockFileFunc == nil {
		return nil, nil
	}
	return io.DownloadOneBlockFileFunc(ctx, oneBlockFile)
}

func (io *TestArchiverIO) StoreOneBlockFile(ctx context.Context, fileName string, block *bstream.Block) error {
	if io.StoreOneBlockFileFunc == nil {
		return nil
	}
	return io.StoreOneBlockFileFunc(ctx, fileName, block)
}
func (io *TestArchiverIO) StoreMergeableOneBlockFile(ctx context.Context, fileName string, block *bstream.Block) error {
	if io.StoreMergeableOneBlockFileFunc == nil {
		return nil
	}
	return io.StoreMergeableOneBlockFileFunc(ctx, fileName, block)
}

func (io *TestArchiverIO) DeleteOneBlockFiles(ctx context.Context, oneBlockFiles []*bundle.OneBlockFile) error {
	if io.DeleteOneBlockFilesFunc == nil {
		return nil
	}
	return io.DeleteOneBlockFilesFunc(ctx, oneBlockFiles)
}

func (io *TestArchiverIO) WalkMergeableOneBlockFiles(ctx context.Context) ([]*bundle.OneBlockFile, error) {
	if io.WalkMergeableOneBlockFilesFunc == nil {
		return nil, nil
	}

	return io.WalkMergeableOneBlockFilesFunc(ctx), nil
}

//var now = time.Now()
//
//func genBlocks(nums ...uint64) (out []*bstream.Block) {
//	prevId := ""
//	for _, num := range nums {
//		if prevId == "" {
//			prevId = fmt.Sprintf("%d", num-1)
//		}
//		blockId := fmt.Sprintf("%d", num)
//		out = append(out, &bstream.Block{Id: blockId, PreviousId: prevId, Number: num, Timestamp: now.Add(-time.Hour).Add(time.Second * time.Duration(num))})
//		prevId = blockId
//	}
//	return
//}
//func genOneBlockFiles(nums ...uint64) (out []string) {
//	blks := genBlocks(nums...)
//	for _, blk := range blks {
//		out = append(out, blockFileNameFromArgs(blk.Number, blk.Timestamp, blk.Id, blk.PreviousId, blk.LibNum, "default.dat"))
//	}
//	return
//}
//
//func TestArchiverSelector(t *testing.T) {
//	t.Skip()
//	//tests := []struct {
//	//	name                       string
//	//	input                      []*bstream.Block
//	//	mergeTimeThreshold         time.Duration
//	//	expectUploadedMergedBlocks map[uint64][]uint64
//	//	expectBufferedMergedBlocks []uint64
//	//	expectMergedFiles          []string
//	//	expectOneBlocks            []string
//	//}{
//	//	{
//	//		name:               "one block",
//	//		input:              genBlocks(99),
//	//		mergeTimeThreshold: 999 * time.Hour,
//	//		expectOneBlocks:    genOneBlockFiles(99),
//	//	},
//	//	{
//	//		name:               "one old block",
//	//		input:              genBlocks(99),
//	//		mergeTimeThreshold: time.Minute,
//	//		expectOneBlocks:    genOneBlockFiles(99),
//	//	},
//	//	{
//	//		name:                       "one boundary old block",
//	//		input:                      genBlocks(100),
//	//		mergeTimeThreshold:         time.Minute,
//	//		expectBufferedMergedBlocks: []uint64{100},
//	//		expectOneBlocks:            nil,
//	//	},
//	//	{
//	//		name:                       "multiple old blocks starting on boundary",
//	//		input:                      genBlocks(100, 101, 102, 103),
//	//		mergeTimeThreshold:         time.Minute,
//	//		expectBufferedMergedBlocks: []uint64{100, 101, 102, 103},
//	//		expectOneBlocks:            nil,
//	//	},
//	//	{
//	//		name:                       "multiple old blocks traverse boundary",
//	//		input:                      genBlocks(98, 99, 100, 101, 102),
//	//		mergeTimeThreshold:         time.Minute,
//	//		expectBufferedMergedBlocks: []uint64{100, 101, 102},
//	//		expectOneBlocks:            genOneBlockFiles(98, 99, 100),
//	//	},
//	//	{
//	//		name:               "multiple old blocks traverse multiple boundaries",
//	//		input:              genBlocks(98, 99, 101, 102, 203, 205, 301),
//	//		mergeTimeThreshold: time.Minute,
//	//		expectUploadedMergedBlocks: map[uint64][]uint64{
//	//			100: {101, 102},
//	//			200: {203, 205},
//	//		},
//	//		expectBufferedMergedBlocks: []uint64{301},
//	//		expectOneBlocks:            genOneBlockFiles(98, 99, 101),
//	//	},
//	//	{
//	//		name:               "multiple old blocks traverse multiple boundaries at same time",
//	//		input:              genBlocks(98, 99, 101, 102, 301, 302, 400),
//	//		mergeTimeThreshold: time.Minute,
//	//		expectUploadedMergedBlocks: map[uint64][]uint64{
//	//			100: {101, 102},
//	//			200: nil,
//	//			300: {301, 302},
//	//		},
//	//		expectBufferedMergedBlocks: []uint64{400},
//	//		expectOneBlocks:            genOneBlockFiles(98, 99, 101),
//	//	},
//	//	{
//	//		name:               "multiple old blocks from boundary traverse boundary",
//	//		input:              genBlocks(100, 102, 203, 205),
//	//		mergeTimeThreshold: time.Minute,
//	//		expectUploadedMergedBlocks: map[uint64][]uint64{
//	//			100: {100, 102},
//	//		},
//	//		expectBufferedMergedBlocks: []uint64{203, 205},
//	//	},
//	//
//	//	{
//	//		name:               "multiple young blocks traverse boundary",
//	//		input:              genBlocks(98, 99, 100, 101, 102),
//	//		mergeTimeThreshold: 999 * time.Hour,
//	//		expectOneBlocks:    genOneBlockFiles(98, 99, 100, 101, 102),
//	//	},
//	//	{
//	//		name:                       "holes in the stream",
//	//		input:                      genBlocks(98, 99, 101, 102),
//	//		mergeTimeThreshold:         time.Minute,
//	//		expectBufferedMergedBlocks: []uint64{101, 102},
//	//		expectOneBlocks:            genOneBlockFiles(98, 99, 101),
//	//	},
//	//	{
//	//		name:                       "from merged to live young blocks",
//	//		input:                      genBlocks(98, 99, 101, 102, 199, 200, 201),
//	//		mergeTimeThreshold:         (3600 - 199) * time.Second,
//	//		expectUploadedMergedBlocks: map[uint64][]uint64{100: {101, 102, 199}},
//	//		expectBufferedMergedBlocks: nil, // no more merged blocks
//	//		expectOneBlocks:            genOneBlockFiles(98, 99, 101, 200, 201),
//	//	},
//	//}
//	//
//	//for _, test := range tests {
//	//	t.Run(test.name, func(t *testing.T) {
//	//		workDir := newWorkDir(t)
//	//		archiver := testNewArchiver(workDir, nil)
//	//		archiverSelector := testNewArchiverSelector(t, archiver)
//	//		archiverSelector.mergeThresholdBlockAge = test.mergeTimeThreshold
//	//
//	//		archiverSelector.Init()
//	//
//	//		for _, blk := range test.input {
//	//			err := archiverSelector.StoreBlock(blk)
//	//			require.NoError(t, err)
//	//		}
//	//
//	//		oneBlocks, mergedFiles, err := getProducedFiles(t, workDir)
//	//		require.NoError(t, err)
//	//		assert.Equal(t, test.expectOneBlocks, oneBlocks)
//	//
//	//		uploadedMergedBlocks := readMergedFilesBlockNums(t, mergedFiles)
//	//		fmt.Println("hey got this", test.expectUploadedMergedBlocks, uploadedMergedBlocks)
//	//		if !assert.Equal(t, test.expectUploadedMergedBlocks, uploadedMergedBlocks, "uploaded merged blocks") {
//	//			fmt.Println("got these", uploadedMergedBlocks)
//	//		}
//	//
//	//		var bufferedMergedBlocks []uint64
//	//		data, err := io.ReadAll(mergeArchiver.buffer)
//	//		require.NoError(t, err)
//	//		if data != nil {
//	//			bufferedMergedBlocks = readMergedBytesBlockNums(t, data)
//	//		}
//	//		assert.Equal(t, test.expectBufferedMergedBlocks, bufferedMergedBlocks, "buffered merged blocks")
//	//
//	//	})
//	//}
//}
//
//func getProducedFiles(t *testing.T, workDir string) (oneBlocks, mergedFiles []string, err error) {
//	err = filepath.Walk(workDir, func(path string, info os.FileInfo, err error) error {
//		if info.IsDir() {
//			return nil
//		}
//		name := info.Name()
//		if strings.HasSuffix(name, "merged") {
//			mergedFiles = append(mergedFiles, path)
//		} else {
//			oneBlocks = append(oneBlocks, name)
//		}
//		return err
//	})
//	return
//}
//
//func mergedFileBaseNum(filename string) (uint64, error) {
//	base := strings.TrimSuffix(path.Base(filename), ".merged")
//	return strconv.ParseUint(base, 10, 64)
//
//}
//
//func readMergedFilesBlockNums(t *testing.T, mergedFiles []string) map[uint64][]uint64 {
//	t.Helper()
//	mergedBlocks := make(map[uint64][]uint64)
//	for _, mf := range mergedFiles {
//		f, err := os.Open(mf)
//		require.NoError(t, err)
//
//		mfNum, err := mergedFileBaseNum(mf)
//		require.NoError(t, err)
//
//		//cnt, err := io.ReadAll(f)
//		//require.NoError(t, err)
//		//fmt.Println(string(cnt))
//		data, err := io.ReadAll(f)
//		require.NoError(t, err)
//
//		mb := readMergedBytesBlockNums(t, data)
//		mergedBlocks[mfNum] = mb
//	}
//	if len(mergedBlocks) > 0 {
//		return mergedBlocks
//	}
//
//	return nil
//}
//
//func readMergedBytesBlockNums(t *testing.T, data []byte) (out []uint64) {
//	t.Helper()
//	blocks := bytes.Split(data, []byte("}{"))
//	for _, blk := range blocks {
//		if len(blk) == 0 {
//			continue
//		}
//		if blk[0] != byte('{') {
//			blk = append([]byte("{"), blk...)
//		}
//		if blk[len(blk)-1] != byte('}') {
//			blk = append(blk, byte('}'))
//		}
//
//		br, err := testBlockReadFactory(bytes.NewReader(blk))
//		require.NoError(t, err)
//		b, err := br.Read()
//		if err != nil {
//			fmt.Println("got error", err)
//			break
//		}
//		out = append(out, b.Number)
//	}
//
//	return
//}
