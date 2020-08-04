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
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/dfuse-io/bstream"
	"github.com/dfuse-io/dstore"
	"github.com/eoscanada/eos-go"
	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func getTestMindReaderPluginCallbacks(t *testing.T) (onError func(error), onComplete func()) {
	t.Helper()

	onError = func(err error) {
		t.Error("should not called", err)
	}

	onComplete = func() {
		t.Error("should not called")
	}

	return
}

func TestMindReaderPlugin_ReadFlow(t *testing.T) {
	s := NewTestStore()

	mindReader, err := testNewMindReaderPlugin(s, 0, 0)
	mindReader.OnTerminating(func(_ error) {
		t.Error("should not be called")
	})
	require.NoError(t, err)

	go mindReader.ReadFlow()

	mindReader.LogLine(`DMLOG {"id":"0000004ez"}`)

	s.consumeBlockFromChannel(t, 5*time.Millisecond)

	assert.Equal(t, 1, len(s.blocks))
	assert.Equal(t, "0000004ez", s.blocks[0].ID())
}

func TestMindReaderPlugin_GatePassed(t *testing.T) {
	s := NewTestStore()

	mindReader, err := testNewMindReaderPlugin(s, 2, 0)
	mindReader.OnTerminating(func(_ error) {
		t.Error("should not be called")
	})
	require.NoError(t, err)

	go mindReader.ReadFlow()

	mindReader.LogLine(`DMLOG {"id":"00000001a"}`)
	mindReader.LogLine(`DMLOG {"id":"00000002a"}`)

	s.consumeBlockFromChannel(t, 5*time.Millisecond)

	assert.Equal(t, 1, len(s.blocks))
	assert.Equal(t, "00000002a", s.blocks[0].ID())
}

func TestMindReaderPlugin_StopAtBlockNumReached(t *testing.T) {
	t.Skip()
	s := NewTestStore()

	done := make(chan interface{})
	mindReader, err := testNewMindReaderPlugin(s, 0, 1)
	mindReader.OnTerminating(func(err error) {
		if err == nil {
			close(done)
		} else {
			t.Error("should not be called")
		}
	})
	require.NoError(t, err)

	go mindReader.ReadFlow()

	mindReader.LogLine(`DMLOG {"id":"00000001a"}`)
	s.consumeBlockFromChannel(t, 5*time.Millisecond)

	mindReader.LogLine(`DMLOG {"id":"00000002a"}`)

	select {
	case <-done:
	case <-time.After(10 * time.Millisecond):
		t.Error("should have call onComplete at this point")
	}

	// Validate actually read block
	assert.True(t, len(s.blocks) >= 1) // moderate requirement, race condition can make it pass more blocks
	assert.Equal(t, "00000001a", s.blocks[0].ID())
}

func TestNewLocalStore(t *testing.T) {
	localArchiveStore, err := dstore.NewDBinStore("/tmp/mr_dest")
	localArchiveMergedStore, err := dstore.NewDBinStore("/tmp/mr_dest_merged")
	require.NoError(t, err)
	archiver := testNewArchiver("/tmp/mr_test", localArchiveStore, localArchiveMergedStore)
	err = archiver.Init()
	require.NoError(t, err)

	mindReader, err := testNewMindReaderPlugin(archiver, 0, 0)
	mindReader.OnTerminating(func(e error) {
		t.Errorf("should not be called: %w", e)
	})
	require.NoError(t, err)

	go mindReader.ReadFlow()

	mindReader.LogLine(`DMLOG {"id":"00000004a"}`)

	time.Sleep(1 * time.Second) //todo: this suck!

	file, err := os.OpenFile(filepath.Join("/tmp/mr_dest/", "0000000004-00010101T000000.0-0000004a-.dbin.zst"), os.O_RDONLY, 0644)
	require.NoError(t, err)

	gzw, err := zstd.NewReader(file)
	require.NoError(t, err)
	data, err := ioutil.ReadAll(gzw)

	require.NoError(t, err)
	assert.JSONEq(t, `{"Id":"00000004a","Number":4,"PreviousId":"","Timestamp":"0001-01-01T00:00:00Z","LibNum":0,"PayloadKind":0,"PayloadVersion":0,"PayloadBuffer":null}`, string(data))
}

func TestNewGSStore(t *testing.T) {
	t.Skip()

	path := "gs://example/dev"
	mergedPath := fmt.Sprintf("%s-merged", path)
	//path := "gs://charlestest1/dev"

	archiveStore, err := dstore.NewDBinStore(path)
	archiveMergedStore, err := dstore.NewDBinStore(mergedPath)
	archiver := testNewArchiver("/tmp/mr_test/", archiveStore, archiveMergedStore)
	err = archiver.Init()
	require.NoError(t, err)

	mindReader, err := testNewMindReaderPlugin(archiver, 1, 0)
	mindReader.OnTerminating(func(_ error) {
		t.Error("should not be called")
	})
	require.NoError(t, err)

	go mindReader.ReadFlow()

	mindReader.LogLine(`DMLOG {"id":"00000004a"}`)

	time.Sleep(2 * time.Second) //todo: this suck!

	exists, err := archiveStore.FileExists(context.Background(), "0000000004-00010101T000000.0-0000004a-")
	require.NoError(t, err)
	require.True(t, exists)
}

func testNewArchiver(path string, store dstore.Store, mergedStore dstore.Store) *HybridArchiver {
	return NewHybridArchiver(store, mergedStore, testBlockWriteFactory, nil, bstream.NewTracker(200), time.Hour*99999, path, testLogger)
}

func testNewMindReaderPlugin(archiver Archiver, startBlock, stopBlock uint64) (*MindReaderPlugin, error) {
	return newMindReaderPlugin(archiver,
		testConsoleReaderFactory,
		testConsoleReaderBlockTransformer,
		&testContinuityChecker{},
		startBlock,
		stopBlock,
		10,
		nil,
		testLogger,
	)
}

var testBlockWriteFactory = bstream.BlockWriterFactoryFunc(newTestBlockWriter)

func newTestBlockWriter(writer io.Writer) (bstream.BlockWriter, error) {
	return &testBlockWriter{
		writer: writer,
	}, nil
}

type testContinuityChecker struct{}

func (t *testContinuityChecker) IsLocked() bool                      { return false }
func (t *testContinuityChecker) Reset()                              {}
func (t *testContinuityChecker) Write(lastSeenBlockNum uint64) error { return nil }

type testBlockWriter struct {
	writer io.Writer
}

func (w *testBlockWriter) Write(block *bstream.Block) error {
	bytes, err := json.Marshal(block)
	if err != nil {
		return nil
	}

	_, err = w.writer.Write(bytes)
	return err
}

func testConsoleReaderFactory(reader io.Reader) (ConsolerReader, error) {
	return newTestConsolerReader(reader), nil
}

type testConsolerReader struct {
	scanner *bufio.Scanner
	done    chan interface{}
}

func newTestConsolerReader(reader io.Reader) *testConsolerReader {
	return &testConsolerReader{
		scanner: bufio.NewScanner(reader),
	}
}

func (c *testConsolerReader) Done() <-chan interface{} {
	return c.done
}

func (c *testConsolerReader) Read() (obj interface{}, err error) {
	success := c.scanner.Scan()
	if !success {
		err := c.scanner.Err()
		if err == nil {
			err = io.EOF
		}

		return nil, err
	}

	return c.scanner.Text()[6:], nil
}

func testConsoleReaderBlockTransformer(obj interface{}) (*bstream.Block, error) {
	content, ok := obj.(string)
	if !ok {
		return nil, fmt.Errorf("expecting type string, got %T", obj)
	}

	type block struct {
		ID string `json:"id"`
	}

	data := new(block)
	err := json.Unmarshal([]byte(content), data)
	if err != nil {
		return nil, fmt.Errorf("marshalling error on '%s': %w", content, err)
	}

	return &bstream.Block{
		Id:     data.ID,
		Number: uint64(eos.BlockNum(data.ID)),
	}, nil
}
