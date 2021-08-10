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
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"testing"
	"time"

	"github.com/dfuse-io/bstream"
	"github.com/streamingfast/dstore"
	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMindReaderPlugin_ReadFlow(t *testing.T) {
	s := NewTestStore()

	mindReader, err := testNewMindReaderPlugin(s, 0, 0)
	mindReader.OnTerminating(func(_ error) {
		t.Error("should not be called")
	})
	require.NoError(t, err)

	go mindReader.Launch(nil)

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

	go mindReader.Launch(nil)

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

	go mindReader.Launch(nil)

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
	require.NoError(t, err)
	archiver := testNewArchiver("/tmp/mr_test", localArchiveStore)
	err = archiver.Init()
	require.NoError(t, err)

	mindReader, err := testNewMindReaderPlugin(archiver, 0, 0)
	mindReader.OnTerminating(func(e error) {
		t.Errorf("should not be called: %w", e)
	})
	require.NoError(t, err)

	go mindReader.Launch(nil)

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
	//path := "gs://charlestest1/dev"

	archiveStore, err := dstore.NewDBinStore(path)
	archiver := testNewArchiver("/tmp/mr_test/", archiveStore)
	err = archiver.Init()
	require.NoError(t, err)

	mindReader, err := testNewMindReaderPlugin(archiver, 1, 0)
	mindReader.OnTerminating(func(_ error) {
		t.Error("should not be called")
	})
	require.NoError(t, err)

	go mindReader.Launch(nil)

	mindReader.LogLine(`DMLOG {"id":"00000004a"}`)

	time.Sleep(2 * time.Second) //todo: this suck!

	exists, err := archiveStore.FileExists(context.Background(), "0000000004-00010101T000000.0-0000004a-")
	require.NoError(t, err)
	require.True(t, exists)
}

func testNewArchiver(path string, store dstore.Store) *OneBlockArchiver {
	return NewOneBlockArchiver(store, testBlockWriteFactory, path, "", testLogger)
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
		Number: toBlockNum(data.ID),
	}, nil
}

type testArchiver struct {
	blocks []*bstream.Block
}

func (_ *testArchiver) Init() error {
	return nil
}
func (_ *testArchiver) Terminate() <-chan interface{} {
	out := make(chan interface{})
	close(out)
	return out
}

func (_ *testArchiver) Start() {
}

func (a *testArchiver) StoreBlock(block *bstream.Block) error {
	a.blocks = append(a.blocks, block)
	return nil
}

func TestFindFilesToUpload(t *testing.T) {
	tmp, err := ioutil.TempDir(os.TempDir(), "archivertest")
	require.NoError(t, err)
	defer os.RemoveAll(tmp)
	for _, f := range []string{"alpha", "charlie", "bravo", "foxtrot", "echo"} {
		require.NoError(t, createEmptyFile(path.Join(tmp, fmt.Sprintf("%s.merged", f))))
	}
	files, err := findFilesToUpload(tmp, nil, "merged")
	require.NoError(t, err)

	expectedShort := []string{"alpha", "bravo", "charlie", "echo", "foxtrot"}
	var expectedLong []string
	for _, s := range expectedShort {
		expectedLong = append(expectedLong, tmp+"/"+s+".merged")
	}
	assert.Equal(t, files, expectedLong)

}

func createEmptyFile(filename string) error {
	emptyFile, err := os.Create(filename)
	if err != nil {
		return err
	}
	emptyFile.Close()
	return nil
}

func toBlockNum(blockID string) uint64 {
	if len(blockID) < 8 {
		return 0
	}
	bin, err := hex.DecodeString(blockID[:8])
	if err != nil {
		return 0
	}
	return uint64(binary.BigEndian.Uint32(bin))
}
