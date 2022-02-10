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
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"github.com/streamingfast/merger/bundle"
	"io"
	"io/ioutil"
	"os"
	"path"
	"testing"
	"time"

	"github.com/streamingfast/bstream"
	"github.com/streamingfast/dstore"
	"github.com/streamingfast/shutter"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
)

func newLocalTestStore(t *testing.T) dstore.Store {
	t.Helper()

	dir, err := ioutil.TempDir("/tmp", "mrtest")
	require.NoError(t, err)
	t.Cleanup(func() { os.RemoveAll(dir) })

	store, err := dstore.NewDBinStore(dir)
	require.NoError(t, err)

	return store
}

func newWorkDir(t *testing.T) string {
	workDir, err := ioutil.TempDir("/tmp", "mrtest")
	require.NoError(t, err)
	t.Cleanup(func() { os.RemoveAll(workDir) })
	return workDir
}

type TestArchiverIO struct {
	MergeAndSaveFunc             func(inclusiveLowerBlock uint64, oneBlockFiles []*bundle.OneBlockFile) (err error)
	FetchMergedOneBlockFilesFunc func(lowBlockNum uint64) ([]*bundle.OneBlockFile, error)
	FetchOneBlockFilesFunc       func(ctx context.Context) (oneBlockFiles []*bundle.OneBlockFile, err error)
	DownloadOneBlockFileFunc     func(ctx context.Context, oneBlockFile *bundle.OneBlockFile) (data []byte, err error)

	SaveOneBlockFileFunc    func(ctx context.Context, fileName string, block *bstream.Block) error
	DeleteOneBlockFilesFunc func(ctx context.Context, oneBlockFiles []*bundle.OneBlockFile) error
	WalkOneBlockFilesFunc   func(ctx context.Context) []*bundle.OneBlockFile
}

func (io *TestArchiverIO) MergeAndSave(inclusiveLowerBlock uint64, oneBlockFiles []*bundle.OneBlockFile) (err error) {
	return io.MergeAndSaveFunc(inclusiveLowerBlock, oneBlockFiles)
}

func (io *TestArchiverIO) FetchMergedOneBlockFiles(lowBlockNum uint64) ([]*bundle.OneBlockFile, error) {
	return io.FetchMergedOneBlockFilesFunc(lowBlockNum)
}

func (io *TestArchiverIO) FetchOneBlockFiles(ctx context.Context) (oneBlockFiles []*bundle.OneBlockFile, err error) {
	return io.FetchOneBlockFilesFunc(ctx)
}

func (io *TestArchiverIO) DownloadOneBlockFile(ctx context.Context, oneBlockFile *bundle.OneBlockFile) (data []byte, err error) {
	return io.DownloadOneBlockFileFunc(ctx, oneBlockFile)
}

func (io *TestArchiverIO) SaveOneBlockFile(ctx context.Context, fileName string, block *bstream.Block) error {
	return io.SaveOneBlockFileFunc(ctx, fileName, block)
}

func (io *TestArchiverIO) DeleteOneBlockFiles(ctx context.Context, oneBlockFiles []*bundle.OneBlockFile) error {
	return io.DeleteOneBlockFilesFunc(ctx, oneBlockFiles)
}

func (io *TestArchiverIO) WalkOneBlockFiles(ctx context.Context) []*bundle.OneBlockFile {
	return io.WalkOneBlockFilesFunc(ctx)
}

func NewDefaultTestArchiverIO(t *testing.T) ArchiverIO {
	return NewArchiverDStoreIO(
		testBlockWriteFactory,
		testBlockReadFactory,
		newLocalTestStore(t),
		newLocalTestStore(t),
		250,
		1,
		250*time.Millisecond,
	)
}

func TestNewLocalStore(t *testing.T) {
	store := newLocalTestStore(t)
	workDir := newWorkDir(t)
	archiver := testNewArchiver(workDir, store)
	mergeArchiver := testNewMergeArchiver(workDir, store)
	archiverSelector := testNewArchiverSelector(t, archiver, mergeArchiver)

	// FIXME this test should not require using a mindreader, just the archiver
	mindReader, err := testNewMindReaderPlugin(archiverSelector, 0, 0)
	mindReader.OnTerminating(func(e error) {
		t.Errorf("should not be called: %s", e)
	})
	require.NoError(t, err)

	mindReader.Launch()

	mindReader.LogLine(`DMLOG {"id":"00000004a"}`)

	time.Sleep(1 * time.Second) //FIXME: this suck!

	ctx := context.Background()
	expectFileName := blockFileNameFromArgs(4, time.Time{}, "0000004a", "", 0, "default")
	exists, err := store.FileExists(ctx, expectFileName)
	require.NoError(t, err)
	require.True(t, exists)

	file, err := store.OpenObject(ctx, expectFileName)
	require.NoError(t, err)
	data, err := ioutil.ReadAll(file)

	require.NoError(t, err)
	assert.JSONEq(t, `{"Id":"00000004a","Number":4,"PreviousId":"","Timestamp":"0001-01-01T00:00:00Z","LibNum":0,"PayloadKind":0,"PayloadVersion":0,"Payload":null}`, string(data))
}

func testNewArchiverSelector(t *testing.T, oba *OneBlockArchiver, ma *MergeArchiver) *ArchiverSelector {
	return &ArchiverSelector{
		Shutter:          shutter.New(),
		io:               NewDefaultTestArchiverIO(t),
		currentlyMerging: true,
		oneblockArchiver: oba,
		mergeArchiver:    ma,
		logger:           testLogger,
		lastSeenLIB:      atomic.NewUint64(0),
	}
}

func testNewArchiver(path string, store dstore.Store) *OneBlockArchiver {
	return NewOneBlockArchiver(store, testBlockWriteFactory, path, "", testLogger)
}
func testNewMergeArchiver(path string, store dstore.Store) *MergeArchiver {
	a := &MergeArchiver{
		workDir:            path,
		Shutter:            shutter.New(),
		store:              store,
		blockWriterFactory: testBlockWriteFactory,
		logger:             testLogger,
	}
	a.newBuffer()
	return a
}

func testNewMindReaderPlugin(archiver Archiver, startBlock, stopBlock uint64) (*MindReaderPlugin, error) {
	return newMindReaderPlugin(archiver,
		testConsoleReaderFactory,
		testConsoleReaderBlockTransformer,
		startBlock,
		stopBlock,
		10,
		nil,
		nil,
		testLogger,
	)
}

var testBlockWriteFactory = bstream.BlockWriterFactoryFunc(newTestBlockWriter)
var testBlockReadFactory = bstream.BlockReaderFactoryFunc(newTestBlockReader)

func newTestBlockReader(reader io.Reader) (bstream.BlockReader, error) {
	return &testBlockReader{
		reader: reader,
	}, nil
}

type testBlockReader struct {
	reader io.Reader
}

func (r *testBlockReader) Read() (*bstream.Block, error) {
	out := &bstream.Block{}

	b, err := io.ReadAll(r.reader)
	if err != nil {
		return nil, err
	}

	err = json.Unmarshal(b, out)
	return out, err
}

func newTestBlockWriter(writer io.Writer) (bstream.BlockWriter, error) {
	return &testBlockWriter{
		writer: writer,
	}, nil
}

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

func testConsoleReaderFactory(lines chan string) (ConsolerReader, error) {
	return newTestConsolerReader(lines), nil
}

type testConsolerReader struct {
	lines chan string
	done  chan interface{}
}

func newTestConsolerReader(lines chan string) *testConsolerReader {
	return &testConsolerReader{
		lines: lines,
	}
}

func (c *testConsolerReader) Done() <-chan interface{} {
	return c.done
}

func (c *testConsolerReader) Read() (obj interface{}, err error) {
	line, _ := <-c.lines
	obj = line[6:]
	return
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
	*shutter.Shutter
	blocks []*bstream.Block
}

func (a *testArchiver) Init() error {
	return nil
}

func (a *testArchiver) Start() {
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
