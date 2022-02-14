package mindreader

import (
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/streamingfast/bstream"
	"github.com/streamingfast/shutter"
)

func TestMindReaderPlugin_ReadFlow(t *testing.T) {
	lines := make(chan string, 1)
	blocks := make(chan *bstream.Block, 1)

	mindReader := &MindReaderPlugin{
		Shutter:       shutter.New(),
		lines:         lines,
		consoleReader: newTestConsolerReader(lines),
		transformer:   testConsoleReaderBlockTransformer,
		startGate:     NewBlockNumberGate(1),
	}

	go func() {
		err := mindReader.readOneMessage(blocks)
		require.NoError(t, err)
	}()

	mindReader.LogLine(`DMLOG {"id":"00000001a"}`)
	select {
	case b := <-blocks:
		{
			require.Equal(t, uint64(01), b.Number)
		}
	case <-time.After(time.Second):
		{
			t.Error("too long")
		}
	}
}

func TestMindReaderPlugin_GatePassed(t *testing.T) {
	lines := make(chan string, 2)
	blocks := make(chan *bstream.Block, 2)

	mindReader := &MindReaderPlugin{
		Shutter:       shutter.New(),
		lines:         lines,
		consoleReader: newTestConsolerReader(lines),
		transformer:   testConsoleReaderBlockTransformer,
		startGate:     NewBlockNumberGate(2),
	}

	go func() {
		for {
			err := mindReader.readOneMessage(blocks)
			require.NoError(t, err)
		}
	}()

	mindReader.LogLine(`DMLOG {"id":"00000001a"}`)
	mindReader.LogLine(`DMLOG {"id":"00000002a"}`)
	select {
	case b := <-blocks:
		{
			require.Equal(t, uint64(02), b.Number)
		}
	case <-time.After(time.Second):
		{
			t.Error("too long")
		}
	}
}

func TestMindReaderPlugin_StopAtBlockNumReached(t *testing.T) {
	lines := make(chan string, 2)
	blocks := make(chan *bstream.Block, 2)
	done := make(chan interface{})

	mindReader := &MindReaderPlugin{
		Shutter:       shutter.New(),
		lines:         lines,
		consoleReader: newTestConsolerReader(lines),
		transformer:   testConsoleReaderBlockTransformer,
		startGate:     NewBlockNumberGate(0),
		stopBlock:     2,
		zlogger:       testLogger,
	}
	mindReader.OnTerminating(func(err error) {
		if err == nil {
			close(done)
		} else {
			t.Error("should not be called")
		}
	})

	go func() {
		for {
			err := mindReader.readOneMessage(blocks)
			require.NoError(t, err)
		}
	}()

	mindReader.LogLine(`DMLOG {"id":"00000001a"}`)
	mindReader.LogLine(`DMLOG {"id":"00000002a"}`)
	select {
	case <-done:
	case <-time.After(1 * time.Millisecond):
		t.Error("too long")
	}

	// Validate actually read block
	assert.Equal(t, 2, len(blocks)) // moderate requirement, race condition can make it pass more blocks
}

func TestMindReaderPlugin_OneBlockSuffixFormat(t *testing.T) {
	assert.NoError(t, validateOneBlockSuffix(""))
	assert.NoError(t, validateOneBlockSuffix("example"))
	assert.NoError(t, validateOneBlockSuffix("example-hostname-123"))
	assert.NoError(t, validateOneBlockSuffix("example_hostname_123"))
	assert.Equal(t, `oneblock_suffix contains invalid characters: "example.lan"`, validateOneBlockSuffix("example.lan").Error())
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
