package mindreader

import (
	"testing"
	"time"

	"github.com/streamingfast/bstream"
	"github.com/streamingfast/shutter"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMindReaderPlugin_ReadFlow(t *testing.T) {
	a := NewTestArchiver()
	mindReader, err := testNewMindReaderPlugin(a, 0, 0)
	mindReader.OnTerminating(func(err error) {
		t.Error("should not be called", err)
	})
	require.NoError(t, err)

	mindReader.Launch()

	mindReader.LogLine(`DMLOG {"id":"0000004ez"}`)

	a.consumeBlockFromChannel(t, 5*time.Millisecond)

	assert.Equal(t, 1, len(a.blocks))
	assert.Equal(t, "0000004ez", a.blocks[0].ID())
}

func TestMindReaderPlugin_GatePassed(t *testing.T) {
	a := NewTestArchiver()

	mindReader, err := testNewMindReaderPlugin(a, 2, 0)
	mindReader.OnTerminating(func(_ error) {
		t.Error("should not be called")
	})
	require.NoError(t, err)

	mindReader.Launch()

	mindReader.LogLine(`DMLOG {"id":"00000001a"}`)
	mindReader.LogLine(`DMLOG {"id":"00000002a"}`)

	a.consumeBlockFromChannel(t, 5*time.Millisecond)

	assert.Equal(t, 1, len(a.blocks))
	assert.Equal(t, "00000002a", a.blocks[0].ID())
}

func TestMindReaderPlugin_StopAtBlockNumReached(t *testing.T) {
	a := NewTestArchiver()

	done := make(chan interface{})
	mindReader, err := testNewMindReaderPlugin(a, 0, 1)
	mindReader.OnTerminating(func(err error) {
		if err == nil {
			close(done)
		} else {
			t.Error("should not be called")
		}
	})
	require.NoError(t, err)

	mindReader.Launch()

	mindReader.LogLine(`DMLOG {"id":"00000001a"}`)
	a.consumeBlockFromChannel(t, 5*time.Millisecond)

	mindReader.LogLine(`DMLOG {"id":"00000002a"}`)

	select {
	case <-done:
	case <-time.After(10 * time.Millisecond):
		t.Error("should have call onComplete at this point")
	}

	// Validate actually read block
	assert.True(t, len(a.blocks) >= 1) // moderate requirement, race condition can make it pass more blocks
	assert.Equal(t, "00000001a", a.blocks[0].ID())
}

type TestArchiver struct {
	*shutter.Shutter
	blocks        []*bstream.Block
	receivedBlock chan *bstream.Block
}

func NewTestArchiver() *TestArchiver {
	return &TestArchiver{
		blocks:        []*bstream.Block{},
		receivedBlock: make(chan *bstream.Block),
	}
}

func (s *TestArchiver) Init() error {
	return nil
}

func (s *TestArchiver) StoreBlock(block *bstream.Block) error {
	s.blocks = append(s.blocks, block)
	s.receivedBlock <- block
	return nil
}

func (s *TestArchiver) consumeBlockFromChannel(t *testing.T, timeout time.Duration) *bstream.Block {
	t.Helper()

	select {
	case blk := <-s.receivedBlock:
		return blk
	case <-time.After(timeout):
		t.Errorf("should have read a block after %s", timeout)
	}

	return nil
}

func (s *TestArchiver) Start() {
}
