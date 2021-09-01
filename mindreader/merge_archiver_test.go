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
	"io"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/streamingfast/bstream"
	"github.com/streamingfast/dbin"
	"github.com/streamingfast/dstore"
	"go.uber.org/zap"
)

func init() {
	bstream.GetBlockWriterFactory = bstream.BlockWriterFactoryFunc(func(writer io.Writer) (bstream.BlockWriter, error) {
		return &bstream.TestBlockWriterBin{
			DBinWriter: dbin.NewWriter(writer),
		}, nil
	})
	bstream.GetBlockReaderFactory = bstream.BlockReaderFactoryFunc(func(reader io.Reader) (bstream.BlockReader, error) {
		return &bstream.TestBlockReaderBin{
			DBinReader: dbin.NewReader(reader),
		}, nil
	})
}
func TestMergeArchiver(t *testing.T) {
	mStore := dstore.NewMockStore(nil)
	a := &MergeArchiver{
		logger:             zap.NewNop(),
		store:              mStore,
		blockWriterFactory: bstream.GetBlockWriterFactory,
	}

	require.NoError(t, a.StoreBlock(&bstream.Block{Number: 99, PayloadBuffer: []byte{0x01}}))
	require.Equal(t, uint64(200), a.nextExclusiveHighestBlockLimit)
	require.Equal(t, uint64(100), a.skipBlockUpTo)
	require.Equal(t, uint64(0), a.currentBlock)
	require.NoError(t, a.StoreBlock(&bstream.Block{Number: 100, PayloadBuffer: []byte{0x01}}))
	require.Equal(t, uint64(100), a.currentBlock)
	prevSize := a.buffer.Len()

	require.NoError(t, a.StoreBlock(&bstream.Block{Number: 99, PayloadBuffer: []byte{0x01}}))
	require.Equal(t, uint64(100), a.skipBlockUpTo)
	require.Equal(t, uint64(100), a.currentBlock)

	for i := 101; i < 200; i++ {
		require.NoError(t, a.StoreBlock(&bstream.Block{Number: uint64(i), PayloadBuffer: []byte{0x01}}))
		require.True(t, a.buffer.Len() > prevSize)
		require.Equal(t, uint64(i), a.currentBlock)
		prevSize = a.buffer.Len()
	}

	require.NoError(t, a.StoreBlock(&bstream.Block{Number: 200, PayloadBuffer: []byte{0x01}}))
	require.Equal(t, uint64(200), a.currentBlock)
	require.Equal(t, uint64(300), a.nextExclusiveHighestBlockLimit)

	require.NoError(t, a.StoreBlock(&bstream.Block{Number: 300, PayloadBuffer: []byte{0x01}}), "should accept any block ...00 after block ...99")
	require.Equal(t, uint64(300), a.currentBlock)
	require.Equal(t, uint64(400), a.nextExclusiveHighestBlockLimit)

}

func TestMergeArchiver_GetProtocolFirstStreamableBlock(t *testing.T) {
	mStore := dstore.NewMockStore(nil)
	a := &MergeArchiver{
		store:              mStore,
		blockWriterFactory: bstream.GetBlockWriterFactory,
	}

	bstream.GetProtocolFirstStreamableBlock = 1
	require.NoError(t, a.StoreBlock(&bstream.Block{Number: 1, PayloadBuffer: []byte{0x01}}))
	require.NotNil(t, a.buffer)
	require.Equal(t, uint64(1), a.currentBlock)
	prevSize := a.buffer.Len()

	for i := 2; i < 99; i++ {
		require.NoError(t, a.StoreBlock(&bstream.Block{Number: uint64(i), PayloadBuffer: []byte{0x01}}))
		require.True(t, a.buffer.Len() > prevSize)
		require.Equal(t, uint64(i), a.currentBlock)
		prevSize = a.buffer.Len()
	}
}
