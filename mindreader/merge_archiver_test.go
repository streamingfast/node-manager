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

	"github.com/abourget/llerrgroup"
	"github.com/dfuse-io/bstream"
	"github.com/dfuse-io/dbin"
	"github.com/dfuse-io/dstore"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

func init() {
	bstream.GetBlockWriterFactory = bstream.BlockWriterFactoryFunc(func(writer io.Writer) (bstream.BlockWriter, error) {
		return &bstream.TestBlockWriterBin{
			DBinWriter: dbin.NewWriter(writer),
		}, nil
	})
}
func TestMergeArchiver(t *testing.T) {
	mStore := dstore.NewMockStore(nil)
	a := &MergeArchiver{
		zlogger:            zap.NewNop(),
		store:              mStore,
		blockWriterFactory: bstream.GetBlockWriterFactory,
		eg:                 llerrgroup.New(2),
	}

	assert.NoError(t, a.storeBlock(&bstream.Block{Number: 99, PayloadBuffer: []byte{0x01}}))
	assert.Nil(t, a.buffer)

	assert.NoError(t, a.storeBlock(&bstream.Block{Number: 100, PayloadBuffer: []byte{0x01}}))
	assert.NotNil(t, a.buffer)
	assert.Equal(t, uint64(101), a.expectBlock)
	size := a.buffer.Len()

	assert.Error(t, a.storeBlock(&bstream.Block{Number: 99, PayloadBuffer: []byte{0x01}}))

	for i := 101; i < 199; i++ {
		assert.NoError(t, a.storeBlock(&bstream.Block{Number: uint64(i), PayloadBuffer: []byte{0x01}}))
		assert.True(t, a.buffer.Len() > size)
		assert.Equal(t, uint64(i+1), a.expectBlock)
		size = a.buffer.Len()
	}

	assert.NoError(t, a.storeBlock(&bstream.Block{Number: 199, PayloadBuffer: []byte{0x01}}))
	assert.True(t, a.buffer.Len() > size)
	assert.Equal(t, uint64(200), a.expectBlock)
}

func TestMergeArchiverSpecialCase(t *testing.T) {
	mStore := dstore.NewMockStore(nil)
	a := &MergeArchiver{
		store:              mStore,
		blockWriterFactory: bstream.GetBlockWriterFactory,
	}

	assert.NoError(t, a.storeBlock(&bstream.Block{Number: 1, PayloadBuffer: []byte{0x01}}))
	assert.NotNil(t, a.buffer)
	assert.Equal(t, uint64(2), a.expectBlock)
	size := a.buffer.Len()

	for i := 2; i < 99; i++ {
		assert.NoError(t, a.storeBlock(&bstream.Block{Number: uint64(i), PayloadBuffer: []byte{0x01}}))
		assert.True(t, a.buffer.Len() > size)
		assert.Equal(t, uint64(i+1), a.expectBlock)
		size = a.buffer.Len()
	}
}
