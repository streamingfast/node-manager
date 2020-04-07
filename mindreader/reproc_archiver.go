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

	"github.com/abourget/llerrgroup"
	"github.com/dfuse-io/bstream"
	"github.com/dfuse-io/dstore"
	"go.uber.org/zap"
)

// ReprocArchiver does the merging in one swift, no need for a merger here.
type ReprocArchiver struct {
	store              dstore.Store
	blockWriterFactory bstream.BlockWriterFactory

	eg          *llerrgroup.Group
	expectBlock uint64
	buffer      *bytes.Buffer
	blockWriter bstream.BlockWriter
}

func NewReprocArchiver(
	store dstore.Store,
	blockWriterFactory bstream.BlockWriterFactory,
) *ReprocArchiver {
	return &ReprocArchiver{
		eg:                 llerrgroup.New(2),
		store:              store,
		blockWriterFactory: blockWriterFactory,
	}
}

func (s *ReprocArchiver) init() error {
	return nil
}

func (s *ReprocArchiver) newBuffer() error {
	s.buffer = &bytes.Buffer{}
	blockWriter, err := s.blockWriterFactory.New(s.buffer)
	if err != nil {
		return fmt.Errorf("blockWriteFactory: %s", err)
	}
	s.blockWriter = blockWriter
	return nil
}

func (s *ReprocArchiver) storeBlock(block *bstream.Block) error {
	if s.buffer == nil && block.Num() < 3 {
		// Special case the beginning of the EOS chain

		if err := s.newBuffer(); err != nil {
			return err
		}
		s.expectBlock = block.Num()
	}

	if block.Num()%100 == 0 {
		if err := s.newBuffer(); err != nil {
			return err
		}

		if s.expectBlock == 0 {
			s.expectBlock = block.Num()
		}
	}

	if s.buffer == nil {
		zlog.Info("ignore blocks before beginning of 100-blocks boundary", zap.Uint64("block_num", block.Num()))
		return nil
	}

	if s.expectBlock != block.Num() {
		return fmt.Errorf("blocks non contiguous, expectedBlock: %d, got block: %d", s.expectBlock, block.Num())
	}
	s.expectBlock++

	if err := s.blockWriter.Write(block); err != nil {
		return fmt.Errorf("blockWriter.Write: %s", err)
	}

	if block.Num()%100 == 99 {

		if s.eg.Stop() {
			return nil // already errored
		}

		baseNum := block.Num() - 99
		baseName := fmt.Sprintf("%010d", baseNum)
		buffer := s.buffer
		s.eg.Go(func() error {
			if baseNum%1000 == 0 {
				zlog.Info("writing merged blocks log (%1000)", zap.String("base_name", baseName))
			}
			err := s.store.WriteObject(baseName, bytes.NewReader(buffer.Bytes()))
			if err != nil {
				return fmt.Errorf("uploading merged file: %s", err)
			}
			return nil
		})
	}

	return nil
}

// uploadFiles does nothing here, it's managed by `storeBlock` when needed
func (s *ReprocArchiver) uploadFiles() error {
	return nil
}
