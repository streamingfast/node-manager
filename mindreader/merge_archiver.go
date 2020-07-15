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
	"context"
	"fmt"
	"time"

	"github.com/abourget/llerrgroup"
	"github.com/dfuse-io/bstream"
	"github.com/dfuse-io/dstore"
	"go.uber.org/zap"
)

// MergeArchiver does the merging in one swift, no need for a merger here.
type MergeArchiver struct {
	store              dstore.Store
	blockWriterFactory bstream.BlockWriterFactory
	stopBlock          uint64
	overflowArchiver   Archiver

	eg          *llerrgroup.Group
	expectBlock uint64
	buffer      *bytes.Buffer
	blockWriter bstream.BlockWriter
	zlogger     *zap.Logger
}

func NewMergeArchiver(
	store dstore.Store,
	blockWriterFactory bstream.BlockWriterFactory,
	stopBlock uint64,
	zlogger *zap.Logger,
	options ...MergeArchiverOption,
) *MergeArchiver {
	ra := &MergeArchiver{
		eg:                 llerrgroup.New(2),
		store:              store,
		stopBlock:          stopBlock,
		blockWriterFactory: blockWriterFactory,
		zlogger:            zlogger,
	}
	for _, opt := range options {
		opt(ra)
	}
	return ra
}

type MergeArchiverOption func(*MergeArchiver)

func WithOverflowArchiver(archiver Archiver) MergeArchiverOption {
	return func(r *MergeArchiver) {
		r.overflowArchiver = archiver
	}
}

func (m *MergeArchiver) Init() error {
	if m.overflowArchiver != nil {
		return m.overflowArchiver.Init()
	}
	return nil
}

func (m *MergeArchiver) newBuffer() error {
	m.buffer = &bytes.Buffer{}
	blockWriter, err := m.blockWriterFactory.New(m.buffer)
	if err != nil {
		return fmt.Errorf("blockWriteFactory: %w", err)
	}
	m.blockWriter = blockWriter
	return nil
}

// WaitForAllFilesToUpload assumes that no more 'storeBlock' command is coming
func (m *MergeArchiver) WaitForAllFilesToUpload() {
	m.eg.Wait()
	if m.overflowArchiver != nil {
		m.overflowArchiver.WaitForAllFilesToUpload()
	}
}

func (m *MergeArchiver) storeBlock(block *bstream.Block) error {
	if m.buffer == nil && block.Num() < 3 {
		// Special case the beginning of the EOS chain

		if err := m.newBuffer(); err != nil {
			return err
		}
		m.expectBlock = block.Num()
	}

	if block.Num()%100 == 0 {
		if err := m.newBuffer(); err != nil {
			return err
		}

		if m.expectBlock == 0 {
			m.expectBlock = block.Num()
		}
	}

	if m.buffer == nil {
		m.zlogger.Info("ignore blocks before beginning of 100-blocks boundary", zap.Uint64("block_num", block.Num()))
		return nil
	}

	if m.stopBlock != 0 && block.Num() >= m.stopBlock {
		if m.overflowArchiver != nil {
			return m.overflowArchiver.storeBlock(block)
		}
		m.zlogger.Debug("ignoring block after stop_block because no passthrough is set", zap.Uint64("block_num", block.Num()))
		return nil
	}

	if m.expectBlock != block.Num() {
		return fmt.Errorf("blocks non contiguous, expectedBlock: %d, got block: %d", m.expectBlock, block.Num())
	}
	m.expectBlock++

	if err := m.blockWriter.Write(block); err != nil {
		return fmt.Errorf("blockWriter.Write: %w", err)
	}

	if block.Num()%100 == 99 {

		if m.eg.Stop() {
			return nil // already errored
		}

		baseNum := block.Num() - 99
		baseName := fmt.Sprintf("%010d", baseNum)
		buffer := m.buffer
		m.eg.Go(func() error {
			if baseNum%1000 == 0 {
				m.zlogger.Info("writing merged blocks log (%1000)", zap.String("base_name", baseName))
			}

			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
			defer cancel()

			err := m.store.WriteObject(ctx, baseName, bytes.NewReader(buffer.Bytes()))
			if err != nil {
				return fmt.Errorf("uploading merged file: %w", err)
			}
			return nil
		})
	}

	return nil
}

// uploadFiles does nothing here, it's managed by `storeBlock` when needed
func (m *MergeArchiver) uploadFiles() error {
	return nil
}
