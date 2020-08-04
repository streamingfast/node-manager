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
	"os"
	"path/filepath"
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

	workDir     string
	eg          *llerrgroup.Group
	expectBlock uint64
	buffer      *bytes.Buffer
	blockWriter bstream.BlockWriter
	zlogger     *zap.Logger
}

func NewMergeArchiver(
	store dstore.Store,
	blockWriterFactory bstream.BlockWriterFactory,
	workDir string,
	zlogger *zap.Logger,
) *MergeArchiver {
	ra := &MergeArchiver{
		eg:                 llerrgroup.New(2),
		store:              store,
		workDir:            workDir,
		blockWriterFactory: blockWriterFactory,
		zlogger:            zlogger,
	}
	return ra
}

func (m *MergeArchiver) Init() error {
	return nil
}

func (m *MergeArchiver) Start() {
	return
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

// Terminate assumes that no more 'StoreBlock' command is coming
func (m *MergeArchiver) Terminate() <-chan interface{} {
	ch := make(chan interface{})
	go func() {
		m.eg.Wait()
		if m.buffer != nil {
			err := m.writePartialFile()
			if err != nil {
				m.zlogger.Error("writing partial file", zap.Error(err))
			}
		}
		close(ch)
	}()
	return ch
}

func (m *MergeArchiver) writePartialFile() error {
	filename := filepath.Join(m.workDir, fmt.Sprintf("archiver_%010d.partial", m.expectBlock))

	f, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer f.Close()

	_, err = m.buffer.WriteTo(f)
	return err
}

func (m *MergeArchiver) StoreBlock(block *bstream.Block) error {
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

		if m.expectBlock%100 == 0 { // relaxing enforcement here, 299 could be followed by 400 if the blocks 300->399 were sent to another archiver
			m.expectBlock = block.Num()
		}
	}

	if m.buffer == nil {
		m.zlogger.Info("ignore blocks before beginning of 100-blocks boundary", zap.Uint64("block_num", block.Num()))
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
