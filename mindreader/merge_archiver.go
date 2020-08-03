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
	"io"
	"os"
	"path/filepath"
	"strconv"
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
	blockReaderFactory bstream.BlockReaderFactory

	workDir     string
	eg          *llerrgroup.Group
	expectBlock uint64
	buffer      *bytes.Buffer
	blockWriter bstream.BlockWriter
	zlogger     *zap.Logger
}

func NewMergeArchiver(
	store dstore.Store,
	workDir string,
	blockWriterFactory bstream.BlockWriterFactory,
	blockReaderFactory bstream.BlockReaderFactory,
	zlogger *zap.Logger,
) *MergeArchiver {
	ra := &MergeArchiver{
		eg:                 llerrgroup.New(2),
		store:              store,
		workDir:            workDir,
		blockWriterFactory: blockWriterFactory,
		blockReaderFactory: blockReaderFactory,
		zlogger:            zlogger,
	}
	return ra
}

func (m *MergeArchiver) Init() error {
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

func (m *MergeArchiver) hasPartialMergedUpTo(next uint64) bool {
	if next%100 == 0 || next == bstream.GetProtocolFirstStreamableBlock {
		return false
	}

	matches, err := filepath.Glob(filepath.Join(m.workDir, "merge_archiver_*.partial"))
	if err != nil {
		m.zlogger.Error("trying to find glob for partial merged", zap.Error(err))
		return false
	}
	for _, match := range matches {
		saved := filepath.Base(match)
		if len(saved) != 33 {
			m.zlogger.Error("trying to restore partial merged but got invalid filename", zap.String("saved", saved), zap.Int("length", len(saved)))
			continue
		}
		savedNum, err := strconv.ParseUint(saved[15:25], 10, 64)
		if err != nil {
			m.zlogger.Error("trying to restore partial merged but got invalid number from filename", zap.String("saved", saved[15:25]), zap.Error(err))
			continue
		}
		if savedNum != next {
			m.zlogger.Info("last partial block file does not match saved, deleting file", zap.Uint64("next", next), zap.Uint64("saved_partial", savedNum))
			os.Remove(match)
			continue
		}

		f, err := os.Open(match)
		if err != nil {
			m.zlogger.Error("trying to restore partial merged but got cannot open file. deleting it", zap.String("filename", match), zap.Error(err))
			os.Remove(match)
			continue
		}

		blockReader, err := m.blockReaderFactory.New(f)
		if err != nil {
			m.zlogger.Error("trying to generate blockreader with file on restore", zap.Error(err))
			f.Close()
			return false
		}

		err = m.newBuffer()
		if err != nil {
			m.zlogger.Error("trying to set new buffer on restore", zap.Error(err))
			f.Close()
			return false
		}

		for {
			blk, err := blockReader.Read()
			if blk != nil {
				if err := m.blockWriter.Write(blk); err != nil {
					m.zlogger.Error("trying to write block on restore", zap.Error(err))
					m.buffer = nil
					m.blockWriter = nil
					f.Close()
					return false
				}
			}
			if err == io.EOF {
				break
			}
			if err != nil {
				m.zlogger.Error("trying to read block on restore. deleting file", zap.Error(err))
				m.buffer = nil
				m.blockWriter = nil
				f.Close()
				return false
			}
		}
		m.expectBlock = next
		f.Close()
		os.Remove(match)
		return true
	}

	return false
}

// WaitForAllFilesToUpload assumes that no more 'storeBlock' command is coming
func (m *MergeArchiver) WaitForAllFilesToUpload() <-chan interface{} {
	ch := make(chan interface{})
	go func() {
		m.eg.Wait()
		err := m.writePartialFile()
		if err != nil {
			m.zlogger.Error("writing partial file", zap.Error(err))
		}
		close(ch)
	}()
	return ch
}

func (m *MergeArchiver) uploadFiles() error {
	return nil
}
func (m *MergeArchiver) writePartialFile() error {
	filename := filepath.Join(m.workDir, fmt.Sprintf("merge_archiver_%010d.partial", m.expectBlock))

	f, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer f.Close()

	_, err = m.buffer.WriteTo(f)
	return err
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
