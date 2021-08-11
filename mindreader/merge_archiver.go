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
	"strings"
	"sync"
	"time"

	"github.com/abourget/llerrgroup"
	"github.com/streamingfast/bstream"
	"github.com/streamingfast/dstore"
	"go.uber.org/zap"
)

// MergeArchiver does the merging in one swift, no need for a merger here.
type MergeArchiver struct {
	store              dstore.Store
	blockWriterFactory bstream.BlockWriterFactory

	uploadMutex sync.Mutex
	workDir     string
	expectBlock uint64
	buffer      *bytes.Buffer
	blockWriter bstream.BlockWriter
	logger      *zap.Logger
}

func NewMergeArchiver(
	store dstore.Store,
	blockWriterFactory bstream.BlockWriterFactory,
	workDir string,
	logger *zap.Logger,
) *MergeArchiver {
	ra := &MergeArchiver{
		store:              store,
		workDir:            workDir,
		blockWriterFactory: blockWriterFactory,
		logger:             logger,
	}
	return ra
}

func (m *MergeArchiver) Init() error {
	return nil
}

func (m *MergeArchiver) Start() {
	lastUploadFailed := false
	for {
		err := m.uploadFiles()
		if err != nil {
			m.logger.Warn("temporary failure trying to upload mindreader merged block files, will retry", zap.Error(err))
			lastUploadFailed = true
		} else {
			if lastUploadFailed {
				m.logger.Warn("success uploading previously failed mindreader merged block files")
				lastUploadFailed = false
			}
		}

		select {
		case <-time.After(500 * time.Millisecond):
		}
	}
}

func (m *MergeArchiver) uploadFiles() error {
	m.uploadMutex.Lock()
	defer m.uploadMutex.Unlock()
	filesToUpload, err := findFilesToUpload(m.workDir, m.logger, ".merged")
	if err != nil {
		return fmt.Errorf("unable to find files to upload: %w", err)
	}

	if len(filesToUpload) == 0 {
		return nil
	}

	eg := llerrgroup.New(5)
	for _, file := range filesToUpload {
		if eg.Stop() {
			break
		}

		file := file
		toBaseName := strings.TrimSuffix(filepath.Base(file), ".merged")

		eg.Go(func() error {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
			defer cancel()

			if traceEnabled {
				m.logger.Debug("uploading file to storage", zap.String("local_file", file), zap.String("remove_base", toBaseName))
			}

			if err = m.store.PushLocalFile(ctx, file, toBaseName); err != nil {
				return fmt.Errorf("moving file %q to storage: %w", file, err)
			}
			return nil
		})
	}

	return eg.Wait()
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
	if m.buffer != nil {
		err := m.writePartialFile()
		if err != nil {
			m.logger.Error("writing partial file", zap.Error(err))
		}
	}
	go func() {
		err := m.uploadFiles()
		if err != nil {
			m.logger.Error("uploading remaining files", zap.Error(err))
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
		m.logger.Info("ignore blocks before beginning of 100-blocks boundary", zap.Uint64("block_num", block.Num()))
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
		baseNum := block.Num() - 99
		baseName := fmt.Sprintf("%010d", baseNum)
		if baseNum%1000 == 0 {
			m.logger.Info("writing merged blocks log (%1000)", zap.String("base_name", baseName))
		}

		tempFile := filepath.Join(m.workDir, baseName+".merged.temp")
		finalFile := filepath.Join(m.workDir, baseName+".merged")

		file, err := os.OpenFile(tempFile, os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			return fmt.Errorf("open file: %w", err)
		}

		m.buffer.WriteTo(file)
		if err := os.Rename(tempFile, finalFile); err != nil {
			return fmt.Errorf("rename %q to %q: %w", tempFile, finalFile, err)
		}

	}

	return nil
}
