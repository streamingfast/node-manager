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
	"github.com/streamingfast/shutter"
	"go.uber.org/zap"
)

// MergeArchiver does the merging in one swift, no need for a merger here.
type MergeArchiver struct {
	*shutter.Shutter
	store              dstore.Store
	blockWriterFactory bstream.BlockWriterFactory

	uploadMutex  sync.Mutex
	workDir      string
	currentBlock uint64
	buffer       *bytes.Buffer
	blockWriter  bstream.BlockWriter
	logger       *zap.Logger
	running      bool
}

//bundler
//merger.IOInterface
func NewMergeArchiver(
	store dstore.Store,
	blockWriterFactory bstream.BlockWriterFactory,
	workDir string,
	logger *zap.Logger,
) *MergeArchiver {
	a := &MergeArchiver{
		Shutter:            shutter.New(),
		store:              store,
		workDir:            workDir,
		blockWriterFactory: blockWriterFactory,
		logger:             logger,
	}
	a.newBuffer()

	a.OnTerminating(func(err error) {
		a.logger.Info("merger archiver is terminating", zap.Error(err))
		e := a.uploadFiles()
		if e != nil {
			logger.Error("terminating: uploading file", zap.Error(e))
		}
	})

	a.OnTerminated(func(err error) {
		a.logger.Info("merger archiver is terminated", zap.Error(err))
	})

	return a
}

func (a *MergeArchiver) Start() {
	if a.running {
		return
	}
	a.running = true

	lastUploadFailed := false
	for {
		err := a.uploadFiles()
		if err != nil {
			a.logger.Warn("temporary failure trying to upload mindreader merged block files, will retry", zap.Error(err))
			lastUploadFailed = true
		} else {
			if lastUploadFailed {
				a.logger.Warn("success uploading previously failed mindreader merged block files")
				lastUploadFailed = false
			}
		}

		select {
		case <-a.Terminating():
			a.logger.Info("terminating upload loop")
			return
		case <-time.After(500 * time.Millisecond):
		}
	}
}

func (a *MergeArchiver) uploadFiles() error {
	a.uploadMutex.Lock()
	defer a.uploadMutex.Unlock()
	filesToUpload, err := findFilesToUpload(a.workDir, a.logger, ".merged")
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
				a.logger.Debug("uploading file to storage", zap.String("local_file", file), zap.String("remove_base", toBaseName))
			}

			if err = a.store.PushLocalFile(ctx, file, toBaseName); err != nil {
				return fmt.Errorf("moving file %q to storage: %w", file, err)
			}
			return nil
		})
	}

	return eg.Wait()
}

func (a *MergeArchiver) newBuffer() {
	a.buffer = &bytes.Buffer{}
	blockWriter, err := a.blockWriterFactory.New(a.buffer)
	if err != nil {
		panic(err) // this should never fail
	}
	a.blockWriter = blockWriter
}

//func (a *MergeArchiver) writePartialFile(lastBlock uint64) error {
//	filename := filepath.Join(a.workDir, fmt.Sprintf("archiver_%010d.partial", lastBlock))
//
//	f, err := os.Create(filename)
//	if err != nil {
//		return err
//	}
//	defer f.Close()
//
//	_, err = a.buffer.WriteTo(f)
//	return err
//}

func (a *MergeArchiver) StoreBlock(block *bstream.Block) error {
	if err := a.blockWriter.Write(block); err != nil {
		return fmt.Errorf("blockWriter.Write: %w", err)
	}

	return nil
}

func (a *MergeArchiver) Merge(baseNum uint64) error {
	baseName := fmt.Sprintf("%010d", baseNum)
	if baseNum%1000 == 0 {
		a.logger.Info("writing merged blocks log (%1000)", zap.String("base_name", baseName))
	}

	tempFile := filepath.Join(a.workDir, baseName+".merged.temp")
	finalFile := filepath.Join(a.workDir, baseName+".merged")

	file, err := os.OpenFile(tempFile, os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("open file: %w", err)
	}

	if _, err := a.buffer.WriteTo(file); err != nil {
		return fmt.Errorf("writing to file %q: %w", tempFile, err)
	}

	if err := os.Rename(tempFile, finalFile); err != nil {
		return fmt.Errorf("rename %q to %q: %w", tempFile, finalFile, err)
	}

	a.newBuffer()
	return nil
}
