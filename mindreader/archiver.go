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
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/abourget/llerrgroup"
	"github.com/dfuse-io/bstream"
	"github.com/dfuse-io/dstore"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

type BlockMarshaller func(block *bstream.Block) ([]byte, error)

type Archiver interface {
	Init() error
	WaitForAllFilesToUpload() <-chan interface{}

	storeBlock(block *bstream.Block) error
	uploadFiles() error
}

type HybridArchiver struct {
	oneBlockStore      dstore.Store
	mergedBlockStore   dstore.Store
	mergeArchiver      *MergeArchiver
	blockWriterFactory bstream.BlockWriterFactory
	storeBlockCalled   bool

	mergeThresholdBlockAge       time.Duration
	lastSeenExistingMergedBlocks *atomic.Uint64
	currentlyMerging             bool

	uploadMutex sync.Mutex
	workDir     string
	zlogger     *zap.Logger
}

func NewHybridArchiver(
	oneBlockStore dstore.Store,
	mergedBlockStore dstore.Store,
	blockWriterFactory bstream.BlockWriterFactory,
	blockReaderFactory bstream.BlockReaderFactory,
	mergeThresholdBlockAge time.Duration,

	workDir string,
	zlogger *zap.Logger,
) *HybridArchiver {
	return &HybridArchiver{
		oneBlockStore:                oneBlockStore,
		mergedBlockStore:             mergedBlockStore,
		mergeArchiver:                NewMergeArchiver(mergedBlockStore, workDir, blockWriterFactory, blockReaderFactory, zlogger),
		blockWriterFactory:           blockWriterFactory,
		mergeThresholdBlockAge:       mergeThresholdBlockAge,
		lastSeenExistingMergedBlocks: atomic.NewUint64(0),

		workDir: workDir,
		zlogger: zlogger,
	}
}

func (s *HybridArchiver) storeBlock(block *bstream.Block) error {
	blockNum := block.Num()
	if !s.storeBlockCalled {
		baseBlockNum := blockNum / 100 * 100
		s.checkNextUploadedMergedBlock(baseBlockNum)
		s.checkNextUploadedMergedBlock(baseBlockNum + 100)

		if s.mergeArchiver.hasPartialMergedUpTo(blockNum) {
			s.zlogger.Info("continuing partially merged file from", zap.Uint64("block_num", blockNum))
			s.currentlyMerging = true
		}
		s.storeBlockCalled = true
	}

	if blockNum%100 == 0 || blockNum == bstream.GetProtocolFirstStreamableBlock {
		switch {
		case blockNum <= s.lastSeenExistingMergedBlocks.Load():
			s.zlogger.Info("merging next blocks directly because merged file exists in store", zap.Uint64("block_num", blockNum)) // todo add optimization to completely skip them based on overwrite param
			s.currentlyMerging = true
			go s.checkNextUploadedMergedBlock(blockNum/100*100 + 100)
		case time.Since(block.Time()) > s.mergeThresholdBlockAge:
			s.zlogger.Info("merging next blocks directly because they are older than threshold", zap.Uint64("block_num", blockNum), zap.Duration("block_age", time.Since(block.Time())))
			s.currentlyMerging = true
			go s.checkNextUploadedMergedBlock(blockNum/100*100 + 100)
		default:
			s.zlogger.Info("producing one-block files...", zap.Uint64("block_num", blockNum))
			s.currentlyMerging = false
		}
	}

	if s.currentlyMerging {
		return s.mergeArchiver.storeBlock(block)
	}

	fileName := blockFileName(block)

	// Store the actual file using multiple folders instead of a single one.
	// We assume 10 digits block number at start of file name. We take the first 7
	// ones and used them as the sub folder for the file.
	subDirectory := fileName[0:7]

	targetDir := filepath.Join(s.workDir, subDirectory)
	if _, err := os.Stat(targetDir); os.IsNotExist(err) {
		err := os.MkdirAll(targetDir, 0755)
		if err != nil {
			return fmt.Errorf("mkdir all: %w", err)
		}
	}

	tempFile := filepath.Join(targetDir, fileName+".dat.temp")
	finalFile := filepath.Join(targetDir, fileName+".dat")

	file, err := os.OpenFile(tempFile, os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("open file: %w", err)
	}

	blockWriter, err := s.blockWriterFactory.New(file)
	if err != nil {
		file.Close()
		return fmt.Errorf("write block factory: %w", err)
	}

	if err := blockWriter.Write(block); err != nil {
		file.Close()
		return fmt.Errorf("write block: %w", err)
	}

	if err := file.Close(); err != nil {
		return fmt.Errorf("close file: %w", err)
	}

	if err := os.Rename(tempFile, finalFile); err != nil {
		return fmt.Errorf("rename %q to %q: %w", tempFile, finalFile, err)
	}

	return nil
}

func (s *HybridArchiver) checkNextUploadedMergedBlock(nextBaseNum uint64) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	nextBaseName := fmt.Sprintf("%010d", nextBaseNum)
	exists, err := s.mergedBlockStore.FileExists(ctx, nextBaseName)
	if err != nil {
		s.zlogger.Warn("cannot check merged block existence in store, will retry...", zap.Error(err), zap.String("next_base_name", nextBaseName))
	}
	if exists {
		if nextBaseNum == 0 {
			s.lastSeenExistingMergedBlocks.Store(bstream.GetProtocolFirstStreamableBlock)
			return
		}
		s.lastSeenExistingMergedBlocks.Store(nextBaseNum)
	}
}

func (s *HybridArchiver) uploadFiles() error {
	s.uploadMutex.Lock()
	defer s.uploadMutex.Unlock()
	filesToUpload, err := findFilesToUpload(s.workDir, s.zlogger)
	if err != nil {
		return fmt.Errorf("unable to find files to upload: %w", err)
	}

	if len(filesToUpload) == 0 {
		return nil
	}

	eg := llerrgroup.New(20)
	for _, file := range filesToUpload {
		if eg.Stop() {
			break
		}

		file := file
		toBaseName := strings.TrimSuffix(filepath.Base(file), ".dat")

		eg.Go(func() error {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
			defer cancel()

			if traceEnabled {
				s.zlogger.Debug("uploading file to storage", zap.String("local_file", file), zap.String("remove_base", toBaseName))
			}

			if err = s.oneBlockStore.PushLocalFile(ctx, file, toBaseName); err != nil {
				return fmt.Errorf("moving file %q to storage: %w", file, err)
			}
			return nil
		})
	}

	return eg.Wait()
}

// WaitForAllFilesToUpload assumes that no more 'storeBlock' command is coming
func (s *HybridArchiver) WaitForAllFilesToUpload() <-chan interface{} {
	ch := make(chan interface{})
	go func() {
		s.uploadFiles()
		<-s.mergeArchiver.WaitForAllFilesToUpload()
		close(ch)
	}()
	return ch
}

func (s *HybridArchiver) Init() error {
	if err := os.MkdirAll(s.workDir, 0755); err != nil {
		return fmt.Errorf("mkdir work folder: %w", err)
	}

	return nil
}

func findFilesToUpload(workingDirectory string, zlogger *zap.Logger) (filesToUpload []string, err error) {
	err = filepath.Walk(workingDirectory, func(path string, info os.FileInfo, err error) error {
		if os.IsNotExist(err) {
			zlogger.Debug("skipping file that disappeared", zap.Error(err))
			return nil
		}
		if err != nil {
			return err
		}

		// clean up empty folders
		if info.IsDir() {
			if path == workingDirectory {
				return nil
			}
			// Prevents deleting folder that JUST got created and causing error on os.Open
			if isDirEmpty(path) && time.Since(info.ModTime()) > 60*time.Second {
				err := os.Remove(path)
				if err != nil {
					zlogger.Warn("cannot delete empty directory", zap.String("filename", path), zap.Error(err))
				}
			}
			return nil
		}

		// process .dat files
		if !strings.HasSuffix(path, ".dat") {
			return nil
		}
		filesToUpload = append(filesToUpload, path)

		return nil
	})

	return
}

func isDirEmpty(name string) bool {
	f, err := os.Open(name)
	if err != nil {
		return false
	}

	defer f.Close()
	_, err = f.Readdir(1)
	if err == io.EOF {
		return true
	}

	return false
}
