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

	"go.uber.org/zap"

	"github.com/abourget/llerrgroup"
	"github.com/dfuse-io/bstream"
	"github.com/dfuse-io/dstore"
)

type BlockMarshaller func(block *bstream.Block) ([]byte, error)

type BlockFileNamer func(block *bstream.Block) string

type Archiver interface {
	init() error
	storeBlock(block *bstream.Block) error
	uploadFiles() error
	cleanup()
}
type OneblockArchiverOption func(*OneblockArchiver)

func WithDiscardFromStopBlock(stopBlock uint64) OneblockArchiverOption {
	return func(a *OneblockArchiver) {
		a.stopBlock = stopBlock
	}
}

type OneblockArchiver struct {
	store              dstore.Store
	blockFileNamer     BlockFileNamer
	blockWriterFactory bstream.BlockWriterFactory
	workDir            string
	uploadMutex        sync.Mutex
	stopBlock          uint64
}

func NewOneblockArchiver(
	workDir string,
	store dstore.Store,
	blockFileNamer BlockFileNamer,
	blockWriterFactory bstream.BlockWriterFactory,
	stopBlock uint64,
	options ...OneblockArchiverOption,
) *OneblockArchiver {
	da := &OneblockArchiver{
		store:              store,
		blockFileNamer:     blockFileNamer,
		blockWriterFactory: blockWriterFactory,
		workDir:            workDir,
		stopBlock:          stopBlock,
	}
	for _, opt := range options {
		opt(da)
	}
	return da
}

// cleanup assumes that no more 'storeBlock' command is coming
func (s *OneblockArchiver) cleanup() {
	s.uploadFiles()
}

func (s *OneblockArchiver) init() error {
	if err := os.MkdirAll(s.workDir, 0755); err != nil {
		return fmt.Errorf("mkdir work folder: %s", err)
	}

	return nil
}

func (s *OneblockArchiver) storeBlock(block *bstream.Block) error {
	fileName := s.blockFileNamer(block)

	// Store the actual file using multiple folders instead of a single one.
	// We assume 10 digits block number at start of file name. We take the first 7
	// ones and used them as the sub folder for the file.
	subDirectory := fileName[0:7]

	targetDir := filepath.Join(s.workDir, subDirectory)
	if _, err := os.Stat(targetDir); os.IsNotExist(err) {
		err := os.MkdirAll(targetDir, 0755)
		if err != nil {
			return fmt.Errorf("mkdir all: %s", err)
		}
	}

	tempFile := filepath.Join(targetDir, fileName+".dat.temp")
	finalFile := filepath.Join(targetDir, fileName+".dat")

	file, err := os.OpenFile(tempFile, os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("open file: %s", err)
	}

	blockWriter, err := s.blockWriterFactory.New(file)
	if err != nil {
		file.Close()
		return fmt.Errorf("blockWriteFactory: %s", err)
	}

	if err := blockWriter.Write(block); err != nil {
		file.Close()
		return fmt.Errorf("blockWriter.Write: %s", err)
	}

	if err := file.Close(); err != nil {
		return fmt.Errorf("close file: %s", err)
	}

	if err := os.Rename(tempFile, finalFile); err != nil {
		return fmt.Errorf("rename %s to %s: %s", tempFile, finalFile, err)
	}

	return nil
}

func (s *OneblockArchiver) uploadFiles() error {
	s.uploadMutex.Lock()
	defer s.uploadMutex.Unlock()
	filesToUpload, err := findFilesToUpload(s.workDir)
	if err != nil {
		return fmt.Errorf("unable to find files to upload: %s", err)
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
			zlog.Debug("about to move file", zap.String("file", file))
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
			defer cancel()

			if err = s.store.PushLocalFile(ctx, file, toBaseName); err != nil {
				return fmt.Errorf("moving file %q to storage: %s", file, err)
			}
			return nil
		})
	}

	return eg.Wait()
}

func findFilesToUpload(workingDirectory string) (filesToUpload []string, err error) {
	err = filepath.Walk(workingDirectory, func(path string, info os.FileInfo, err error) error {
		if os.IsNotExist(err) {
			zlog.Debug("filesToUpload skipping file that disappeared", zap.Error(err))
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
					zlog.Warn("cannot delete empty directory", zap.String("filename", path), zap.Error(err))
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
