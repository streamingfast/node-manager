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
	"go.uber.org/zap"
)

type BlockMarshaller func(block *bstream.Block) ([]byte, error)

type Archiver interface {
	Init() error
	WaitForAllFilesToUpload() <-chan interface{}

	storeBlock(block *bstream.Block) error
	uploadFiles() error
}

type OneblockArchiver struct {
	store              dstore.Store
	blockWriterFactory bstream.BlockWriterFactory
	workDir            string
	uploadMutex        sync.Mutex
	stopBlock          uint64
	zlogger            *zap.Logger
}

func NewOneblockArchiver(
	workDir string,
	store dstore.Store,
	blockWriterFactory bstream.BlockWriterFactory,
	stopBlock uint64,
	zlogger *zap.Logger,
) *OneblockArchiver {
	return &OneblockArchiver{
		store:              store,
		blockWriterFactory: blockWriterFactory,
		workDir:            workDir,
		stopBlock:          stopBlock,
		zlogger:            zlogger,
	}
}

// WaitForAllFilesToUpload assumes that no more 'storeBlock' command is coming
func (s *OneblockArchiver) WaitForAllFilesToUpload() <-chan interface{} {
	ch := make(chan interface{})
	go func() {
		s.uploadFiles()
		close(ch)
	}()
	return ch
}

func (s *OneblockArchiver) Init() error {
	if err := os.MkdirAll(s.workDir, 0755); err != nil {
		return fmt.Errorf("mkdir work folder: %w", err)
	}

	return nil
}

func (s *OneblockArchiver) storeBlock(block *bstream.Block) error {
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

func (s *OneblockArchiver) uploadFiles() error {
	s.uploadMutex.Lock()
	defer s.uploadMutex.Unlock()
	filesToUpload, err := s.findFilesToUpload(s.workDir)
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

			if err = s.store.PushLocalFile(ctx, file, toBaseName); err != nil {
				return fmt.Errorf("moving file %q to storage: %w", file, err)
			}
			return nil
		})
	}

	return eg.Wait()
}

func (s *OneblockArchiver) findFilesToUpload(workingDirectory string) (filesToUpload []string, err error) {
	err = filepath.Walk(workingDirectory, func(path string, info os.FileInfo, err error) error {
		if os.IsNotExist(err) {
			s.zlogger.Debug("skipping file that disappeared", zap.Error(err))
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
					s.zlogger.Warn("cannot delete empty directory", zap.String("filename", path), zap.Error(err))
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
