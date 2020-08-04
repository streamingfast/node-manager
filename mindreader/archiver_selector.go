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
	"io"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/dfuse-io/bstream"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

type ArchiverSelector struct {
	oneblockArchiver Archiver
	mergeArchiver    Archiver

	blockReaderFactory bstream.BlockReaderFactory

	firstBlockPassed    bool
	firstBoundaryPassed bool
	currentlyMerging    bool

	batchMode              bool // forces merging blocks without tracker or LIB checking
	tracker                *bstream.Tracker
	mergeThresholdBlockAge time.Duration
	lastSeenLIB            *atomic.Uint64

	workDir string
	logger  *zap.Logger
}

func NewArchiverSelector(
	oneblockArchiver Archiver,
	mergeArchiver Archiver,
	blockReaderFactory bstream.BlockReaderFactory,
	batchMode bool,
	tracker *bstream.Tracker,
	mergeThresholdBlockAge time.Duration,
	workDir string,
	logger *zap.Logger,
) *ArchiverSelector {
	s := &ArchiverSelector{
		oneblockArchiver:       oneblockArchiver,
		mergeArchiver:          mergeArchiver,
		blockReaderFactory:     blockReaderFactory,
		batchMode:              batchMode,
		tracker:                tracker,
		mergeThresholdBlockAge: mergeThresholdBlockAge,
		lastSeenLIB:            atomic.NewUint64(0),
		workDir:                workDir,
		logger:                 logger,
	}
	s.updateLastSeenLIB()
	return s
}

func (s *ArchiverSelector) updateLastSeenLIB() {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	ref, err := s.tracker.Get(ctx, bstream.NetworkLIBTarget)
	if err != nil {
		s.logger.Warn("cannot get lib from tracker", zap.Error(err))
		return
	}
	s.lastSeenLIB.Store(ref.Num())
}

// shouldMerge completely manages 'currentlyMerging', 'firstBoundaryPassed' and 'firstBlockPassed'
func (s *ArchiverSelector) shouldMerge(block *bstream.Block) (should bool, writeToBoth bool, previousBlocks []*bstream.Block) {
	if s.firstBoundaryPassed && !s.currentlyMerging {
		return false, false, nil // never go back to merging after passing a boundary
	}

	blockNum := block.Num()
	isBoundaryBlock := (blockNum%100 == 0 || blockNum == bstream.GetProtocolFirstStreamableBlock)
	if !isBoundaryBlock {
		if s.firstBlockPassed {
			return s.currentlyMerging, false, nil //only change merging state on boundaries or initial block
		}
		s.firstBlockPassed = true

		previousBlocks = s.loadLastPartial(blockNum)
		if previousBlocks == nil {
			return false, false, nil // never suggest 'merging' when we are not on a boundary and have no partial blocks
		}
	} else {
		s.firstBoundaryPassed = true
	}
	s.firstBlockPassed = true

	lastSeenLIB := s.lastSeenLIB.Load()
	if blockNum+10000 >= lastSeenLIB { // optimization: skip doing this every time if blockNum is "not even close"
		go s.updateLastSeenLIB() // for next time
	}

	blockAge := time.Since(block.Time())
	if blockAge > s.mergeThresholdBlockAge {
		s.logger.Info("merging next blocks directly because they are older than threshold", zap.Uint64("block_num", blockNum), zap.Duration("block_age", blockAge))
		writeToBoth = s.currentlyMerging == false
		s.currentlyMerging = true
		return true, writeToBoth, previousBlocks
	}

	if blockNum+100 <= lastSeenLIB {
		s.logger.Info("merging next blocks directly because they are older than LIB", zap.Uint64("block_num", blockNum), zap.Uint64("lib", lastSeenLIB))
		writeToBoth = s.currentlyMerging == false
		s.currentlyMerging = true
		return true, writeToBoth, previousBlocks
	}

	s.logger.Info("producing one-block files...", zap.Uint64("block_num", blockNum))
	s.currentlyMerging = false
	return false, false, previousBlocks
}

func (s *ArchiverSelector) loadLastPartial(next uint64) []*bstream.Block {
	matches, err := filepath.Glob(filepath.Join(s.workDir, "archiver_*.partial"))
	if err != nil {
		s.logger.Warn("trying to find glob for partial archive", zap.Error(err))
		return nil
	}
	for _, match := range matches {
		saved := filepath.Base(match)
		if len(saved) != 27 {
			s.logger.Error("trying to restore partial archive but got invalid filename", zap.String("saved", saved), zap.Int("length", len(saved)))
			continue
		}
		savedNum, err := strconv.ParseUint(saved[9:19], 10, 64)
		if err != nil {
			s.logger.Error("trying to restore partial archive but got invalid number from filename", zap.String("saved", saved[9:19]), zap.Error(err))
			continue
		}
		if savedNum != next {
			s.logger.Info("last partial block file does not match saved, deleting file", zap.Uint64("next", next), zap.Uint64("saved_partial", savedNum))
			os.Remove(match)
			continue
		}

		f, err := os.Open(match)
		if err != nil {
			s.logger.Error("trying to restore partial archive but got cannot open file. deleting it", zap.String("filename", match), zap.Error(err))
			os.Remove(match)
			continue
		}

		blockReader, err := s.blockReaderFactory.New(f)
		if err != nil {
			s.logger.Error("trying to generate blockreader with file on restore", zap.Error(err))
			f.Close()
			return nil
		}

		var blocks []*bstream.Block
		for {
			blk, err := blockReader.Read()
			if blk != nil {
				blocks = append(blocks, blk)
			}
			if err == io.EOF {
				break
			}
			if err != nil {
				s.logger.Error("trying to read block on partial archive restore caused an error. deleting file", zap.Error(err))
				os.Remove(match)
				f.Close()
				return nil
			}
		}
		f.Close()
		os.Remove(match)
		return blocks
	}

	return nil
}

func (s *ArchiverSelector) StoreBlock(block *bstream.Block) error {

	shouldMerge, writeToBoth, previousBlocks := s.shouldMerge(block)

	arch := s.oneblockArchiver
	if shouldMerge {
		arch = s.mergeArchiver
	}

	for _, blk := range previousBlocks {
		if err := arch.StoreBlock(blk); err != nil {
			return err
		}
	}
	if writeToBoth {
		if err := s.oneblockArchiver.StoreBlock(block); err != nil {
			return err
		}
	}
	return arch.StoreBlock(block)
}

func (s *ArchiverSelector) Start() {
	s.logger.Info("Starting OneBlock uploads")
	go s.oneblockArchiver.Start()
	s.logger.Info("Starting MergedBlocks uploads")
	go s.mergeArchiver.Start()
}

// Terminate assumes that no more 'StoreBlock' command is coming
func (s *ArchiverSelector) Terminate() <-chan interface{} {
	ch := make(chan interface{})
	go func() {
		<-s.mergeArchiver.Terminate()
		<-s.oneblockArchiver.Terminate()
		close(ch)
	}()
	return ch
}

func (s *ArchiverSelector) Init() error {
	if err := s.oneblockArchiver.Init(); err != nil {
		return err
	}
	return s.mergeArchiver.Init()
}
