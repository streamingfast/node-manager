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

	"github.com/streamingfast/bstream"
	"github.com/streamingfast/shutter"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

type Archiver interface {
	Shutdown(error)              // shutter
	Start()                      // shutter
	Terminated() <-chan struct{} //shutter

	StoreBlock(*bstream.Block) error
}

type ArchiverSelector struct {
	*shutter.Shutter
	oneblockArchiver *OneBlockArchiver
	mergeArchiver    *MergeArchiver

	blockReaderFactory bstream.BlockReaderFactory

	firstBlockPassed    bool
	firstBoundaryPassed bool
	currentlyMerging    bool
	nextBoundary        uint64

	batchMode              bool // forces merging blocks without tracker or LIB checking
	tracker                *bstream.Tracker
	mergeThresholdBlockAge time.Duration
	lastSeenLIB            *atomic.Uint64

	workDir string
	logger  *zap.Logger
	running bool
}

func NewArchiverSelector(
	oneblockArchiver *OneBlockArchiver,
	mergeArchiver *MergeArchiver,
	blockReaderFactory bstream.BlockReaderFactory,
	batchMode bool,
	tracker *bstream.Tracker,
	mergeThresholdBlockAge time.Duration,
	workDir string,
	logger *zap.Logger,
) *ArchiverSelector {
	a := &ArchiverSelector{
		Shutter:                shutter.New(),
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
	if !batchMode {
		a.launchLastLIBUpdater()
	}

	a.OnTerminating(func(err error) {
		a.logger.Info("archiver selector is terminating", zap.Error(err))
		if !a.mergeArchiver.IsTerminating() {
			a.mergeArchiver.Shutdown(err)
		}
		if !a.oneblockArchiver.IsTerminating() {
			a.oneblockArchiver.Shutdown(err)
		}
	})

	a.OnTerminated(func(err error) {
		a.logger.Info("archiver selector is terminated", zap.Error(err))
	})

	return a
}

func (s *ArchiverSelector) updateLastLIB() error {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	ref, err := s.tracker.Get(ctx, bstream.NetworkLIBTarget)
	if err != nil {
		return err
	}
	s.lastSeenLIB.Store(ref.Num())
	return nil
}

func (s *ArchiverSelector) launchLastLIBUpdater() {
	var fetchedOnce bool
	var warned bool
	var err error
	sleepTime := 200 * time.Millisecond

	err = s.updateLastLIB()
	if err == nil { // don't warn on first error, maybe blockmeta is booting with us
		fetchedOnce = true
		sleepTime = 30 * time.Second
	}

	go func() {
		for {
			time.Sleep(sleepTime)

			if s.firstBoundaryPassed {
				if !s.currentlyMerging { // we will never 'go back' to merging
					if !fetchedOnce {
						s.logger.Warn("could not get LIB from blockmeta after a few attempts. Not merging blocks", zap.Error(err))
					}
					return
				}
				sleepTime = 30 * time.Second

				if !fetchedOnce && !warned {
					s.logger.Warn("cannot get LIB from blockmeta, merging blocks based on their blocktime only (will retry)", zap.Duration("merge_threshold_block_age", s.mergeThresholdBlockAge))
					warned = true
				}
			}

			err = s.updateLastLIB()
			if err == nil {
				if !fetchedOnce {
					fetchedOnce = true
					sleepTime = 30 * time.Second
					if warned {
						s.logger.Warn("success connecting to blockmeta after previous failures")
					}
				}
			}
		}
	}()
}

func (s *ArchiverSelector) mergeable(block *bstream.Block) bool {
	if s.batchMode {
		return true
	}

	blockNum := block.Num()
	lastSeenLIB := s.lastSeenLIB.Load()

	blockAge := time.Since(block.Time())
	if blockAge > s.mergeThresholdBlockAge {
		return true
	}

	if blockNum+100 <= lastSeenLIB {
		return true
	}

	return false
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

func (s *ArchiverSelector) atBoundary(block *bstream.Block) bool {
	blockNum := block.Num()

	if s.nextBoundary == 0 { //first block
		s.nextBoundary = ((blockNum / 100) * 100) + 100
		if blockNum == bstream.GetProtocolFirstStreamableBlock {
			return true
		}
		if blockNum%100 == 0 {
			return true
		}
		return false
	}

	if blockNum >= s.nextBoundary {
		s.nextBoundary += 100
		return true
	}

	return false
}

func (s *ArchiverSelector) StoreBlock(block *bstream.Block) error {

	if !s.currentlyMerging && s.firstBoundaryPassed {
		return s.oneblockArchiver.StoreBlock(block) // once we passed a boundary with oneblocks, we always send oneblocks
	}

	isBoundaryBlock := s.atBoundary(block)
	if isBoundaryBlock && !s.firstBoundaryPassed {
		defer func() { s.firstBoundaryPassed = true }()
	}

	isFirstBlock := !s.firstBlockPassed
	if isFirstBlock {
		s.firstBlockPassed = true
		if s.mergeable(block) {
			if isBoundaryBlock {
				s.logger.Info("merging blocks on first boundary", zap.Stringer("block", block))
				s.currentlyMerging = true
				return s.mergeArchiver.StoreBlock(block)
			}
			if previousBlocks := s.loadLastPartial(block.Number); previousBlocks != nil {
				s.logger.Info("merging on first block using loaded previous partial blocks", zap.Stringer("block", block))
				s.currentlyMerging = true
				for _, blk := range previousBlocks {
					if err := s.mergeArchiver.StoreBlock(blk); err != nil {
						return err
					}
				}
				return s.mergeArchiver.StoreBlock(block)
			}
			s.logger.Info("cannot ensure complete merged block bundle, not merging until next boundary", zap.Stringer("block", block))
		}
	}

	if isBoundaryBlock {
		if s.currentlyMerging {
			if err := s.mergeArchiver.Merge(block.Number/100*100 - 100); err != nil { // merge previous bundle
				return err
			}

			if s.mergeable(block) {
				return s.mergeArchiver.StoreBlock(block)
			}

			s.logger.Info("switching to one-blocks", zap.Stringer("block", block))
			s.currentlyMerging = false
			return s.oneblockArchiver.StoreBlock(block)
		}

		if s.mergeable(block) {
			s.logger.Info("switching to merged-blocks (sending this block to both archivers)", zap.Stringer("block", block))
			s.currentlyMerging = true
			if err := s.oneblockArchiver.StoreBlock(block); err != nil {
				return err
			}
			if err := s.mergeArchiver.StoreBlock(block); err != nil {
				return err
			}
			return nil
		}

	}

	if s.currentlyMerging {
		return s.mergeArchiver.StoreBlock(block)
	}
	return s.oneblockArchiver.StoreBlock(block)
}

func (s *ArchiverSelector) Start() {
	if s.running {
		return
	}
	s.running = true

	s.logger.Info("starting one block(s) uploads")
	go s.oneblockArchiver.Start()
	s.logger.Info("starting merged blocks(s) uploads")
	go s.mergeArchiver.Start()

}

func (s *ArchiverSelector) Init() error {
	return s.oneblockArchiver.Init()
}

/*
	if a.buffer != nil {
		err := a.writePartialFile(lastBlock)
		if err != nil {
			a.logger.Error("writing partial file", zap.Error(err))
		}
	}

*/

/*
	if block.Num() < a.currentBlock {
		return fmt.Errorf("merge archiver does not support replaying forks (resending block %d which is less than current block %d", block.Num(), a.currentBlock)
	}

	if a.nextExclusiveHighestBlockLimit == 0 {
		highestBlockLimit := ((block.Num() / 100) * 100) + 200 //skipping first bundle because it will be incomplete
		a.skipBlockUpTo = highestBlockLimit - 100
		if block.Num() == bstream.GetProtocolFirstStreamableBlock {
			highestBlockLimit -= 100 //because this the first streamable block it is ok to produce and incomplete bundle.
			a.skipBlockUpTo = bstream.GetProtocolFirstStreamableBlock
		}
		a.nextExclusiveHighestBlockLimit = highestBlockLimit
	}

	if block.Num() < a.skipBlockUpTo { //skipping block of the first incomplete bundle
		return nil
	}
	a.currentBlock = block.Num()

	if block.Num() >= a.nextExclusiveHighestBlockLimit {

		baseNum := a.nextExclusiveHighestBlockLimit - 100
//MERGE(baseNum)

		//resetting
		a.nextExclusiveHighestBlockLimit += 100
	}


*/

//nextExclusiveHighestBlockLimit uint64
//skipBlockUpTo                  uint64
