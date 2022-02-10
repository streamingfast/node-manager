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
	"github.com/streamingfast/merger/bundle"
	"math"
	"strings"
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

	io ArchiverIO

	currentlyMerging bool

	batchMode              bool // forces merging blocks without tracker or LIB checking
	tracker                *bstream.Tracker
	mergeThresholdBlockAge time.Duration
	lastSeenLIB            *atomic.Uint64

	workDir string
	logger  *zap.Logger
	running bool

	bundler *bundle.Bundler
}

func NewArchiverSelector(
	oneblockArchiver *OneBlockArchiver,
	mergeArchiver *MergeArchiver,
	io ArchiverIO,
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
		io:                     io,
		batchMode:              batchMode,
		tracker:                tracker,
		mergeThresholdBlockAge: mergeThresholdBlockAge,
		lastSeenLIB:            atomic.NewUint64(0),
		workDir:                workDir,
		logger:                 logger,
		currentlyMerging:       true,
	}

	libUpdaterCtx, libUpdaterCancel := context.WithCancel(context.Background())
	if !batchMode {
		a.launchLastLIBUpdater(libUpdaterCtx)
	}

	a.OnTerminating(func(err error) {
		a.logger.Info("archiver selector is terminating", zap.Error(err))
		if !a.mergeArchiver.IsTerminating() {
			a.mergeArchiver.Shutdown(err)
		}
		if !a.oneblockArchiver.IsTerminating() {
			a.oneblockArchiver.Shutdown(err)
		}
		libUpdaterCancel()
	})

	a.OnTerminated(func(err error) {
		a.logger.Info("archiver selector is terminated", zap.Error(err))
	})

	return a
}

func (s *ArchiverSelector) updateLastLIB(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	ref, err := s.tracker.Get(ctx, bstream.NetworkLIBTarget)
	if err != nil {
		return err
	}
	s.lastSeenLIB.Store(ref.Num())
	return nil
}

func (s *ArchiverSelector) launchLastLIBUpdater(ctx context.Context) {
	var err error
	sleepTime := 200 * time.Millisecond

	err = s.updateLastLIB(ctx)
	if err == nil { // don't warn on first error, maybe blockmeta is booting with us
		sleepTime = 30 * time.Second
	}

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-time.After(sleepTime):
				//
			}

			err = s.updateLastLIB(ctx)
			if err != nil {
				s.logger.Warn("failed getting last lib from blockmeta", zap.Error(err))
			}
			sleepTime = 30 * time.Second
		}
	}()
}

func (s *ArchiverSelector) shouldMerge(block *bstream.Block) bool {
	if s.batchMode {
		return true
	}

	//Be default currently merging is set to true
	if !s.currentlyMerging {
		return false
	}

	blockAge := time.Since(block.Time())
	if blockAge > s.mergeThresholdBlockAge {
		return true
	}

	lastSeenLIB := s.lastSeenLIB.Load()
	if block.Number+100 <= lastSeenLIB {
		return true
	}

	return false
}

func (s *ArchiverSelector) loadLastPartial(block *bstream.Block) {
	oneBlockFiles := s.io.WalkOneBlockFiles(context.TODO())

	var highestOneBlockFile *bundle.OneBlockFile
	for _, oneBlockFile := range oneBlockFiles {
		if highestOneBlockFile == nil {
			highestOneBlockFile = oneBlockFile
		}
		if oneBlockFile.Num > highestOneBlockFile.Num {
			highestOneBlockFile = oneBlockFile
		}
	}

	if highestOneBlockFile == nil {
		return
	}

	if !strings.HasSuffix(block.PreviousID(), highestOneBlockFile.ID) {
		s.logger.Info(
			"last partial block does not connect to the current block",
			zap.String("block_previous_id", block.PreviousID()),
			zap.String("last_partial_block_id", highestOneBlockFile.ID),
		)
		return
	}

	// load files into current bundle
	for _, oneBlockFile := range oneBlockFiles {
		s.bundler.AddOneBlockFile(oneBlockFile)
	}
}

func (s *ArchiverSelector) StoreBlock(block *bstream.Block) error {
	ctx := context.Background() //

	merging := s.shouldMerge(block)
	if !merging {
		if s.bundler != nil {
			// download all the one block files in the current incomplete bundle and store them
			// in the oneblockArchiver
			oneblocks := s.bundler.ToBundle(math.MaxUint64)
			for _, oneblock := range oneblocks {
				oneBlockBytes, err := s.io.DownloadOneBlockFile(context.TODO(), oneblock)
				if err != nil {
					return err
				}

				blk, err := bstream.NewBlockFromBytes(oneBlockBytes)
				if err != nil {
					return err
				}

				err = s.oneblockArchiver.StoreBlock(blk)
				if err != nil {
					return err
				}
			}
		}
		s.bundler = nil

		return s.oneblockArchiver.StoreBlock(block) // once we passed a boundary with oneblocks, we always send oneblocks
	}

	if s.bundler == nil {
		exclusiveHighestBlockLimit := ((block.Number / 100) * 100) + 100
		s.bundler = bundle.NewBundler(100, exclusiveHighestBlockLimit)
		s.loadLastPartial(block)
	}

	fileName := bundle.BlockFileName(block)
	oneBlockFile := bundle.MustNewOneBlockFile(fileName)
	s.bundler.AddOneBlockFile(oneBlockFile)

	if block.Number < s.bundler.BundleInclusiveLowerBlock() {
		oneBlockFile.Merged = true
		//at this point it is certain that the bundle can not be completed
		return nil
	}

	err := s.io.SaveOneBlockFile(ctx, fileName, block)
	if err != nil {
		return err
	}

	bundleCompleted, highestBlockLimit := s.bundler.BundleCompleted()
	if bundleCompleted {
		oneBlockFiles := s.bundler.ToBundle(highestBlockLimit)
		for _, oneBlockFile := range oneBlockFiles {
			data, err := s.io.DownloadOneBlockFile(ctx, oneBlockFile)
			if err != nil {
				return err
			}

			block, err := bstream.NewBlockFromBytes(data)
			if err != nil {
				return err
			}

			if err := s.mergeArchiver.StoreBlock(block); err != nil {
				return err
			}
		}

		err := s.mergeArchiver.Merge(s.bundler.BundleInclusiveLowerBlock())
		if err != nil {
			return err
		}

		s.bundler.Commit(highestBlockLimit)
		s.bundler.Purge(func(oneBlockFilesToDelete []*bundle.OneBlockFile) {
			_ = s.io.DeleteOneBlockFiles(context.Background(), oneBlockFiles)
		})

		if !s.shouldMerge(block) { //this could change once a bundle is completed
			s.currentlyMerging = false
			return s.oneblockArchiver.StoreBlock(block)
		}
	}

	return nil
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
