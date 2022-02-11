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
	"math"
	"strings"
	"time"

	"github.com/streamingfast/merger/bundle"

	"github.com/streamingfast/bstream"
	"github.com/streamingfast/shutter"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

type Archiver struct {
	*shutter.Shutter

	bundler *bundle.Bundler
	io      ArchiverIO

	currentlyMerging bool

	batchMode              bool // forces merging blocks without tracker or LIB checking
	tracker                *bstream.Tracker
	mergeThresholdBlockAge time.Duration
	lastSeenLIB            *atomic.Uint64

	logger     *zap.Logger
	bundleSize uint64
}

func NewArchiver(
	bundleSize uint64,
	io ArchiverIO,
	batchMode bool,
	tracker *bstream.Tracker,
	mergeThresholdBlockAge time.Duration,
	logger *zap.Logger,
) *Archiver {
	a := &Archiver{
		Shutter:                shutter.New(),
		bundleSize:             bundleSize,
		io:                     io,
		batchMode:              batchMode,
		tracker:                tracker,
		mergeThresholdBlockAge: mergeThresholdBlockAge,
		lastSeenLIB:            atomic.NewUint64(0),
		logger:                 logger,
		currentlyMerging:       true,
	}

	return a
}

func (a *Archiver) Start(ctx context.Context) {
	libUpdaterCtx, libUpdaterCancel := context.WithCancel(ctx)
	if !a.batchMode {
		a.launchLastLIBUpdater(libUpdaterCtx)
	}

	a.OnTerminating(func(err error) {
		a.logger.Info("archiver selector is terminating", zap.Error(err))
		libUpdaterCancel()
	})

	a.OnTerminated(func(err error) {
		a.logger.Info("archiver selector is terminated", zap.Error(err))
	})
}

func (s *Archiver) updateLastLIB(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	ref, err := s.tracker.Get(ctx, bstream.NetworkLIBTarget)
	if err != nil {
		return err
	}
	s.lastSeenLIB.Store(ref.Num())
	return nil
}

func (s *Archiver) launchLastLIBUpdater(ctx context.Context) {
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

func (s *Archiver) shouldMerge(block *bstream.Block) bool {
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
	if block.Number+s.bundleSize <= lastSeenLIB {
		return true
	}

	return false
}

func (s *Archiver) loadLastPartial(block *bstream.Block) error {
	oneBlockFiles, err := s.io.WalkMergeableOneBlockFiles(context.TODO())
	if err != nil {
		return fmt.Errorf("walking mergeable one block files: %w", err)
	}
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
		return nil
	}

	if !strings.HasSuffix(block.PreviousID(), highestOneBlockFile.ID) {
		s.logger.Info(
			"last partial block does not connect to the current block",
			zap.String("block_previous_id", block.PreviousID()),
			zap.String("last_partial_block_id", highestOneBlockFile.ID),
		)
		return nil
	}

	// load files into current bundle
	for _, oneBlockFile := range oneBlockFiles {
		s.bundler.AddOneBlockFile(oneBlockFile)
	}

	return nil
}
func (s *Archiver) storeBlock(ctx context.Context, oneBlockFile *bundle.OneBlockFile, block *bstream.Block) error {
	merging := s.shouldMerge(block)
	if !merging {
		if s.bundler != nil {
			// download all the one block files in the current incomplete bundle and store them
			// in the oneblockArchiver
			oneBlockFiles := s.bundler.ToBundle(math.MaxUint64)
			for _, oneBlockFile := range oneBlockFiles {
				oneBlockBytes, err := s.io.DownloadOneBlockFile(context.TODO(), oneBlockFile)
				if err != nil {
					return err
				}

				blk, err := bstream.NewBlockFromBytes(oneBlockBytes)
				if err != nil {
					return err
				}

				err = s.io.StoreOneBlockFile(ctx, oneBlockFile.CanonicalName, blk)
				if err != nil {
					return err
				}
			}
		}
		s.bundler = nil

		return s.io.StoreOneBlockFile(ctx, oneBlockFile.CanonicalName, block)
	}

	if s.bundler == nil {
		exclusiveHighestBlockLimit := ((block.Number / s.bundleSize) * s.bundleSize) + s.bundleSize
		s.bundler = bundle.NewBundler(s.bundleSize, exclusiveHighestBlockLimit)
		if err := s.loadLastPartial(block); err != nil {
			return fmt.Errorf("loading partial: %w", err)
		}
	}

	s.bundler.AddOneBlockFile(oneBlockFile)

	if block.Number < s.bundler.BundleInclusiveLowerBlock() {
		oneBlockFile.Merged = true
		//at this point it is certain that the bundle can not be completed
		return nil
	}

	err := s.io.StoreMergeableOneBlockFile(ctx, oneBlockFile.CanonicalName, block)
	if err != nil {
		return err
	}

	bundleCompleted, highestBlockLimit := s.bundler.BundleCompleted()
	if bundleCompleted {
		oneBlockFiles := s.bundler.ToBundle(highestBlockLimit)

		err := s.io.MergeAndStore(s.bundler.BundleInclusiveLowerBlock(), oneBlockFiles)
		if err != nil {
			return fmt.Errorf("merging and saving merged block: %w", err)
		}

		s.bundler.Commit(highestBlockLimit)
		s.bundler.Purge(func(oneBlockFilesToDelete []*bundle.OneBlockFile) {
			_ = s.io.DeleteOneBlockFiles(context.Background(), oneBlockFiles)
		})

		if !s.shouldMerge(block) { //this could change once a bundle is completed
			s.currentlyMerging = false
			return s.io.StoreOneBlockFile(ctx, oneBlockFile.CanonicalName, block)
		}
	}

	return nil

}
func (s *Archiver) StoreBlock(ctx context.Context, block *bstream.Block) error {
	oneBlockFileName := bundle.BlockFileName(block)

	return s.storeBlock(ctx, bundle.MustNewOneBlockFile(oneBlockFileName), block)
}
