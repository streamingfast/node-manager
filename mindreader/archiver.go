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

	"github.com/streamingfast/bstream"
	"github.com/streamingfast/merger/bundle"
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

	logger         *zap.Logger
	bundleSize     uint64
	oneblockSuffix string
}

func NewArchiver(
	bundleSize uint64,
	io ArchiverIO,
	batchMode bool,
	tracker *bstream.Tracker,
	oneblockSuffix string,
	mergeThresholdBlockAge time.Duration,
	logger *zap.Logger,
) *Archiver {
	a := &Archiver{
		Shutter:                shutter.New(),
		bundleSize:             bundleSize,
		io:                     io,
		batchMode:              batchMode,
		tracker:                tracker,
		oneblockSuffix:         oneblockSuffix,
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

func (a *Archiver) updateLastLIB(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	ref, err := a.tracker.Get(ctx, bstream.NetworkLIBTarget)
	if err != nil {
		return err
	}
	a.lastSeenLIB.Store(ref.Num())
	return nil
}

func (a *Archiver) launchLastLIBUpdater(ctx context.Context) {
	var err error
	sleepTime := 200 * time.Millisecond

	err = a.updateLastLIB(ctx)
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

			err = a.updateLastLIB(ctx)
			if err != nil {
				a.logger.Warn("failed getting last lib from blockmeta", zap.Error(err))
			}
			sleepTime = 30 * time.Second
		}
	}()
}

func (a *Archiver) shouldMerge(block *bstream.Block) bool {
	if a.batchMode {
		return true
	}

	//Be default currently merging is set to true
	if !a.currentlyMerging {
		return false
	}

	blockAge := time.Since(block.Time())
	if blockAge > a.mergeThresholdBlockAge {
		return true
	}

	lastSeenLIB := a.lastSeenLIB.Load()
	if block.Number+a.bundleSize <= lastSeenLIB {
		return true
	}

	return false
}

func (a *Archiver) loadLastPartial(ctx context.Context, block *bstream.Block) error {
	oneBlockFiles, err := a.io.WalkMergeableOneBlockFiles(ctx)
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
		a.logger.Info(
			"last partial block does not connect to the current block",
			zap.String("block_previous_id", block.PreviousID()),
			zap.String("last_partial_block_id", highestOneBlockFile.ID),
		)
		return nil
	}

	// load files into current bundle
	for _, oneBlockFile := range oneBlockFiles {
		a.bundler.AddOneBlockFile(oneBlockFile)
	}

	return nil
}
func (a *Archiver) storeBlock(ctx context.Context, oneBlockFile *bundle.OneBlockFile, block *bstream.Block) error {
	merging := a.shouldMerge(block)
	if !merging {
		if a.bundler != nil {
			// download all the one block files in the current incomplete bundle and store them
			// in the oneblockArchiver
			oneBlockFiles := a.bundler.ToBundle(math.MaxUint64)
			for _, oneBlockFile := range oneBlockFiles {
				oneBlockBytes, err := a.io.DownloadOneBlockFile(context.TODO(), oneBlockFile)
				if err != nil {
					return fmt.Errorf("downloading one block file: %w", err)
				}

				blk, err := bstream.NewBlockFromBytes(oneBlockBytes)
				if err != nil {
					return fmt.Errorf("new block from bytes: %w", err)
				}

				err = a.io.StoreOneBlockFile(ctx, bundle.BlockFileName(blk), blk)
				if err != nil {
					return fmt.Errorf("storing one block file: %w", err)
				}
			}
		}
		a.bundler = nil

		return a.io.StoreOneBlockFile(ctx, bundle.BlockFileName(block), block)
	}

	if a.bundler == nil {
		exclusiveHighestBlockLimit := ((block.Number / a.bundleSize) * a.bundleSize) + a.bundleSize
		a.bundler = bundle.NewBundler(a.bundleSize, exclusiveHighestBlockLimit)
		if err := a.loadLastPartial(ctx, block); err != nil {
			return fmt.Errorf("loading partial: %w", err)
		}
	}

	a.bundler.AddOneBlockFile(oneBlockFile)

	if block.Number < a.bundler.BundleInclusiveLowerBlock() {
		oneBlockFile.Merged = true
		//at this point it is certain that the bundle can not be completed
		return nil
	}

	err := a.io.StoreMergeableOneBlockFile(ctx, oneBlockFile.CanonicalName, block)
	if err != nil {
		return fmt.Errorf("storing one block to be merged: %w", err)
	}

	bundleCompleted, highestBlockLimit := a.bundler.BundleCompleted()
	if bundleCompleted {
		oneBlockFiles := a.bundler.ToBundle(highestBlockLimit)

		err := a.io.MergeAndStore(a.bundler.BundleInclusiveLowerBlock(), oneBlockFiles)
		if err != nil {
			return fmt.Errorf("merging and saving merged block: %w", err)
		}

		a.bundler.Commit(highestBlockLimit)
		a.bundler.Purge(func(oneBlockFilesToDelete []*bundle.OneBlockFile) {
			a.io.Delete(oneBlockFiles)
		})
	}

	return nil

}
func (a *Archiver) StoreBlock(ctx context.Context, block *bstream.Block) error {
	oneBlockFileName := bundle.BlockFileNameWithSuffix(block, a.oneblockSuffix)

	return a.storeBlock(ctx, bundle.MustNewOneBlockFile(oneBlockFileName), block)
}
