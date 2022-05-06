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
	"strings"
	"time"

	"github.com/streamingfast/bstream"
	"github.com/streamingfast/merger/bundle"
	"github.com/streamingfast/shutter"
	"go.uber.org/zap"
)

type Archiver struct {
	*shutter.Shutter

	bundler *bundle.Bundler
	io      ArchiverIO

	currentlyMerging    bool
	firstBlockSeen      bool
	firstBoundaryTarget uint64

	mergeThresholdBlockAge time.Duration

	logger         *zap.Logger
	bundleSize     uint64
	oneblockSuffix string
}

func NewArchiver(
	bundleSize uint64,
	io ArchiverIO,
	oneblockSuffix string,
	mergeThresholdBlockAge time.Duration,
	logger *zap.Logger,
) *Archiver {
	a := &Archiver{
		Shutter:                shutter.New(),
		bundleSize:             bundleSize,
		io:                     io,
		oneblockSuffix:         oneblockSuffix,
		mergeThresholdBlockAge: mergeThresholdBlockAge,
		logger:                 logger,
		currentlyMerging:       true,
	}

	return a
}

func (a *Archiver) Start(ctx context.Context) {
	a.OnTerminating(func(err error) {
		a.logger.Info("archiver selector is terminating", zap.Error(err))
	})

	a.OnTerminated(func(err error) {
		a.logger.Info("archiver selector is terminated", zap.Error(err))
	})
}

func (a *Archiver) shouldMerge(block *bstream.Block) bool {
	// Be default currently merging is set to true
	if !a.currentlyMerging {
		return false
	}

	if a.mergeThresholdBlockAge == 0 {
		a.currentlyMerging = false
		return false
	}

	if a.mergeThresholdBlockAge == 1 {
		return true
	}

	blockAge := time.Since(block.Time())
	if blockAge > a.mergeThresholdBlockAge {
		return true
	}

	a.currentlyMerging = false
	return false
}

func (a *Archiver) sendBundleAsIndividualBlocks(ctx context.Context, oneBlockFiles []*bundle.OneBlockFile) error {
	for _, oneBlockFile := range oneBlockFiles {
		oneBlockBytes, err := a.io.DownloadOneBlockFile(context.TODO(), oneBlockFile)
		if err != nil {
			return fmt.Errorf("downloading one block file: %w", err)
		}

		blk, err := bstream.NewBlockFromBytes(oneBlockBytes)
		if err != nil {
			return fmt.Errorf("new block from bytes: %w", err)
		}

		err = a.io.StoreOneBlockFile(ctx, bundle.BlockFileNameWithSuffix(blk, a.oneblockSuffix), blk)
		if err != nil {
			return fmt.Errorf("storing one block file: %w", err)
		}
	}
	return nil
}

func validatePartialBlocks(ctx context.Context, partialBlocks []*bundle.OneBlockFile, block *bstream.Block, bundleSize uint64) error {
	if len(partialBlocks) == 0 {
		return nil
	}

	highest := partialBlocks[len(partialBlocks)-1]
	if !strings.HasSuffix(block.PreviousId, highest.ID) {
		return fmt.Errorf("highest mergeable block on disk #%d (%s) is not the parent of first seen block %s, expecting %s", highest.Num, highest.ID, block.String(), block.PreviousId)
	}

	lowest := partialBlocks[0]
	if lowBoundary(lowest.Num, bundleSize) != lowBoundary(block.Number, bundleSize) {
		return fmt.Errorf("lowest mergeable block is not part of the same bundle as first seen block")
	}

	return nil
}

func initializeBundlerFromFirstBlock(ctx context.Context, block *bstream.Block, io ArchiverIO, bundleSize uint64, logger *zap.Logger) (*bundle.Bundler, error) {

	partialBlocks, err := io.WalkMergeableOneBlockFiles(ctx)
	if err != nil {
		return nil, fmt.Errorf("walking mergeable one block files: %w", err)
	}

	err = validatePartialBlocks(ctx, partialBlocks, block, bundleSize)
	if err != nil {
		return nil, fmt.Errorf("validating partial blocks on disk: %w", err)
	}

	bundleLow := lowBoundary(block.Number, bundleSize)

	bundler := bundle.NewBundler(bundleLow, bstream.GetProtocolFirstStreamableBlock, bundleSize)
	if len(partialBlocks) != 0 {
		logger.Info("setting up bundler from partial files",
			zap.Uint64("low_boundary", bundleLow),
			zap.Uint64("block_number", block.Number),
			zap.Int("len_partial_blocks", len(partialBlocks)),
		)
		lowest := partialBlocks[0]
		if lowest.Num == bundleLow { //exception for FirstStreamableBlock not on boundary
			bundler.InitLIB(bstream.NewBlockRef(lowest.ID, lowest.Num))
		}
		for _, blk := range partialBlocks {
			bundler.AddOneBlockFile(blk)
		}
		return bundler, nil
	}

	if isBoundary(block.Number, bundleSize) {
		logger.Info("setting up bundler on a boundary block",
			zap.Uint64("low_boundary", bundleLow),
			zap.Uint64("block_number", block.Number),
		)
		if block.Number == bundleLow { //exception for FirstStreamableBlock not on boundary
			blkrefShortID := bstream.NewBlockRef(shortBlockID(block.Id), block.Number)
			logger.Debug("initializing lib",
				zap.Stringer("block", blkrefShortID),
			)
			bundler.InitLIB(blkrefShortID)
		}
		return bundler, nil
	}

	err = bundler.Bootstrap(func(lowBlockNum uint64) (oneBlockFiles []*bundle.OneBlockFile, err error) {
		oneBlockFiles, fetchErr := io.FetchMergedOneBlockFiles(lowBlockNum)
		if fetchErr != nil {
			return nil, fmt.Errorf("fetching one block files from merged file with low block num %d: %w", lowBlockNum, fetchErr)
		}
		return oneBlockFiles, err
	})
	if err != nil {
		logger.Debug("trying to bootstrap non-boundary block", zap.Stringer("block", block), zap.Error(err))
		return nil, nil
	}

	firstBlockNumFound, err := bundler.LongestChainFirstBlockNum()
	if err != nil {
		logger.Debug("bootstrap did not find a longest chain", zap.Error(err))
		return nil, nil
	}

	logger.Info("setting up bundler from a block that connects to previous bundles",
		zap.Uint64("low_boundary", bundleLow),
		zap.Uint64("block_number", block.Number),
		zap.Uint64("first_block_num_found", firstBlockNumFound),
	)
	return bundler, nil
}

func (a *Archiver) storeBlock(ctx context.Context, block *bstream.Block) error {
	if !a.firstBlockSeen {
		defer func() { a.firstBlockSeen = true }()
	}
	merging := a.shouldMerge(block)
	if !merging {
		if !a.firstBlockSeen || a.bundler != nil {
			err := a.io.SendMergeableAsOneBlockFiles(ctx)
			if err != nil {
				a.logger.Warn("cannot send stale mergeable blocks")
			}
		}
		a.bundler = nil

		return a.io.StoreOneBlockFile(ctx, bundle.BlockFileNameWithSuffix(block, a.oneblockSuffix), block)
	}

	if a.bundler == nil {
		if a.firstBoundaryTarget == 0 {
			bundler, err := initializeBundlerFromFirstBlock(ctx, block, a.io, a.bundleSize, a.logger)
			if err != nil {
				return fmt.Errorf("initializing bundler: %w", err)
			}
			if bundler == nil {
				a.firstBoundaryTarget = highBoundary(block.Number, a.bundleSize)
				a.logger.Debug("sending one-blocks directly until first boundary is met and we can start merging",
					zap.Stringer("block", block),
					zap.Uint64("first_boundary_target", a.firstBoundaryTarget),
				)
				return a.io.StoreOneBlockFile(ctx, bundle.BlockFileNameWithSuffix(block, a.oneblockSuffix), block)
			}

			a.bundler = bundler
		} else if block.Number < a.firstBoundaryTarget {
			a.logger.Debug("still waiting for first boundary before we create a bundle and start merging",
				zap.Stringer("block", block),
				zap.Uint64("first_boundary_target", a.firstBoundaryTarget),
			)
			return a.io.StoreOneBlockFile(ctx, bundle.BlockFileNameWithSuffix(block, a.oneblockSuffix), block)
		} else {
			bundleLow := lowBoundary(block.Number, a.bundleSize)
			a.bundler = bundle.NewBundler(bundleLow, bstream.GetProtocolFirstStreamableBlock, a.bundleSize)
			if block.Number == bundleLow { //exception for FirstStreamableBlock not on boundary
				blkrefShortID := bstream.NewBlockRef(shortBlockID(block.Id), block.Number)
				a.logger.Debug("initializing lib",
					zap.Stringer("block", blkrefShortID),
				)
				a.bundler.InitLIB(blkrefShortID)
			}
			err := a.io.StoreOneBlockFile(ctx, bundle.BlockFileNameWithSuffix(block, a.oneblockSuffix), block)
			if err != nil {
				return err
			}

		}

	}

	oneBlockFileName := bundle.BlockFileNameWithSuffix(block, a.oneblockSuffix)
	oneBlockFile := bundle.MustNewOneBlockFile(oneBlockFileName)
	err := a.io.StoreMergeableOneBlockFile(ctx, oneBlockFileName, block)
	if err != nil {
		return fmt.Errorf("storing one block to be merged: %w", err)
	}
	a.bundler.AddOneBlockFile(oneBlockFile)

	bundleCompleted, highestBlockLimit, err := a.bundler.BundleCompleted()
	if err != nil {
		return fmt.Errorf("heyhey error: %w", err)
	}
	if bundleCompleted {
		a.logger.Info("bundle completed, will merge and store it", zap.String("details", a.bundler.String()))
		oneBlockFiles := a.bundler.ToBundle(highestBlockLimit)

		err := a.io.MergeAndStore(a.bundler.BundleInclusiveLowerBlock(), oneBlockFiles)
		if err != nil {
			return fmt.Errorf("merging and saving merged block: %w", err)
		}

		a.bundler.Commit(highestBlockLimit)
		a.bundler.Purge(func(toDelete []*bundle.OneBlockFile) {
			a.io.Delete(toDelete)
		})
	}

	return nil
}

func (a *Archiver) StoreBlock(ctx context.Context, block *bstream.Block) error {
	return a.storeBlock(ctx, block)
}

func isBoundary(i, mod uint64) bool {
	return i%mod == 0 || i == bstream.GetProtocolFirstStreamableBlock
}

func lowBoundary(i uint64, mod uint64) uint64 {
	return i - (i % mod)
}
func highBoundary(i uint64, mod uint64) uint64 {
	return i - (i % mod) + mod
}
