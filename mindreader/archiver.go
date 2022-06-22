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
	"time"

	"github.com/streamingfast/bstream"
	"github.com/streamingfast/logging"
	"github.com/streamingfast/merger"
	"github.com/streamingfast/shutter"
	"go.uber.org/zap"
)

type Archiver struct {
	*shutter.Shutter

	bundler *merger.Bundler
	io      ArchiverIO

	currentlyMerging bool
	firstBlockSeen   bool

	firstStreamableBlock uint64

	mergeThresholdBlockAge time.Duration

	bundleSize     uint64
	oneblockSuffix string

	logger *zap.Logger
	tracer logging.Tracer
}

func NewArchiver(
	firstStreamableBlock uint64,
	bundleSize uint64,
	io ArchiverIO,
	oneblockSuffix string,
	mergeThresholdBlockAge time.Duration,
	logger *zap.Logger,
	tracer logging.Tracer,
) *Archiver {
	a := &Archiver{
		Shutter:                shutter.New(),
		firstStreamableBlock:   firstStreamableBlock,
		bundleSize:             bundleSize,
		io:                     io,
		oneblockSuffix:         oneblockSuffix,
		mergeThresholdBlockAge: mergeThresholdBlockAge,
		currentlyMerging:       true,
		logger:                 logger,
		tracer:                 tracer,
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
		if a.tracer.Enabled() {
			a.logger.Debug("not merging on block because currently merging state is false", zap.Stringer("block", block))
		}

		return false
	}

	if a.mergeThresholdBlockAge == 0 {
		if a.tracer.Enabled() {
			a.logger.Debug("not merging on block because merge threshold block age is 0 (never)", zap.Stringer("block", block))
		}

		a.currentlyMerging = false
		return false
	}

	if a.mergeThresholdBlockAge == 1 {
		if a.tracer.Enabled() {
			a.logger.Debug("merging on block because merge threshold block age is 1 (always)", zap.Stringer("block", block))
		}

		return true
	}

	blockAge := time.Since(block.Time())
	if blockAge > a.mergeThresholdBlockAge {
		if a.tracer.Enabled() {
			a.logger.Debug("merging on block because merge threshold block age is > block age", zap.Stringer("block", block), zap.Duration("block_age", blockAge), zap.Duration("threshold", a.mergeThresholdBlockAge))
		}

		return true
	}

	if a.tracer.Enabled() {
		a.logger.Debug("not merging on block because no condition met", zap.Stringer("block", block))
	}

	a.currentlyMerging = false
	return false
}

func (a *Archiver) initializeBundlerFromFirstBlock(ctx context.Context, block *bstream.Block) (*merger.Bundler, error) {
	existingOneBlocks, err := a.io.WalkMergeableOneBlockFiles(ctx)
	if err != nil {
		return nil, fmt.Errorf("walking mergeable one block files: %w", err)
	}

	if len(existingOneBlocks) != 0 {
		if block.Number < existingOneBlocks[0].Num {
			return nil, fmt.Errorf("existing one-block-files from a previous run are above first received block: you must manually delete them (existing: %s, got %s)", existingOneBlocks[0], block.String())
		}

		linkable := false
		targetID := bstream.TruncateBlockID(block.PreviousId)
		for _, b := range existingOneBlocks {
			if b.ID == targetID {
				linkable = true
				break
			}
		}

		if !linkable {
			return nil, fmt.Errorf("received block cannot be linked against existing one-block-files from a previous run: you must manually delete them (got %s, not linkable to any of the %d existing files, between %s and %s)",
				block.String(),
				len(existingOneBlocks),
				existingOneBlocks[0],
				existingOneBlocks[len(existingOneBlocks)-1],
			)
		}

		bundleLowBoundary := lowBoundary(existingOneBlocks[0].Num, a.bundleSize)
		bundler := merger.NewBundler(bundleLowBoundary, a.bundleSize, a.io)
		a.logger.Info("setting up bundler from existing one-block-files",
			zap.Uint64("low_boundary", bundleLowBoundary),
			zap.Uint64("bundler_base_block_num", bundler.BaseBlockNum()),
			zap.Uint64("received_block_number", block.Number),
			zap.Int("len_partial_blocks", len(existingOneBlocks)),
		)

		return bundler, nil
	}

	if block.Number == a.firstStreamableBlock {
		bundleLowBoundary := lowBoundary(block.Number, a.bundleSize)
		lib := bstream.NewBlockRef(bstream.TruncateBlockID(block.Id), block.Number)

		bundler := merger.NewBundler(bundleLowBoundary, a.bundleSize, a.io)
		bundler.Reset(bundleLowBoundary, lib)

		a.logger.Info("setting up bundler from first_streamable_block",
			zap.Uint64("low_boundary", bundleLowBoundary),
			zap.Uint64("bundler_base_block_num", bundler.BaseBlockNum()),
			zap.Stringer("received_block", block),
			zap.Stringer("new_lib", lib),
		)
		return bundler, nil
	}

	if isBoundary(block.Number, a.bundleSize) {
		bundleLowBoundary := block.Number
		bundler := merger.NewBundler(bundleLowBoundary, a.bundleSize, a.io)
		a.logger.Info("setting up bundler on a boundary block",
			zap.Uint64("low_boundary", bundleLowBoundary),
			zap.Stringer("block", block),
		)
		return bundler, nil
	}

	nextBundleBoundary := nextBoundary(block.Number, a.bundleSize)
	bundler := merger.NewBundler(nextBundleBoundary, a.bundleSize, a.io)
	a.logger.Info("setting up bundler on the next boundary block",
		zap.Uint64("next_boundary", nextBundleBoundary),
		zap.Stringer("block", block),
	)
	return bundler, nil
}

func (a *Archiver) StoreBlock(ctx context.Context, block *bstream.Block) error {
	if block.Number < a.firstStreamableBlock {
		a.logger.Info("skipping block below first_streamable_block", zap.Stringer("block", block), zap.Uint64("first_streamable_block", a.firstStreamableBlock))
		return nil
	}

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
			a.bundler = nil
		}

		return a.io.StoreOneBlockFile(ctx, bstream.BlockFileNameWithSuffix(block, a.oneblockSuffix), block)
	}

	if a.bundler == nil {
		bundler, err := a.initializeBundlerFromFirstBlock(ctx, block)
		if err != nil {
			return fmt.Errorf("initializing bundler: %w", err)
		}
		a.bundler = bundler
	}

	oneBlockFileName := bstream.BlockFileNameWithSuffix(block, a.oneblockSuffix)
	oneBlockFile := bstream.MustNewOneBlockFile(oneBlockFileName)
	err := a.io.StoreMergeableOneBlockFile(ctx, oneBlockFileName, block)
	if err != nil {
		return fmt.Errorf("storing one block to be merged: %w", err)
	}

	prevBase := a.bundler.BaseBlockNum()
	if err := a.bundler.HandleBlockFile(oneBlockFile); err != nil {
		return err
	}

	if newBase := a.bundler.BaseBlockNum(); newBase != prevBase {
		return a.deleteMergedFilesBelow(ctx, newBase)
	}
	return nil
}

func (a *Archiver) deleteMergedFilesBelow(ctx context.Context, base uint64) error {
	existingOneBlocks, err := a.io.WalkMergeableOneBlockFiles(ctx)
	if err != nil {
		return fmt.Errorf("walking mergeable one block files to delete them: %w", err)
	}
	var toDelete []*bstream.OneBlockFile
	for _, ob := range existingOneBlocks {
		if ob.Num < base {
			toDelete = append(toDelete, ob)
		}
	}
	if len(toDelete) != 0 {
		a.io.DeleteAsync(toDelete)
	}
	return nil
}

func isBoundary(i, mod uint64) bool {
	return i%mod == 0
}

func lowBoundary(i uint64, mod uint64) uint64 {
	return i - (i % mod)
}
func nextBoundary(i uint64, mod uint64) uint64 {
	return i - (i % mod) + mod
}
