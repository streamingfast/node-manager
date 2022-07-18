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
	"bytes"
	"context"
	"fmt"

	"github.com/streamingfast/bstream"
	"github.com/streamingfast/dstore"
	"github.com/streamingfast/logging"
	"github.com/streamingfast/shutter"
	"go.uber.org/zap"
)

type Archiver struct {
	*shutter.Shutter

	startBlock     uint64
	oneblockSuffix string

	localOneBlocksStore dstore.Store
	blockWriterFactory  bstream.BlockWriterFactory

	fileUploader *FileUploader
	logger       *zap.Logger
	tracer       logging.Tracer
}

func NewArchiver(
	startBlock uint64,
	oneblockSuffix string,
	localOneBlocksStore dstore.Store,
	remoteOneBlocksStore dstore.Store,
	blockWriterFactory bstream.BlockWriterFactory,
	logger *zap.Logger,
	tracer logging.Tracer,
) *Archiver {

	fileUploader := NewFileUploader(
		localOneBlocksStore,
		remoteOneBlocksStore,
		logger)

	a := &Archiver{
		Shutter:             shutter.New(),
		startBlock:          startBlock,
		oneblockSuffix:      oneblockSuffix,
		localOneBlocksStore: localOneBlocksStore,
		blockWriterFactory:  blockWriterFactory,
		fileUploader:        fileUploader,
		logger:              logger,
		tracer:              tracer,
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
	go a.fileUploader.Start(ctx)
}

func (a *Archiver) StoreBlock(ctx context.Context, block *bstream.Block) error {
	if block.Number < a.startBlock {
		a.logger.Debug("skipping block below start_block", zap.Stringer("block", block), zap.Uint64("start_block", a.startBlock))
		return nil
	}

	buffer := bytes.NewBuffer(nil)
	blockWriter, err := a.blockWriterFactory.New(buffer)
	if err != nil {
		return fmt.Errorf("write block factory: %w", err)
	}

	if err := blockWriter.Write(block); err != nil {
		return fmt.Errorf("write block: %w", err)
	}

	return a.localOneBlocksStore.WriteObject(ctx, bstream.BlockFileNameWithSuffix(block, a.oneblockSuffix), buffer)
}
