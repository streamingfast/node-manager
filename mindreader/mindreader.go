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
	"path"
	"regexp"
	"time"

	"github.com/streamingfast/bstream"
	"github.com/streamingfast/bstream/blockstream"
	"github.com/streamingfast/dstore"
	"github.com/streamingfast/logging"
	nodeManager "github.com/streamingfast/node-manager"
	"github.com/streamingfast/shutter"
	"go.uber.org/zap"
)

var (
	oneblockSuffixRegexp = regexp.MustCompile(`^[\w\-]+$`)
)

type ConsolerReader interface {
	ReadBlock() (obj *bstream.Block, err error)
	Done() <-chan interface{}
}

type ConsolerReaderFactory func(lines chan string) (ConsolerReader, error)

type MindReaderPlugin struct {
	*shutter.Shutter
	zlogger *zap.Logger

	startGate *BlockNumberGate // if set, discard blocks before this
	stopBlock uint64           // if set, call shutdownFunc(nil) when we hit this number

	waitUploadCompleteOnShutdown time.Duration // if non-zero, will try to upload files for this amount of time. Failed uploads will stay in workingDir

	lines         chan string
	consoleReader ConsolerReader // contains the 'reader' part of the pipe

	channelCapacity int // transformed blocks are buffered in a channel

	archiver                 *Archiver // transformed blocks are sent to Archiver
	oneBlockFileUploader     *FileUploader
	mergedBlocksFileUploader *FileUploader

	consumeReadFlowDone chan interface{}

	blockStreamServer    *blockstream.Server
	headBlockUpdateFunc  nodeManager.HeadBlockUpdater
	consoleReaderFactory ConsolerReaderFactory
}

// NewMindReaderPlugin initiates its own:
// * ConsoleReader (from given Factory)
// * Archiver (from archive store params)
// * ContinuityChecker
// * Shutter
func NewMindReaderPlugin(
	archiveStoreURL string,
	mergeArchiveStoreURL string,
	mergeThresholdBlockAge string,
	workingDirectory string,
	consoleReaderFactory ConsolerReaderFactory,
	startBlockNum uint64,
	stopBlockNum uint64,
	channelCapacity int,
	headBlockUpdateFunc nodeManager.HeadBlockUpdater,
	shutdownFunc func(error),
	waitUploadCompleteOnShutdown time.Duration,
	oneblockSuffix string,
	blockStreamServer *blockstream.Server,
	zlogger *zap.Logger,
	tracer logging.Tracer,
) (*MindReaderPlugin, error) {
	err := validateOneBlockSuffix(oneblockSuffix)
	if err != nil {
		return nil, err
	}

	var parsedMergeThresholdBlockAge time.Duration
	switch mergeThresholdBlockAge {
	case "never":
		parsedMergeThresholdBlockAge = 0
	case "always":
		parsedMergeThresholdBlockAge = 1
	default:
		parsedMergeThresholdBlockAge, err = time.ParseDuration(mergeThresholdBlockAge)
		if err != nil {
			return nil, fmt.Errorf("cannot parse merge-threshold-duration. Should be one of 'never', 'always', or a valid golang duration string (ex: 1h)")
		}
	}
	zlogger.Info("creating mindreader plugin",
		zap.String("archive_store_url", archiveStoreURL),
		zap.String("merge_archive_store_url", mergeArchiveStoreURL),
		zap.String("oneblock_suffix", oneblockSuffix),
		zap.Duration("merge_threshold_age", parsedMergeThresholdBlockAge),
		zap.String("working_directory", workingDirectory),
		zap.Uint64("start_block_num", startBlockNum),
		zap.Uint64("stop_block_num", stopBlockNum),
		zap.Int("channel_capacity", channelCapacity),
		zap.Bool("with_head_block_update_func", headBlockUpdateFunc != nil),
		zap.Bool("with_shutdown_func", shutdownFunc != nil),
		zap.Duration("wait_upload_complete_on_shutdown", waitUploadCompleteOnShutdown),
	)

	// Create directory and its parent(s), it's a no-op if everything already exists
	err = os.MkdirAll(workingDirectory, os.ModePerm)
	if err != nil {
		return nil, fmt.Errorf("create working directory: %w", err)
	}

	mergeableOneBlockDir := path.Join(workingDirectory, "mergeable")
	uploadableOneBlocksDir := path.Join(workingDirectory, "uploadable-oneblock")
	uploadableMergedBlocksDir := path.Join(workingDirectory, "uploadable-merged")

	// remote stores
	newDBinStoreNoCompress := func(s string) (dstore.Store, error) {
		return dstore.NewStore(s, "dbin.zst", "", false)
	}
	oneBlocksStore, err := newDBinStoreNoCompress(archiveStoreURL)
	if err != nil {
		return nil, fmt.Errorf("new one block store: %w", err)
	}
	mergedBlocksStore, err := newDBinStoreNoCompress(mergeArchiveStoreURL)
	if err != nil {
		return nil, fmt.Errorf("new merge blocks store: %w", err)
	}

	// local stores
	mergeableOneBlocksStore, err := dstore.NewDBinStore(mergeableOneBlockDir)
	if err != nil {
		return nil, fmt.Errorf("new mergeableOneBlocksStore: %w", err)
	}
	uploadableMergedBlocksStore, err := dstore.NewDBinStore(uploadableMergedBlocksDir)
	if err != nil {
		return nil, fmt.Errorf("new uploadableMergedBlocksStore: %w", err)
	}
	uploadableOneBlocksStore, err := dstore.NewDBinStore(uploadableOneBlocksDir)
	if err != nil {
		return nil, fmt.Errorf("new uploadableOneBlocksStore: %w", err)
	}

	bundleSize := uint64(100) //todo: replace this with parameter
	lowestPossibleBlock := bstream.GetProtocolFirstStreamableBlock

	archiverIO := NewArchiverDStoreIO(
		bstream.GetBlockWriterFactory,
		bstream.GetBlockReaderFactory,
		oneBlocksStore,
		uploadableOneBlocksStore,
		mergeableOneBlocksStore,
		uploadableMergedBlocksStore,
		mergedBlocksStore,
		250,
		5,
		500*time.Millisecond,
		lowestPossibleBlock,
		bundleSize,
		zlogger,
		tracer,
	)

	archiver := NewArchiver(
		bundleSize,
		archiverIO,
		oneblockSuffix,
		parsedMergeThresholdBlockAge,
		zlogger,
		tracer,
	)

	oneBlockFileUploader := NewFileUploader(uploadableOneBlocksStore, oneBlocksStore, zlogger)
	mergedBlocksFileUploader := NewFileUploader(uploadableMergedBlocksStore, mergedBlocksStore, zlogger)

	mindReaderPlugin, err := newMindReaderPlugin(
		archiver,
		oneBlockFileUploader,
		mergedBlocksFileUploader,
		consoleReaderFactory,
		startBlockNum,
		stopBlockNum,
		channelCapacity,
		headBlockUpdateFunc,
		blockStreamServer,
		zlogger,
	)
	if err != nil {
		return nil, err
	}
	mindReaderPlugin.waitUploadCompleteOnShutdown = waitUploadCompleteOnShutdown

	return mindReaderPlugin, nil
}

func validateOneBlockSuffix(suffix string) error {
	if suffix == "" {
		return fmt.Errorf("oneblock_suffix cannot be empty")
	}
	if !oneblockSuffixRegexp.MatchString(suffix) {
		return fmt.Errorf("oneblock_suffix contains invalid characters: %q", suffix)
	}
	return nil
}

// Other components may have issues finding the one block files if suffix is invalid

func newMindReaderPlugin(
	archiver *Archiver,
	oneBlockFileUploader *FileUploader,
	mergedBlocksFileUploader *FileUploader,
	consoleReaderFactory ConsolerReaderFactory,
	startBlock uint64,
	stopBlock uint64,
	channelCapacity int,
	headBlockUpdateFunc nodeManager.HeadBlockUpdater,
	blockStreamServer *blockstream.Server,
	zlogger *zap.Logger,
) (*MindReaderPlugin, error) {
	zlogger.Info("creating new mindreader plugin")
	return &MindReaderPlugin{
		Shutter:                  shutter.New(),
		consoleReaderFactory:     consoleReaderFactory,
		archiver:                 archiver,
		oneBlockFileUploader:     oneBlockFileUploader,
		mergedBlocksFileUploader: mergedBlocksFileUploader,
		startGate:                NewBlockNumberGate(startBlock),
		stopBlock:                stopBlock,
		channelCapacity:          channelCapacity,
		headBlockUpdateFunc:      headBlockUpdateFunc,
		zlogger:                  zlogger,
		blockStreamServer:        blockStreamServer,
	}, nil
}

func (p *MindReaderPlugin) Name() string {
	return "MindReaderPlugin"
}

func (p *MindReaderPlugin) Launch() {
	ctx, cancel := context.WithCancel(context.Background())
	p.OnTerminating(func(err error) {
		cancel()
	})

	p.zlogger.Info("starting mindreader")

	p.consumeReadFlowDone = make(chan interface{})

	lines := make(chan string, 10000) //need a config here?
	p.lines = lines

	consoleReader, err := p.consoleReaderFactory(lines)
	if err != nil {
		p.Shutdown(err)
	}
	p.consoleReader = consoleReader

	p.zlogger.Debug("starting archiver")
	p.archiver.Start(ctx)
	p.zlogger.Debug("starting one block uploader")
	go p.oneBlockFileUploader.Start(ctx)
	p.zlogger.Debug("starting file uploader")
	go p.mergedBlocksFileUploader.Start(ctx)

	p.launch()

}
func (p *MindReaderPlugin) launch() {
	blocks := make(chan *bstream.Block, p.channelCapacity)
	p.zlogger.Debug("launching consume read flow", zap.Int("capacity", p.channelCapacity))
	go p.consumeReadFlow(blocks)

	go func() {
		for {
			err := p.readOneMessage(blocks)
			if err != nil {
				if err == io.EOF {
					p.zlogger.Info("reached end of console reader stream, nothing more to do")
					close(blocks)
					return
				}
				p.zlogger.Error("reading from console logs", zap.Error(err))
				p.Shutdown(err)
				// Always read messages otherwise you'll stall the shutdown lifecycle of the managed process, leading to corrupted database if exit uncleanly afterward
				p.drainMessages()
				close(blocks)
				return
			}
		}
	}()
}

func (p MindReaderPlugin) Stop() {
	p.zlogger.Info("mindreader is stopping")
	if p.lines == nil {
		// If the `lines` channel was not created yet, it means everything was shut down very rapidly
		// and means MindreaderPlugin has not launched yet. Since it has not launched yet, there is
		// no point in waiting for the read flow to complete since the read flow never started. So
		// we exit right now.
		return
	}

	p.Shutdown(nil)

	close(p.lines)
	p.waitForReadFlowToComplete()
}

func (p *MindReaderPlugin) waitForReadFlowToComplete() {
	p.zlogger.Info("waiting until consume read flow (i.e. blocks) is actually done processing blocks...")
	<-p.consumeReadFlowDone
	p.zlogger.Info("consume read flow terminate")
}

// consumeReadFlow is the one function blocking termination until consumption/writeBlock/upload is done
func (p *MindReaderPlugin) consumeReadFlow(blocks <-chan *bstream.Block) {
	p.zlogger.Info("starting consume flow")
	defer close(p.consumeReadFlowDone)

	ctx := context.Background()
	for {
		p.zlogger.Debug("waiting to consume next block.")
		block, ok := <-blocks
		if !ok {
			p.zlogger.Info("all blocks in channel were drained, exiting read flow")
			p.archiver.Shutdown(nil)
			select {
			case <-time.After(p.waitUploadCompleteOnShutdown):
				p.zlogger.Info("upload may not be complete: timeout waiting for UploadComplete on shutdown", zap.Duration("wait_upload_complete_on_shutdown", p.waitUploadCompleteOnShutdown))
			case <-p.archiver.Terminated():
				p.zlogger.Info("archiver Terminate done")
			}

			return
		}

		p.zlogger.Debug("got one block", zap.Uint64("block_num", block.Number))

		err := p.archiver.StoreBlock(ctx, block)
		if err != nil {
			p.zlogger.Error("failed storing block in archiver, shutting down and trying to send next blocks individually. You will need to reprocess over this range.", zap.Error(err), zap.Stringer("received_block", block))

			if !p.IsTerminating() {
				p.archiver.currentlyMerging = false // no more merging when broken
				go p.Shutdown(fmt.Errorf("archiver store block failed: %w", err))
				continue
			}
		}
		if p.blockStreamServer != nil {
			err = p.blockStreamServer.PushBlock(block)
			if err != nil {
				p.zlogger.Error("failed passing block to blockStreamServer (this should not happen, shutting down)", zap.Error(err))
				if !p.IsTerminating() {
					go p.Shutdown(fmt.Errorf("blockstreamserver failed: %w", err))
				}

				continue
			}
		}

	}
}

func (p *MindReaderPlugin) drainMessages() {
	for line := range p.lines {
		_ = line
	}
	return
}

func (p *MindReaderPlugin) readOneMessage(blocks chan<- *bstream.Block) error {
	block, err := p.consoleReader.ReadBlock()
	if err != nil {
		return err
	}

	if !p.startGate.pass(block) {
		return nil
	}

	if p.headBlockUpdateFunc != nil {
		p.headBlockUpdateFunc(block.Num(), block.ID(), block.Time())
	}

	blocks <- block

	if p.stopBlock != 0 && block.Num() >= p.stopBlock && !p.IsTerminating() {
		p.zlogger.Info("shutting down because requested end block reached", zap.Uint64("block_num", block.Num()))
		go p.Shutdown(nil)
	}

	return nil
}

// LogLine receives log line and write it to "pipe" of the local console reader
func (p *MindReaderPlugin) LogLine(in string) {
	if p.IsTerminating() {
		return
	}
	p.lines <- in
}
