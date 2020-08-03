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
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/dfuse-io/bstream"
	"github.com/dfuse-io/bstream/blockstream"
	"github.com/dfuse-io/dstore"
	nodeManager "github.com/dfuse-io/node-manager"
	"github.com/dfuse-io/shutter"
	"go.uber.org/zap"
)

type ConsolerReader interface {
	Read() (obj interface{}, err error)
	Done() <-chan interface{}
}

type ConsolerReaderFactory func(reader io.Reader) (ConsolerReader, error)

// ConsoleReaderBlockTransformer is a function that accepts an `obj` of type
// `interface{}` as produced by a specialized ConsoleReader implementation and
// turns it into a `bstream.Block` that is able to flow in block streams.
type ConsoleReaderBlockTransformer func(obj interface{}) (*bstream.Block, error)

type MindReaderPlugin struct {
	*shutter.Shutter
	zlogger *zap.Logger

	startGate          *BlockNumberGate // if set, discard blocks before this
	stopBlock          uint64           // if set, call stopBlockReachFunc() when we hit this number
	stopBlockReachFunc func()

	waitUploadCompleteOnShutdown time.Duration // if non-zero, will try to upload files for this amount of time. Failed uploads will stay in workingDir

	writer        *io.PipeWriter // LogLine writes there
	consoleReader ConsolerReader // contains the 'reader' part of the pipe

	transformer     ConsoleReaderBlockTransformer // objects read from consoleReader are transformed into blocks
	channelCapacity int                           // transformed blocks are buffered in a channel
	archiver        Archiver                      // transformed blocks are sent to Archiver

	consumeReadFlowDone chan interface{}
	continuityChecker   ContinuityChecker

	blockServer         *blockstream.Server
	headBlockUpdateFunc nodeManager.HeadBlockUpdater

	setMaintenanceFunc func()
}

// NewMindReaderPlugin initiates its own:
// * ConsoleReader (from given Factory)
// * ConsoleReaderBlockTransformer (from given Factory)
// * Archiver (from archive store params)
// * ContinuityChecker
// * Shutter
func NewMindReaderPlugin(
	archiveStoreURL string,
	mergeArchiveStoreURL string,
	batchMode bool,
	mergeThresholdBlockAge time.Duration,
	workingDirectory string,
	consoleReaderFactory ConsolerReaderFactory,
	consoleReaderTransformer ConsoleReaderBlockTransformer,
	startBlockNum uint64,
	stopBlockNum uint64,
	discardAfterStopBlock bool,
	channelCapacity int,
	headBlockUpdateFunc nodeManager.HeadBlockUpdater,
	setMaintenanceFunc func(),
	stopBlockReachFunc func(),
	failOnNonContinuousBlocks bool,
	waitUploadCompleteOnShutdown time.Duration,
	zlogger *zap.Logger,
) (*MindReaderPlugin, error) {
	zlogger.Info("creating mindreader plugin",
		zap.String("archive_store_url", archiveStoreURL),
		zap.String("merge_archive_store_url", mergeArchiveStoreURL),
		zap.Bool("batch_mode", batchMode),
		zap.Duration("merge_threshold_age", mergeThresholdBlockAge),
		zap.String("working_directory", workingDirectory),
		zap.Uint64("start_block_num", startBlockNum),
		zap.Uint64("stop_block_num", stopBlockNum),
		zap.Int("channel_capacity", channelCapacity),
		zap.Bool("with_head_block_update_func", headBlockUpdateFunc != nil),
		zap.Bool("with_set_maintenance_func", setMaintenanceFunc != nil),
		zap.Bool("with_stop_block_reach_func", stopBlockReachFunc != nil),
		zap.Bool("fail_on_non_continuous_blocks", failOnNonContinuousBlocks),
		zap.Duration("wait_upload_complete_on_shutdown", waitUploadCompleteOnShutdown),
	)

	// Create directory and its parent(s), it's a no-op if everything already exists
	err := os.MkdirAll(workingDirectory, os.ModePerm)
	if err != nil {
		return nil, fmt.Errorf("unable to create working directory %q: %w", workingDirectory, err)
	}

	var continuityChecker ContinuityChecker
	cc, err := NewContinuityChecker(filepath.Join(workingDirectory, "continuity_check"), zlogger)
	if err != nil {
		return nil, fmt.Errorf("error setting up continuity checker: %w", err)
	}

	if failOnNonContinuousBlocks {
		continuityChecker = cc
	} else {
		cc.Reset()
	}

	oneblockArchiveStore, err := dstore.NewDBinStore(archiveStoreURL) // never overwrites
	if err != nil {
		return nil, fmt.Errorf("setting up archive store: %w", err)
	}

	mergeArchiveStore, err := dstore.NewDBinStore(mergeArchiveStoreURL)
	if err != nil {
		return nil, fmt.Errorf("setting up merge archive store: %w", err)
	}

	var archiver Archiver

	if batchMode {
		mergeArchiveStore.SetOverwrite(true)
		archiver = NewMergeArchiver(mergeArchiveStore, workingDirectory, bstream.GetBlockWriterFactory, bstream.GetBlockReaderFactory, zlogger)
	} else {
		archiver = NewHybridArchiver(oneblockArchiveStore, mergeArchiveStore, bstream.GetBlockWriterFactory, bstream.GetBlockReaderFactory, mergeThresholdBlockAge, workingDirectory, zlogger)
	}

	if err := archiver.Init(); err != nil {
		return nil, fmt.Errorf("failed to init archiver: %w", err)
	}

	mindReaderPlugin, err := newMindReaderPlugin(
		archiver,
		consoleReaderFactory,
		consoleReaderTransformer,
		continuityChecker,
		startBlockNum,
		stopBlockNum,
		channelCapacity,
		headBlockUpdateFunc,
		zlogger,
	)
	if err != nil {
		return nil, err
	}
	mindReaderPlugin.waitUploadCompleteOnShutdown = waitUploadCompleteOnShutdown
	mindReaderPlugin.setMaintenanceFunc = setMaintenanceFunc
	mindReaderPlugin.stopBlockReachFunc = stopBlockReachFunc

	mindReaderPlugin.OnTerminating(func(_ error) {
		zlogger.Info("mindreader plugin OnTerminating called")
		mindReaderPlugin.waitForReadFlowToComplete()
		if stopBlockNum != 0 {
			mindReaderPlugin.stopBlockReachFunc()
		}
	})

	return mindReaderPlugin, nil
}

func (p *MindReaderPlugin) Run(server *blockstream.Server) {
	p.zlogger.Info("running")
	p.blockServer = server
	go p.ReadFlow()
}

func newMindReaderPlugin(
	archiver Archiver,
	consoleReaderFactory ConsolerReaderFactory,
	consoleReaderTransformer ConsoleReaderBlockTransformer,
	continuityChecker ContinuityChecker,
	startBlock uint64,
	stopBlock uint64,
	channelCapacity int,
	headBlockUpdateFunc nodeManager.HeadBlockUpdater,
	zlogger *zap.Logger,
) (*MindReaderPlugin, error) {
	pipeReader, pipeWriter := io.Pipe()
	consoleReader, err := consoleReaderFactory(pipeReader)
	if err != nil {
		return nil, err
	}
	zlogger.Info("creating new mindreader plugin")
	return &MindReaderPlugin{
		Shutter:             shutter.New(),
		consoleReader:       consoleReader,
		continuityChecker:   continuityChecker,
		consumeReadFlowDone: make(chan interface{}),
		transformer:         consoleReaderTransformer,
		writer:              pipeWriter,
		archiver:            archiver,
		startGate:           NewBlockNumberGate(startBlock),
		stopBlock:           stopBlock,
		channelCapacity:     channelCapacity,
		headBlockUpdateFunc: headBlockUpdateFunc,
		zlogger:             zlogger,
	}, nil
}

func (p *MindReaderPlugin) waitForReadFlowToComplete() {
	p.zlogger.Info("waiting until consume read flow (i.e. blocks) is actually done processing blocks...")
	<-p.consumeReadFlowDone
}

func (p *MindReaderPlugin) ReadFlow() {
	p.zlogger.Info("starting read flow")
	blocks := make(chan *bstream.Block, p.channelCapacity)

	go p.consumeReadFlow(blocks)
	go p.alwaysUploadFiles()

	for {
		// ALWAYS READ (otherwise you'll stall `nodeos`' shutdown process, want a dirty flag?)
		err := p.readOneMessage(blocks)
		if err != nil {
			if err == io.EOF {
				p.zlogger.Info("reached end of console reader stream, nothing more to do")
				close(blocks)
				return
			}
			p.zlogger.Error("reading from console logs", zap.Error(err))
			p.setMaintenanceFunc()
			continue
		}
	}
}

func (p *MindReaderPlugin) alwaysUploadFiles() {
	p.zlogger.Info("starting file upload")
	lastUploadFailed := false
	for {
		if p.IsTerminating() { // the uploadFiles will be called again in 'WaitForAllFilesToUpload()', we can leave here early
			return
		}

		err := p.archiver.uploadFiles()
		if err != nil {
			p.zlogger.Warn("temporary failure trying to upload mindreader block files, will retry", zap.Error(err))
			lastUploadFailed = true
		} else {
			if lastUploadFailed {
				p.zlogger.Warn("success uploading previously failed mindreader block files")
				lastUploadFailed = false
			}
		}

		select {
		case <-p.Terminating():
			return
		case <-time.After(500 * time.Millisecond):
		}
	}
}

// consumeReadFlow is the one function blocking termination until consumption/writeBlock/upload is done
func (p *MindReaderPlugin) consumeReadFlow(blocks <-chan *bstream.Block) {
	p.zlogger.Info("starting consume flow")
	defer close(p.consumeReadFlowDone)

	for {
		block, ok := <-blocks
		if !ok {
			p.zlogger.Info("all blocks in channel were drained, exiting read flow")
			select {
			case <-time.After(p.waitUploadCompleteOnShutdown):
				p.zlogger.Info("upload may not be complete: timeout waiting for UploadComplete on shutdown", zap.Duration("wait_upload_complete_on_shutdown", p.waitUploadCompleteOnShutdown))
			case <-p.archiver.WaitForAllFilesToUpload():
				p.zlogger.Info("archiver WaitForAllFilesToUpload done")
			}
			return
		}

		err := p.archiver.storeBlock(block)
		if err != nil {
			p.zlogger.Error("failed storing block in archiver, shutting down and losing blocks in transit...", zap.Error(err))
			go p.Shutdown(fmt.Errorf("archiver store block failed: %w", err))
			return
		}
		if p.blockServer != nil {
			err = p.blockServer.PushBlock(block)
			if err != nil {
				p.zlogger.Error("failed passing block to blockServer (this should not happen)", zap.Error(err))
				return
			}
		}

		if p.continuityChecker != nil {
			err = p.continuityChecker.Write(block.Num())
			if err != nil {
				p.zlogger.Error("failed continuity check", zap.Error(err))
				p.setMaintenanceFunc()
				continue
			}
		}
	}
}

func (p *MindReaderPlugin) readOneMessage(blocks chan<- *bstream.Block) error {
	obj, err := p.consoleReader.Read()
	if err != nil {
		return err
	}

	block, err := p.transformer(obj)
	if err != nil {
		return fmt.Errorf("unable to transform console read obj to bstream.Block: %w", err)
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

func (p *MindReaderPlugin) Close(err error) {
	p.zlogger.Info("closing pipe writer and shutting down plugin")
	p.writer.CloseWithError(err)
	p.Shutdown(err)
}

// LogLine receives log line and write it to "pipe" of the local console reader
func (p *MindReaderPlugin) LogLine(in string) {
	if _, err := p.writer.Write(append([]byte(in), '\n')); err != nil {
		p.zlogger.Error("writing to export pipeline", zap.Error(err))
		p.Shutdown(err)
	}
}

func (p *MindReaderPlugin) HasContinuityChecker() bool {
	return p.continuityChecker != nil
}

func (p *MindReaderPlugin) ResetContinuityChecker() {
	if p.continuityChecker != nil {
		p.continuityChecker.Reset()
	}
}
