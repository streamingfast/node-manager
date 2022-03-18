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

	"github.com/zhongshuwen/node-manager"

	"github.com/dfuse-io/bstream"
	"github.com/dfuse-io/bstream/blockstream"
	"github.com/dfuse-io/dstore"
	"github.com/streamingfast/shutter"
	"go.uber.org/zap"
	"google.golang.org/grpc"
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

	writer              *io.PipeWriter
	consoleReader       ConsolerReader
	consumeReadFlowDone chan interface{}
	ContinuityChecker   ContinuityChecker
	transformer         ConsoleReaderBlockTransformer
	archiver            Archiver
	gator               Gator
	stopAtBlockNum      uint64
	channelCapacity     int
	blockServer         *blockstream.Server

	headBlockUpdateFunc manageos.HeadBlockUpdater

	setMaintenanceFunc func()
	stopBlockReachFunc func()
}

func RunMindReaderPlugin(
	archiveStoreURL string,
	mergeArchiveStoreURL string,
	mergeUploadDirectly bool,
	discardAfterStopBlock bool,
	workingDirectory string,
	blockFileNamer BlockFileNamer,
	consoleReaderFactory ConsolerReaderFactory,
	consoleReaderTransformer ConsoleReaderBlockTransformer,
	grpcServer *grpc.Server,
	startBlockNum uint64,
	stopBlockNum uint64,
	channelCapacity int,
	headBlockUpdateFunc manageos.HeadBlockUpdater,
	setMaintenanceFunc func(),
	stopBlockReachFunc func(),
	failOnNonContinuousBlocks bool,
) (*MindReaderPlugin, error) {
	archiveStore, err := dstore.NewDBinStore(archiveStoreURL)
	if err != nil {
		return nil, fmt.Errorf("setting up dbin store: %s", err)
	}

	archiveStore.SetOverwrite(true)

	gator := NewBlockNumberGator(startBlockNum)

	blockServer := blockstream.NewServer(grpcServer)

	// checking if working directory exists, if it does not create it....

	if _, err := os.Stat(workingDirectory); os.IsNotExist(err) {
		err = os.MkdirAll(workingDirectory, os.ModePerm)
		if err != nil {
			// TODO: maybe we should exist out?
			zlog.Error("unable to create working directory", zap.String("working_directory", workingDirectory))
		}
	}

	var continuityChecker ContinuityChecker // cannot use *continuityChecker here, because of golang caveat with checking nil value on interface{}

	cc, err := newContinuityChecker(filepath.Join(workingDirectory, "continuity_check"))
	if err != nil {
		return nil, fmt.Errorf("error setting up continuity checker: %s", err)
	}
	if failOnNonContinuousBlocks {
		continuityChecker = cc
	} else {
		cc.Reset()
	}

	var archiver Archiver
	if mergeUploadDirectly {
		zlog.Debug("using merge-upload-directly")
		mergeArchiveStore, err := dstore.NewDBinStore(mergeArchiveStoreURL)
		if err != nil {
			return nil, fmt.Errorf("setting up merge dbin store: %s", err)
		}
		mergeArchiveStore.SetOverwrite(true)

		var options []MergeArchiverOption
		if stopBlockNum != 0 {
			if discardAfterStopBlock {
				zlog.Info("archivestore will discard any block after stop-block-num -- this will create a hole in block files after restart", zap.Uint64("stop-block-num", stopBlockNum))
			} else {
				zlog.Info("blocks after stop-block-num will be saved to Oneblock files to be merged afterwards", zap.Uint64("stop-block-num", stopBlockNum))
				oneblockArchiver := NewOneblockArchiver(workingDirectory, archiveStore, blockFileNamer, bstream.GetBlockWriterFactory, 0)
				options = append(options, WithOverflowArchiver(oneblockArchiver))
			}
		}
		ra := NewMergeArchiver(mergeArchiveStore, bstream.GetBlockWriterFactory, stopBlockNum, options...)
		archiver = ra
	} else {
		var archiverStopBlockNum uint64
		if stopBlockNum != 0 && discardAfterStopBlock {
			zlog.Info("setting ArchiveStore to discard any block after stop-block-num -- this will create a hole after restart", zap.Uint64("stop-block-num", stopBlockNum))
			archiverStopBlockNum = stopBlockNum
		}
		archiver = NewOneblockArchiver(workingDirectory, archiveStore, blockFileNamer, bstream.GetBlockWriterFactory, archiverStopBlockNum)
	}

	if err := archiver.init(); err != nil {
		return nil, fmt.Errorf("failed to init archiver: %s", err)
	}

	mindReaderPlugin, err := NewMindReaderPlugin(archiver, blockServer, consoleReaderFactory, consoleReaderTransformer, continuityChecker, gator, stopBlockNum, channelCapacity, headBlockUpdateFunc)
	if err != nil {
		return nil, err
	}
	mindReaderPlugin.setMaintenanceFunc = setMaintenanceFunc
	mindReaderPlugin.stopBlockReachFunc = stopBlockReachFunc

	mindReaderPlugin.OnTerminating(func(_ error) {
		zlog.Info("mindreader plugin OnTerminating called")
		mindReaderPlugin.setMaintenanceFunc()
		mindReaderPlugin.cleanUp()
		if stopBlockNum != 0 {
			mindReaderPlugin.stopBlockReachFunc()
		}
	})

	go mindReaderPlugin.ReadFlow()

	return mindReaderPlugin, nil
}

func NewMindReaderPlugin(
	archiver Archiver,
	blockServer *blockstream.Server,
	consoleReaderFactory ConsolerReaderFactory,
	consoleReaderTransformer ConsoleReaderBlockTransformer,
	continuityChecker ContinuityChecker,
	gator Gator,
	stopAtBlockNum uint64,
	channelCapacity int,
	headBlockUpdateFunc manageos.HeadBlockUpdater,
) (*MindReaderPlugin, error) {
	pipeReader, pipeWriter := io.Pipe()
	consoleReader, err := consoleReaderFactory(pipeReader)
	if err != nil {
		return nil, err
	}

	return &MindReaderPlugin{
		Shutter:             shutter.New(),
		consoleReader:       consoleReader,
		ContinuityChecker:   continuityChecker,
		consumeReadFlowDone: make(chan interface{}),
		transformer:         consoleReaderTransformer,
		blockServer:         blockServer,
		writer:              pipeWriter,
		archiver:            archiver,
		gator:               gator,
		stopAtBlockNum:      stopAtBlockNum,
		channelCapacity:     channelCapacity,
		headBlockUpdateFunc: headBlockUpdateFunc,
	}, nil
}

func (p *MindReaderPlugin) cleanUp() {
	zlog.Info("waiting until consume read flow (i.e. blocks) is actually done processing blocks...")
	<-p.consumeReadFlowDone
}

func (p *MindReaderPlugin) ReadFlow() {
	blocks := make(chan *bstream.Block, p.channelCapacity)

	go p.consumeReadFlow(blocks)
	go p.alwaysUploadFiles()

	for {
		// ALWAYS READ (otherwise you'll stall `nodeos`' shutdown process, want a dirty flag?)
		err := p.readOneMessage(blocks)
		if err != nil {
			if err == io.EOF {
				zlog.Info("Mindreader plugin shut down correctly")
				return
			}
			zlog.Error("reading from console logs", zap.Error(err))
			p.setMaintenanceFunc()
			continue
		}
	}
}

func (p *MindReaderPlugin) alwaysUploadFiles() {
	for {
		if p.IsTerminating() { // the uploadFiles will be called again in 'cleanup()', we can leave here early
			return
		}

		if err := p.archiver.uploadFiles(); err != nil {
			zlog.Warn("failed to upload stale files", zap.Error(err))
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
	defer func() {
		p.archiver.cleanup()
		zlog.Debug("archiver cleanup done")
		close(p.consumeReadFlowDone)
	}()

	for {
		select {
		case <-p.Terminating():
			// We keep going if more blocks to process, so we never skip writing blocks
			zlog.Debug("received a terminating sig")
			if len(blocks) == 0 {
				zlog.Info("will shutdown when console reader is done")
				<-p.consoleReader.Done()
				zlog.Info("console reader is done")
				return
			}
			zlog.Info("will shutdown when block count == 0", zap.Int("block_count", len(blocks)))

		case block := <-blocks:
			err := p.archiver.storeBlock(block)
			if err != nil {
				zlog.Error("failed storing block in archiver", zap.Error(err))
				p.Shutdown(fmt.Errorf("archiver.storeBlock failed: %s", err))
				return
			}

			if p.ContinuityChecker != nil {
				err = p.ContinuityChecker.Write(block.Num())
				if err != nil {
					zlog.Error("failed continuity check", zap.Error(err))
					p.setMaintenanceFunc()
					continue
				}
			}

			err = p.blockServer.PushBlock(block)
			if err != nil {
				zlog.Error("failed passing block to blockServer", zap.Error(err))
				p.Shutdown(fmt.Errorf("failed writing to blocks server handler: %s", err))
				return
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
		return fmt.Errorf("unable to transform console read obj to bstream.Block: %s", err)
	}

	if !p.gator.pass(block) {
		return nil
	}

	if p.headBlockUpdateFunc != nil {
		p.headBlockUpdateFunc(block.Num(), block.ID(), block.Time())
	}

	blocks <- block

	if p.stopAtBlockNum != 0 && block.Num() >= p.stopAtBlockNum && !p.IsTerminating() {
		zlog.Info("shutting down because requested end block reached", zap.Uint64("block_num", block.Num()))
		go p.Shutdown(nil)
	}

	return nil
}

func (p *MindReaderPlugin) Close(err error) {
	e := p.writer.CloseWithError(err)
	if e != nil {
		zlog.Error("error closing mindreader writer", zap.Error(e))
	}
	p.Shutdown(err)
}

// LogLine receives log line and write it to "pipe" of the local console reader
func (p *MindReaderPlugin) LogLine(in string) {
	if _, err := p.writer.Write(append([]byte(in), '\n')); err != nil {
		zlog.Error("writing to export pipeline", zap.Error(err))
		p.Shutdown(err)
	}
}
