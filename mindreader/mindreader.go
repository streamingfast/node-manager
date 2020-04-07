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

	pbbstream "github.com/dfuse-io/pbgo/dfuse/bstream/v1"
	"github.com/dfuse-io/shutter"
	"github.com/dfuse-io/bstream"
	"github.com/dfuse-io/bstream/blockstream"
	"github.com/dfuse-io/dstore"
	"github.com/dfuse-io/manageos/metrics"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

type ConsolerReader interface {
	Read() (obj interface{}, err error)
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
	setMaintenanceFunc  func()
}

func RunMindReaderPlugin(
	archiveStoreURL string,
	mergeUploadDirectly bool,
	workingDirectory string,
	blockFileNamer BlockFileNamer,
	blockKind pbbstream.Protocol,
	consoleReaderFactory ConsolerReaderFactory,
	consoleReaderTransformer ConsoleReaderBlockTransformer,
	grpcServer *grpc.Server,
	startBlockNum uint64,
	stopBlockNum uint64,
	channelCapacity int,
	setMaintenanceFunc func(),
) (*MindReaderPlugin, error) {
	archiveStore, err := dstore.NewDBinStore(archiveStoreURL)
	if err != nil {
		return nil, fmt.Errorf("setting up dbin store: %s", err)
	}

	archiveStore.SetOverwrite(true)

	gator := NewBlockNumberGator(startBlockNum)
	var archiver Archiver

	blockServer := blockstream.NewServer(grpcServer)

	// checking if working directory exists, if it does not create it....

	if _, err := os.Stat(workingDirectory); os.IsNotExist(err) {
		err = os.MkdirAll(workingDirectory, os.ModePerm)
		if err != nil {
			// TODO: maybe we should exist out?
			zlog.Error("unable to create working directory", zap.String("working_directory", workingDirectory))
		}
	}

	cc, err := newContinuityChecker(filepath.Join(workingDirectory, "continuity_check"))
	if err != nil {
		return nil, fmt.Errorf("error setting up continuity checker: %s", err)
	}
	if cc.IsLocked() {
		zlog.Error("continuity checker shows that a hole was previously detected. NOT STARTING PROCESS WITHOUT MANUAL reset_cc or restore")
	}
	if mergeUploadDirectly {
		ra := NewReprocArchiver(archiveStore, bstream.MustGetBlockWriterFactory(blockKind))
		archiver = ra
	} else {
		archiver = NewDefaultArchiver(workingDirectory, archiveStore, blockFileNamer, bstream.MustGetBlockWriterFactory(blockKind))
	}

	if err = archiver.init(); err != nil {
		return nil, fmt.Errorf("failed to init archiver: %s", err)
	}
	mindReaderPlugin, err := NewMindReaderPlugin(archiver, blockServer, consoleReaderFactory, consoleReaderTransformer, cc, gator, stopBlockNum, channelCapacity)
	if err != nil {
		return nil, err
	}
	mindReaderPlugin.setMaintenanceFunc = setMaintenanceFunc
	mindReaderPlugin.OnTerminating(func(_ error) {
		zlog.Info("mindreader plugin OnTerminating called")
		mindReaderPlugin.setMaintenanceFunc()
		mindReaderPlugin.cleanUp()
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
			zlog.Error("reading from console logs", zap.Error(err))
			p.setMaintenanceFunc()
		}
	}
}

func (p *MindReaderPlugin) alwaysUploadFiles() {
	for {
		// We continue to upload files as long as the plugin is not terminated yet,
		// so that blocks created while in the terminating steps are correctly processed!
		if p.IsTerminated() {
			return
		}

		if err := p.archiver.uploadFiles(); err != nil {
			zlog.Warn("failed to upload stale files", zap.Error(err))
		}

		select {
		case <-p.Terminated():
			return
		case <-time.After(500 * time.Millisecond):
		}
	}
}

func (p *MindReaderPlugin) consumeReadFlow(blocks <-chan *bstream.Block) {
	defer close(p.consumeReadFlowDone)

	for {
		select {
		case <-p.Terminating():
			// We keep going if more blocks to process, so we never skip writing blocks
			zlog.Debug("received a terminating sig")
			if len(blocks) == 0 {
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

			err = p.ContinuityChecker.Write(block.Num())
			if err != nil {
				zlog.Error("failed continuity check", zap.Error(err))
				p.setMaintenanceFunc()
				continue
			}

			err = p.blockServer.PushBlock(block)
			if err != nil {
				zlog.Error("failed passing block to blockServer", zap.Error(err))
				p.Shutdown(fmt.Errorf("failed writing to blocks server handler: %s", err))
				return
			}

			if p.stopAtBlockNum != 0 && block.Num() >= p.stopAtBlockNum {
				zlog.Info("shutting down because requested end block reached", zap.Uint64("block_num", block.Num()))
				p.Shutdown(nil)
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

	// TODO: this is an ugly hack, for no data chain, this will prevent filling the buffer for reprocessing
	if p.stopAtBlockNum != 0 && block.Num() > p.stopAtBlockNum {
		return nil
	}

	metrics.HeadBlockNumber.SetUint64(block.Num())
	metrics.HeadBlockTimeDrift.SetBlockTime(block.Time())

	blocks <- block
	//if len(blocks) > 9*cap(blocks)/10 { // when channel is 90%, returning an error here will trigger a shutdown of the emitting process, but it may generate more blocks before it is completely stopped, so we keep a buffer
	//	return fmt.Errorf("blocks chan streaming out of mindreader is getting full, shutting down to prevent catastrophic failure")
	//}
	return nil
}

// LogLine receives log line and write it to "pipe" of the local console reader
func (p *MindReaderPlugin) LogLine(in string) {
	if _, err := p.writer.Write(append([]byte(in), '\n')); err != nil {
		zlog.Error("writing to export pipeline", zap.Error(err))
		p.Shutdown(err)
	}
}
