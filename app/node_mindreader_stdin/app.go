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

package node_mindreader_stdin

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/dfuse-io/bstream"
	"github.com/dfuse-io/bstream/blockstream"
	"github.com/streamingfast/dgrpc"
	nodeManager "github.com/streamingfast/node-manager"
	"github.com/streamingfast/node-manager/mindreader"
	"github.com/streamingfast/shutter"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

type Config struct {
	GRPCAddr                     string
	ArchiveStoreURL              string
	MergeArchiveStoreURL         string
	OneblockSuffix               string
	BatchMode                    bool
	MergeThresholdBlockAge       time.Duration
	MindReadBlocksChanCapacity   int
	FailOnNonContinuousBlocks    bool
	StartBlockNum                uint64
	StopBlockNum                 uint64
	DiscardAfterStopBlock        bool
	WorkingDir                   string
	WaitUploadCompleteOnShutdown time.Duration
}

type Modules struct {
	ConsoleReaderFactory       mindreader.ConsolerReaderFactory
	ConsoleReaderTransformer   mindreader.ConsoleReaderBlockTransformer
	MetricsAndReadinessManager *nodeManager.MetricsAndReadinessManager
	RegisterGRPCService        func(server *grpc.Server) error
	Tracker                    *bstream.Tracker
}

type App struct {
	*shutter.Shutter
	Config    *Config
	ReadyFunc func()
	modules   *Modules
	zlogger   *zap.Logger
}

func New(c *Config, modules *Modules, zlogger *zap.Logger) *App {
	n := &App{
		Shutter:   shutter.New(),
		Config:    c,
		ReadyFunc: func() {},
		modules:   modules,
		zlogger:   zlogger,
	}
	return n
}

func (a *App) Run() error {
	a.zlogger.Info("launching nodeos mindreader-stdin", zap.Reflect("config", a.Config))

	gs := dgrpc.NewServer(dgrpc.WithLogger(a.zlogger))

	a.zlogger.Info("launching mindreader plugin")
	mindreaderLogPlugin, err := mindreader.NewMindReaderPlugin(
		a.Config.ArchiveStoreURL,
		a.Config.MergeArchiveStoreURL,
		a.Config.BatchMode,
		a.Config.MergeThresholdBlockAge,
		a.Config.WorkingDir,
		a.modules.ConsoleReaderFactory,
		a.modules.ConsoleReaderTransformer,
		a.modules.Tracker,
		a.Config.StartBlockNum,
		a.Config.StopBlockNum,
		a.Config.MindReadBlocksChanCapacity,
		a.modules.MetricsAndReadinessManager.UpdateHeadBlock,
		a.Shutdown,
		a.Config.FailOnNonContinuousBlocks,
		a.Config.WaitUploadCompleteOnShutdown,
		a.Config.OneblockSuffix,
		a.zlogger,
	)
	if err != nil {
		return err
	}

	a.zlogger.Debug("configuring shutter")
	a.OnTerminating(mindreaderLogPlugin.Shutdown)

	// It's important that this call goes prior running gRPC server since it's doing
	// some service registration. If it's call later on, the overall application exits.
	blockServer := blockstream.NewServer(gs)

	if a.modules.RegisterGRPCService != nil {
		err := a.modules.RegisterGRPCService(gs)
		if err != nil {
			return fmt.Errorf("register extra grpc service: %w", err)
		}
	}

	err = mindreader.RunGRPCServer(gs, a.Config.GRPCAddr, a.zlogger)
	if err != nil {
		return err
	}

	a.zlogger.Debug("running mindreader log plugin")
	go mindreaderLogPlugin.Launch(blockServer)

	go a.modules.MetricsAndReadinessManager.Launch()

	stdin := bufio.NewReader(os.Stdin)
	go func() {
		a.zlogger.Info("starting stdin reader")
		for {
			in, err := stdin.ReadString('\n')
			if err != nil {
				if err != io.EOF {
					a.zlogger.Error("got an error from readstring", zap.Error(err))
					mindreaderLogPlugin.Close(err)
					return
				}
				if len(in) == 0 {
					a.zlogger.Info("done reading from stdin")
					mindreaderLogPlugin.Close(nil)
					return
				}
				a.zlogger.Debug("got io.EOF on stdin, but still had data to send")
			}

			mindreaderLogPlugin.LogLine(in)
		}
	}()

	return nil
}

func (a *App) OnReady(f func()) {
	a.ReadyFunc = f
}

func (a *App) IsReady() bool {
	return true
}
