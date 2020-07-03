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
	"github.com/dfuse-io/bstream/blockstream"
	"io"
	"os"
	"time"

	"github.com/dfuse-io/dgrpc"
	"github.com/dfuse-io/node-manager/mindreader"
	"github.com/dfuse-io/shutter"
	"go.uber.org/zap"
)

type Config struct {
	ArchiveStoreURL            string
	MergeArchiveStoreURL       string
	MergeUploadDirectly        bool
	MindReadBlocksChanCapacity int
	GRPCAddr                   string
	WorkingDir                 string
	DisableProfiler            bool
	BlockFileNamerFunc mindreader.BlockFileNamer
}

type Modules struct {
	ConsoleReaderFactory     mindreader.ConsolerReaderFactory
	ConsoleReaderTransformer mindreader.ConsoleReaderBlockTransformer
}

type App struct {
	*shutter.Shutter
	Config    *Config
	ReadyFunc func()
	modules   *Modules
	zlogger *zap.Logger
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
	mindreaderLogPlugin, err := mindreader.RunMindReaderPlugin(
		a.Config.ArchiveStoreURL,
		a.Config.MergeArchiveStoreURL,
		a.Config.MergeUploadDirectly,
		false,
		a.Config.WorkingDir,
		a.Config.BlockFileNamerFunc,
		a.modules.ConsoleReaderFactory,
		a.modules.ConsoleReaderTransformer,
		gs,
		0,
		0,
		a.Config.MindReadBlocksChanCapacity,
		func(uint64, string, time.Time) {},
		func() {},
		func() {},
		false,
		a.zlogger,
	)
	if err != nil {
		return err
	}

	err = mindreader.RunGRPCServer(gs, a.Config.GRPCAddr, a.zlogger)
	if err != nil {
		return err
	}
	mindreaderLogPlugin.OnTerminated(a.Shutdown)
	a.OnTerminating(mindreaderLogPlugin.Shutdown)

	blockServer := blockstream.NewServer(gs)
	go mindreaderLogPlugin.Run(blockServer)

	reader := bufio.NewReader(os.Stdin)
	go func() {
		for {
			in, err := reader.ReadString('\n')
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
