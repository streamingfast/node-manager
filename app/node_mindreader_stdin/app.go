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

	dgrpcserver "github.com/streamingfast/dgrpc/server"
	dgrpcfactory "github.com/streamingfast/dgrpc/server/factory"
	"github.com/streamingfast/logging"
	nodeManager "github.com/streamingfast/node-manager"
	logplugin "github.com/streamingfast/node-manager/log_plugin"
	"github.com/streamingfast/node-manager/mindreader"
	"github.com/streamingfast/shutter"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

type Config struct {
	GRPCAddr                   string
	OneBlocksStoreURL          string
	OneBlockSuffix             string
	MindReadBlocksChanCapacity int
	StartBlockNum              uint64
	StopBlockNum               uint64
	WorkingDir                 string
	LogToZap                   bool
	DebugDeepMind              bool
}

type Modules struct {
	ConsoleReaderFactory       mindreader.ConsolerReaderFactory
	MetricsAndReadinessManager *nodeManager.MetricsAndReadinessManager
	RegisterGRPCService        func(server grpc.ServiceRegistrar) error
}

type App struct {
	*shutter.Shutter
	Config    *Config
	ReadyFunc func()
	modules   *Modules
	zlogger   *zap.Logger
	tracer    logging.Tracer
}

func New(c *Config, modules *Modules, zlogger *zap.Logger, tracer logging.Tracer) *App {
	n := &App{
		Shutter:   shutter.New(),
		Config:    c,
		ReadyFunc: func() {},
		modules:   modules,
		zlogger:   zlogger,
		tracer:    tracer,
	}
	return n
}

func (a *App) Run() error {
	a.zlogger.Info("launching nodeos mindreader-stdin", zap.Reflect("config", a.Config))

	gs := dgrpcfactory.ServerFromOptions(dgrpcserver.WithLogger(a.zlogger))

	a.zlogger.Info("launching mindreader plugin")
	mindreaderLogPlugin, err := mindreader.NewMindReaderPlugin(
		a.Config.OneBlocksStoreURL,
		a.Config.WorkingDir,
		a.modules.ConsoleReaderFactory,
		a.Config.StartBlockNum,
		a.Config.StopBlockNum,
		a.Config.MindReadBlocksChanCapacity,
		a.modules.MetricsAndReadinessManager.UpdateHeadBlock,
		func(_ error) {},
		a.Config.OneBlockSuffix,
		nil,
		a.zlogger,
		a.tracer,
	)
	if err != nil {
		return err
	}

	a.zlogger.Debug("configuring shutter")
	mindreaderLogPlugin.OnTerminated(a.Shutdown)
	a.OnTerminating(mindreaderLogPlugin.Shutdown)

	if a.modules.RegisterGRPCService != nil {
		err := a.modules.RegisterGRPCService(gs.ServiceRegistrar())
		if err != nil {
			return fmt.Errorf("register extra grpc service: %w", err)
		}
	}
	gs.OnTerminated(a.Shutdown)
	go gs.Launch(a.Config.GRPCAddr)

	a.zlogger.Debug("running mindreader log plugin")
	mindreaderLogPlugin.Launch()
	go a.modules.MetricsAndReadinessManager.Launch()

	var logPlugin *logplugin.ToZapLogPlugin
	if a.Config.LogToZap {
		logPlugin = logplugin.NewToZapLogPlugin(a.Config.DebugDeepMind, a.zlogger)
	}

	stdin := bufio.NewReaderSize(os.Stdin, 50*1024*1024)
	go func() {
		a.zlogger.Info("starting stdin reader")
		for {
			in, _, err := stdin.ReadLine()
			if err != nil {
				if err != io.EOF {
					a.zlogger.Error("got an error from readstring", zap.Error(err))
					mindreaderLogPlugin.Shutdown(err)
					return
				}
				if len(in) == 0 {
					a.zlogger.Info("done reading from stdin")
					return
				}
				a.zlogger.Debug("got io.EOF on stdin, but still had data to send")
			}
			line := string(in)

			if logPlugin != nil {
				logPlugin.LogLine(line)
			}

			mindreaderLogPlugin.LogLine(line)
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
