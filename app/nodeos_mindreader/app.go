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

package nodeos_mindreader

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/dfuse-io/dgrpc"
	"github.com/dfuse-io/manageos/mindreader"
	nodeosMindreader "github.com/dfuse-io/manageos/mindreader/nodeos"
	"github.com/dfuse-io/manageos/operator"
	"github.com/dfuse-io/manageos/profiler"
	"github.com/dfuse-io/manageos/superviser/nodeos"
	"github.com/dfuse-io/shutter"
	"github.com/gorilla/mux"
	"go.uber.org/zap"
)

type Config struct {
	ManagerAPIAddress   string
	NodeosAPIAddress    string
	ConnectionWatchdog  bool
	NodeosConfigDir     string
	NodeosBinPath       string
	NodeosDataDir       string
	ProducerHostname    string
	TrustedProducer     string
	ReadinessMaxLatency time.Duration

	NodeosExtraArgs []string

	// Common Flags
	BackupStoreURL      string
	BackupTag           string
	BootstrapDataURL    string
	DebugDeepMind       bool
	LogToZap            bool
	AutoRestoreLatest   bool
	RestoreBackupName   string
	RestoreSnapshotName string
	SnapshotStoreURL    string
	ShutdownDelay       time.Duration

	ArchiveStoreURL            string
	MergeUploadDirectly        bool
	GRPCAddr                   string
	StartBlockNum              uint64
	StopBlockNum               uint64
	MindReadBlocksChanCapacity int
	WorkingDir                 string

	DisableProfiler         bool
	StartFailureHandlerFunc func()
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
}

func New(c *Config, modules *Modules) *App {
	n := &App{
		Shutter:   shutter.New(),
		Config:    c,
		ReadyFunc: func() {},
		modules:   modules,
	}
	return n
}

func (a *App) Run() error {
	zlog.Info("launching nodeos mindreader", zap.Reflect("config", a.Config))

	hostname, _ := os.Hostname()
	zlog.Info("retrieved hostname from os", zap.String("hostname", hostname))

	chainSuperviser, err := nodeos.NewSuperviser(zlog, a.Config.DebugDeepMind, &nodeos.SuperviserOptions{
		LocalNodeEndpoint:   a.Config.NodeosAPIAddress,
		ConfigDir:           a.Config.NodeosConfigDir,
		BinPath:             a.Config.NodeosBinPath,
		DataDir:             a.Config.NodeosDataDir,
		Hostname:            hostname,
		ProducerHostname:    a.Config.ProducerHostname,
		TrustedProducer:     a.Config.TrustedProducer,
		ReadinessMaxLatency: a.Config.ReadinessMaxLatency,
		AdditionalArgs:      a.Config.NodeosExtraArgs,
		LogToZap:            a.Config.LogToZap,
		MonitorHeadBlock:    false,
	})
	if err != nil {
		return fmt.Errorf("unable to create nodeos chain superviser: %w", err)
	}
	if a.Config.StartFailureHandlerFunc != nil {
		chainSuperviser.Superviser.RegisterStartFailureHandler(a.Config.StartFailureHandlerFunc)
	}

	var p *profiler.Profiler
	if !a.Config.DisableProfiler {
		p = profiler.MaybeNew()
	}

	chainOperator, err := operator.New(zlog, chainSuperviser, &operator.Options{
		BootstrapDataURL:    a.Config.BootstrapDataURL,
		BackupTag:           a.Config.BackupTag,
		BackupStoreURL:      a.Config.BackupStoreURL,
		AutoRestoreLatest:   a.Config.AutoRestoreLatest,
		ShutdownDelay:       a.Config.ShutdownDelay,
		RestoreBackupName:   a.Config.RestoreBackupName,
		RestoreSnapshotName: a.Config.RestoreSnapshotName,
		SnapshotStoreURL:    a.Config.SnapshotStoreURL,
		Profiler:            p,
		ReadyFunc:           a.ReadyFunc,
	})
	if err != nil {
		return fmt.Errorf("unable to create chain operator: %w", err)
	}

	gs := dgrpc.NewServer(dgrpc.WithLogger(zlog))

	zlog.Info("launching mindreader plugin")
	mindreaderLogPlugin, err := mindreader.RunMindReaderPlugin(
		a.Config.ArchiveStoreURL,
		a.Config.MergeUploadDirectly,
		a.Config.WorkingDir,
		nodeosMindreader.BlockFileNamer,
		a.modules.ConsoleReaderFactory,
		a.modules.ConsoleReaderTransformer,
		gs,
		a.Config.StartBlockNum,
		a.Config.StopBlockNum,
		a.Config.MindReadBlocksChanCapacity,
		chainOperator.SetMaintenance,
	)
	if err != nil {
		return err
	}

	err = mindreader.RunGRPCServer(gs, a.Config.GRPCAddr)
	if err != nil {
		return err
	}

	chainSuperviser.RegisterLogPlugin(mindreaderLogPlugin)
	chainSuperviser.RegisterPostRestoreHandler(mindreaderLogPlugin.ContinuityChecker.Reset)

	chainOperator.OnTerminating(mindreaderLogPlugin.Shutdown)
	mindreaderLogPlugin.OnTerminated(chainOperator.Shutdown)

	a.OnTerminating(chainOperator.Shutdown)
	chainOperator.OnTerminated(func(err error) {
		zlog.Info("chain operator terminated shutting down mindreader app")
		a.Shutdown(err)
	})

	if a.Config.ConnectionWatchdog {
		go chainSuperviser.LaunchConnectionWatchdog(chainOperator.Terminating())
	}

	startNodeosOnLaunch := !mindreaderLogPlugin.ContinuityChecker.IsLocked()
	httpOptions := []operator.HTTPOption{
		func(r *mux.Router) {
			r.HandleFunc("/v1/reset_cc", func(w http.ResponseWriter, r *http.Request) {
				mindreaderLogPlugin.ContinuityChecker.Reset()
				w.Write([]byte("ok"))
			})
		},
	}

	go chainOperator.Launch(startNodeosOnLaunch, a.Config.ManagerAPIAddress, httpOptions...)

	return nil
}

func (a *App) OnReady(f func()) {
	a.ReadyFunc = f
}

func (a *App) IsReady() bool {
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	url := fmt.Sprintf("http://%s/healthz", a.Config.ManagerAPIAddress)
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		zlog.Warn("unable to build get health request", zap.Error(err))
		return false
	}

	client := http.DefaultClient
	res, err := client.Do(req)
	if err != nil {
		zlog.Debug("unable to execute get health request", zap.Error(err))
		return false
	}

	return res.StatusCode == 200
}
