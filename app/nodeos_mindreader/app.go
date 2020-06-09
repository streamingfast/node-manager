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

	"github.com/dfuse-io/manageos"

	"github.com/dfuse-io/bstream/blockstream"

	logplugin "github.com/dfuse-io/manageos/log_plugin"

	"github.com/dfuse-io/dmetrics"

	"github.com/dfuse-io/dgrpc"
	"github.com/dfuse-io/manageos/metrics"
	"github.com/dfuse-io/manageos/mindreader"
	"github.com/dfuse-io/manageos/operator"
	"github.com/dfuse-io/shutter"
	"go.uber.org/zap"
)

type Config struct {
	NodeosAPIAddress    string
	ManagerAPIAddress   string
	ConnectionWatchdog  bool
	NoBlocksLog         bool
	ReadinessMaxLatency time.Duration
	// Backup Flags
	AutoBackupModulo int
	AutoBackupPeriod time.Duration
	// Snapshot Flags
	AutoSnapshotModulo      int
	AutoSnapshotPeriod      time.Duration
	NumberOfSnapshotsToKeep int
	DebugDeepMind           bool
	GRPCAddr                string
	StartFailureHandlerFunc func()
}

type Modules struct {
	Operator                     *operator.Operator
	MetricsAndReadinessManager   *manageos.MetricsAndReadinessManager
	LogPlugin                    logplugin.LogPlugin
	LaunchConnectionWatchdogFunc func(terminating <-chan struct{})
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

	dmetrics.Register(metrics.NodeosMetricset)
	dmetrics.Register(metrics.Metricset)

	a.modules.Operator.ConfigureAutoBackup(a.Config.AutoBackupPeriod, a.Config.AutoBackupModulo)
	a.modules.Operator.ConfigureAutoSnapshot(a.Config.AutoSnapshotPeriod, a.Config.AutoSnapshotModulo)

	gs := dgrpc.NewServer(dgrpc.WithLogger(zlog))

	//some magic here
	if s, ok := a.modules.LogPlugin.(logplugin.BlockStreamer); ok {
		server := blockstream.NewServer(gs)
		s.Run(server)
	}

	err := mindreader.RunGRPCServer(gs, a.Config.GRPCAddr)
	if err != nil {
		return err
	}

	//TODO: ContinuityChecker
	//TODO: ContinuityChecker
	//TODO: ContinuityChecker
	//TODO: ContinuityChecker
	//TODO: ContinuityChecker

	//if a.Config.FailOnNonContinuousBlocks {
	//	a.modules.Superviser.RegisterPostRestoreHandler(mindreaderLogPlugin.ContinuityChecker.Reset)
	//}

	if p, ok := a.modules.LogPlugin.(logplugin.Shutter); ok {
		a.modules.Operator.OnTerminating(p.Shutdown)
		p.OnTerminated(a.modules.Operator.Shutdown)
	}

	a.OnTerminating(a.modules.Operator.Shutdown)
	a.modules.Operator.OnTerminated(func(err error) {
		zlog.Info("chain operator terminated shutting down mindreader app")
		a.Shutdown(err)
	})

	if a.Config.ConnectionWatchdog {
		go a.modules.LaunchConnectionWatchdogFunc(a.modules.Operator.Terminating())
	}

	startNodeosOnLaunch := true
	var httpOptions []operator.HTTPOption

	//if a.Config.FailOnNonContinuousBlocks {
	//	if mindreaderLogPlugin.ContinuityChecker.IsLocked() {
	//		zlog.Error("continuity checker shows that a hole was previously detected. NOT STARTING PROCESS WITHOUT MANUAL reset_cc or restore")
	//		startNodeosOnLaunch = false
	//	}
	//
	//	httpOptions = append(httpOptions, func(r *mux.Router) {
	//		r.HandleFunc("/v1/reset_cc", func(w http.ResponseWriter, r *http.Request) {
	//			mindreaderLogPlugin.ContinuityChecker.Reset()
	//			w.Write([]byte("ok"))
	//		})
	//	})
	//}

	zlog.Info("launching operator")
	go a.modules.MetricsAndReadinessManager.Launch()
	go a.modules.Operator.Launch(startNodeosOnLaunch, a.Config.ManagerAPIAddress, httpOptions...)

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
