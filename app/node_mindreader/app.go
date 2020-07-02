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

package node_mindreader

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"time"

	"google.golang.org/grpc"

	"github.com/dfuse-io/dmetrics"
	nodeManager "github.com/dfuse-io/node-manager"
	logplugin "github.com/dfuse-io/node-manager/log_plugin"
	"github.com/dfuse-io/node-manager/metrics"
	"github.com/dfuse-io/node-manager/mindreader"
	"github.com/dfuse-io/node-manager/operator"
	"github.com/dfuse-io/shutter"
	"github.com/gorilla/mux"
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
	GRPCAddr                string
	StartFailureHandlerFunc func()
}

type Modules struct {
	Operator                     *operator.Operator
	MetricsAndReadinessManager   *nodeManager.MetricsAndReadinessManager
	LogPlugin                    logplugin.LogPlugin
	ContinuityChecker            mindreader.ContinuityChecker
	LaunchConnectionWatchdogFunc func(terminating <-chan struct{})
	GRPCServer                   *grpc.Server
}

type App struct {
	*shutter.Shutter
	Config  *Config
	modules *Modules
	zlogger *zap.Logger
}

func New(c *Config, modules *Modules, zlogger *zap.Logger) *App {
	n := &App{
		Shutter: shutter.New(),
		Config:  c,
		modules: modules,
		zlogger: zlogger,
	}
	return n
}

func (a *App) Run() error {
	a.zlogger.Info("launching nodeos mindreader", zap.Reflect("config", a.Config))

	hostname, _ := os.Hostname()
	a.zlogger.Info("retrieved hostname from os", zap.String("hostname", hostname))

	dmetrics.Register(metrics.NodeosMetricset)
	dmetrics.Register(metrics.Metricset)

	a.modules.Operator.ConfigureAutoBackup(a.Config.AutoBackupPeriod, a.Config.AutoBackupModulo)
	a.modules.Operator.ConfigureAutoSnapshot(a.Config.AutoSnapshotPeriod, a.Config.AutoSnapshotModulo)

	err := mindreader.RunGRPCServer(a.modules.GRPCServer, a.Config.GRPCAddr, a.zlogger)
	if err != nil {
		return err
	}

	if p, ok := a.modules.LogPlugin.(logplugin.Shutter); ok {
		a.modules.Operator.OnTerminating(p.Shutdown)
		p.OnTerminated(a.modules.Operator.Shutdown)
	}

	a.OnTerminating(a.modules.Operator.Shutdown)
	a.modules.Operator.OnTerminated(func(err error) {
		a.zlogger.Info("chain operator terminated shutting down mindreader app")
		a.Shutdown(err)
	})

	if a.Config.ConnectionWatchdog {
		go a.modules.LaunchConnectionWatchdogFunc(a.modules.Operator.Terminating())
	}

	startNodeosOnLaunch := true
	var httpOptions []operator.HTTPOption

	if a.modules.ContinuityChecker != nil {

		httpOptions = append(httpOptions, func(r *mux.Router) {
			r.HandleFunc("/v1/reset_cc", func(w http.ResponseWriter, r *http.Request) {
				a.modules.ContinuityChecker.Reset()
				w.Write([]byte("ok"))
			})
		})
	}

	a.zlogger.Info("launching operator")
	go a.modules.MetricsAndReadinessManager.Launch()
	go a.modules.Operator.Launch(startNodeosOnLaunch, a.Config.ManagerAPIAddress, httpOptions...)

	return nil
}

func (a *App) IsReady() bool {
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	url := fmt.Sprintf("http://%s/healthz", a.Config.ManagerAPIAddress)
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		a.zlogger.Warn("unable to build get health request", zap.Error(err))
		return false
	}

	client := http.DefaultClient
	res, err := client.Do(req)
	if err != nil {
		a.zlogger.Debug("unable to execute get health request", zap.Error(err))
		return false
	}

	return res.StatusCode == 200
}
