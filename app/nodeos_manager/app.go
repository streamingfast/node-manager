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

package nodeos_manager

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/dfuse-io/dmetrics"
	"github.com/dfuse-io/manageos"

	"github.com/dfuse-io/manageos/metrics"
	"github.com/dfuse-io/manageos/operator"
	"github.com/dfuse-io/shutter"
	"go.uber.org/zap"
)

type Config struct {
	ManagerAPIAddress  string
	ConnectionWatchdog bool

	// Backup Flags
	AutoBackupModulo int
	AutoBackupPeriod time.Duration

	// Snapshot Flags
	AutoSnapshotModulo int
	AutoSnapshotPeriod time.Duration
	DisableProfiler    bool
}

type Modules struct {
	Operator                     *operator.Operator
	MetricsAndReadinessManager   *manageos.MetricsAndReadinessManager
	LaunchConnectionWatchdogFunc func(terminating <-chan struct{})
}

type App struct {
	*shutter.Shutter
	Config  *Config
	Modules *Modules
}

func New(config *Config, modules *Modules) *App {
	return &App{
		Shutter: shutter.New(),
		Config:  config,
		Modules: modules,
	}
}

func (a *App) Run() error {
	zlog.Info("running nodeos manager app", zap.Reflect("config", a.Config))

	hostname, _ := os.Hostname()
	zlog.Info("retrieved hostname from os", zap.String("hostname", hostname))

	dmetrics.Register(metrics.NodeosMetricset)
	dmetrics.Register(metrics.Metricset)

	a.Modules.Operator.ConfigureAutoBackup(a.Config.AutoBackupPeriod, a.Config.AutoBackupModulo)
	a.Modules.Operator.ConfigureAutoSnapshot(a.Config.AutoSnapshotPeriod, a.Config.AutoSnapshotModulo)

	a.OnTerminating(a.Modules.Operator.Shutdown)
	a.Modules.Operator.OnTerminating(a.Shutdown)

	if a.Config.ConnectionWatchdog {
		go a.Modules.LaunchConnectionWatchdogFunc(a.Terminating())
	}

	zlog.Info("launching operator")
	go a.Modules.MetricsAndReadinessManager.Launch()
	go a.Shutdown(a.Modules.Operator.Launch(true, a.Config.ManagerAPIAddress))

	return nil
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
