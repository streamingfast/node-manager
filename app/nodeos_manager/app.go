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

	"github.com/dfuse-io/shutter"
	"github.com/dfuse-io/manageos/operator"
	"github.com/dfuse-io/manageos/profiler"
	"github.com/dfuse-io/manageos/superviser/nodeos"
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
	ForceProduction     bool

	NodeosExtraArgs []string

	// Common Flags
	BackupStoreURL      string
	BootstrapDataURL    string
	DebugDeepMind       bool
	LogToZap            bool
	AutoRestoreLatest   bool
	RestoreBackupName   string
	RestoreSnapshotName string
	SnapshotStoreURL    string
	ShutdownDelay       time.Duration

	// Backup Flags
	BackupTag        string
	AutoBackupModulo int
	AutoBackupPeriod time.Duration

	// Snapshot Flags
	AutoSnapshotModulo int
	AutoSnapshotPeriod time.Duration

	StartFailureHandlerFunc func()

	DisableProfiler bool
}

type App struct {
	*shutter.Shutter
	Config    *Config
	ReadyFunc func()
}

func New(config *Config) *App {
	return &App{
		Shutter:   shutter.New(),
		Config:    config,
		ReadyFunc: func() {},
	}
}

func (a *App) Run() error {
	zlog.Info("running nodeos manager app", zap.Reflect("config", a.Config))

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
		ForceProduction:     a.Config.ForceProduction,
		LogToZap:            a.Config.LogToZap,
		MonitorHeadBlock:    true,
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

	chainOperator.ConfigureAutoBackup(a.Config.AutoBackupPeriod, a.Config.AutoBackupModulo)
	chainOperator.ConfigureAutoSnapshot(a.Config.AutoSnapshotPeriod, a.Config.AutoSnapshotModulo)

	a.OnTerminating(chainOperator.Shutdown)
	chainOperator.OnTerminating(a.Shutdown)

	if a.Config.ConnectionWatchdog {
		go chainSuperviser.LaunchConnectionWatchdog(a.Terminating())
	}

	zlog.Info("launching operator")
	go a.Shutdown(chainOperator.Launch(true, a.Config.ManagerAPIAddress))

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