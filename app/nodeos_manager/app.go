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

	"github.com/streamingfast/shutter"
	"github.com/zhongshuwen/node-manager"

	"github.com/streamingfast/shutter"
	"github.com/zhongshuwen/node-manager/metrics"
	"github.com/zhongshuwen/node-manager/operator"
	"github.com/zhongshuwen/node-manager/profiler"
	"github.com/zhongshuwen/node-manager/superviser/nodeos"
	"go.uber.org/zap"
)

type Config struct {
	MetricID            string
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
	AutoRestoreSource   string // backup, snapshot, none
	RestoreBackupName   string
	RestoreSnapshotName string
	SnapshotStoreURL    string
	ShutdownDelay       time.Duration

	// Backup Flags
	BackupTag               string
	AutoBackupModulo        int
	AutoBackupPeriod        time.Duration
	AutoBackupHostnameMatch string // If non-empty, will only apply autobackup if we have that hostname

	// Snapshot Flags
	AutoSnapshotModulo        int
	AutoSnapshotPeriod        time.Duration
	AutoSnapshotHostnameMatch string // If non-empty, will only apply autosnapshot if we have that hostname
	NumberOfSnapshotsToKeep   int    // do not delete if 0

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

	metricID := a.Config.MetricID
	if metricID == "" {
		metricID = "manager"
	}
	zlog.Info("setting up metrics", zap.String("metric_id", metricID))
	headBlockTimeDrift := metrics.NewHeadBlockTimeDrift(metricID)
	headBlockNumber := metrics.NewHeadBlockNumber(metricID)

	metricsAndReadinessManager := manageos.NewMetricsAndReadinessManager(headBlockTimeDrift, headBlockNumber, a.Config.ReadinessMaxLatency)

	chainSuperviser, err := nodeos.NewSuperviser(zlog, zlogNodeos, a.Config.DebugDeepMind, metricsAndReadinessManager.UpdateHeadBlock, &nodeos.SuperviserOptions{
		LocalNodeEndpoint: a.Config.NodeosAPIAddress,
		ConfigDir:         a.Config.NodeosConfigDir,
		BinPath:           a.Config.NodeosBinPath,
		DataDir:           a.Config.NodeosDataDir,
		Hostname:          hostname,
		ProducerHostname:  a.Config.ProducerHostname,
		TrustedProducer:   a.Config.TrustedProducer,
		AdditionalArgs:    a.Config.NodeosExtraArgs,
		ForceProduction:   a.Config.ForceProduction,
		LogToZap:          a.Config.LogToZap,
	})
	if err != nil {
		return fmt.Errorf("unable to create nodeos chain superviser: %w", err)
	}

	dmetrics.Register(metrics.NodeosMetricset)
	dmetrics.Register(metrics.Metricset)

	var p *profiler.Profiler
	if !a.Config.DisableProfiler {
		p = profiler.MaybeNew()
	}

	chainOperator, err := operator.New(zlog, chainSuperviser, metricsAndReadinessManager, &operator.Options{
		BootstrapDataURL:           a.Config.BootstrapDataURL,
		BackupTag:                  a.Config.BackupTag,
		BackupStoreURL:             a.Config.BackupStoreURL,
		AutoRestoreSource:          a.Config.AutoRestoreSource,
		ShutdownDelay:              a.Config.ShutdownDelay,
		RestoreBackupName:          a.Config.RestoreBackupName,
		RestoreSnapshotName:        a.Config.RestoreSnapshotName,
		SnapshotStoreURL:           a.Config.SnapshotStoreURL,
		StartFailureHandlerFunc:    a.Config.StartFailureHandlerFunc,
		NumberOfSnapshotsToKeep:    a.Config.NumberOfSnapshotsToKeep,
		EnableSupervisorMonitoring: true,
		Profiler:                   p,
		ReadyFunc:                  a.ReadyFunc,
	})

	if err != nil {
		return fmt.Errorf("unable to create chain operator: %w", err)
	}

	chainOperator.ConfigureAutoBackup(a.Config.AutoBackupPeriod, a.Config.AutoBackupModulo, a.Config.AutoBackupHostnameMatch, hostname)
	chainOperator.ConfigureAutoSnapshot(a.Config.AutoSnapshotPeriod, a.Config.AutoSnapshotModulo, a.Config.AutoSnapshotHostnameMatch, hostname)

	a.OnTerminating(chainOperator.Shutdown)
	chainOperator.OnTerminating(a.Shutdown)

	if a.Config.ConnectionWatchdog {
		go chainSuperviser.LaunchConnectionWatchdog(a.Terminating())
	}

	zlog.Info("launching operator")
	go metricsAndReadinessManager.Launch()
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
