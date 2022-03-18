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

	"github.com/streamingfast/dmetrics"
	manageos "github.com/zhongshuwen/node-manager"

	"github.com/dfuse-io/dgrpc"
	"github.com/gorilla/mux"
	"github.com/streamingfast/shutter"
	"github.com/zhongshuwen/node-manager/metrics"
	"github.com/zhongshuwen/node-manager/mindreader"
	nodeosMindreader "github.com/zhongshuwen/node-manager/mindreader/nodeos"
	"github.com/zhongshuwen/node-manager/operator"
	"github.com/zhongshuwen/node-manager/profiler"
	"github.com/zhongshuwen/node-manager/superviser/nodeos"
	"go.uber.org/zap"
)

type Config struct {
	MetricID                  string
	ManagerAPIAddress         string
	NodeosAPIAddress          string
	ConnectionWatchdog        bool
	NodeosConfigDir           string
	NodeosBinPath             string
	NodeosDataDir             string
	NoBlocksLog               bool
	ProducerHostname          string
	TrustedProducer           string
	ReadinessMaxLatency       time.Duration
	FailOnNonContinuousBlocks bool // Will enable the ContinuityChecker, which stops nodeos if a block was produced with a gap, to prevent a restart from going over problem blocks

	NodeosExtraArgs []string

	// Common Flags
	BackupStoreURL string

	// Backup Flags
	BackupTag               string
	AutoBackupModulo        int
	AutoBackupPeriod        time.Duration
	AutoBackupHostnameMatch string // If non-empty, will only apply autobackup if we have that hostname

	// Snapshot Flags
	AutoSnapshotModulo        int
	AutoSnapshotPeriod        time.Duration
	NumberOfSnapshotsToKeep   int
	AutoSnapshotHostnameMatch string // If non-empty, will only apply autosnapshot if we have that hostname

	BootstrapDataURL    string
	DebugDeepMind       bool
	LogToZap            bool
	AutoRestoreSource   string
	RestoreBackupName   string
	RestoreSnapshotName string
	SnapshotStoreURL    string
	ShutdownDelay       time.Duration

	ArchiveStoreURL            string
	MergeArchiveStoreURL       string
	MergeUploadDirectly        bool
	GRPCAddr                   string
	StartBlockNum              uint64
	StopBlockNum               uint64
	DiscardAfterStopBlock      bool
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

	metricID := a.Config.MetricID
	if metricID == "" {
		metricID = "manager"
	}
	zlog.Info("setting up metrics", zap.String("metric_id", metricID))

	headBlockTimeDrift := metrics.NewHeadBlockTimeDrift(metricID)
	headBlockNumber := metrics.NewHeadBlockNumber(metricID)

	metricsAndReadinessManager := manageos.NewMetricsAndReadinessManager(headBlockTimeDrift, headBlockNumber, a.Config.ReadinessMaxLatency)

	chainSuperviser, err := nodeos.NewSuperviser(zlog, zlogNodeos, a.Config.DebugDeepMind, nil, &nodeos.SuperviserOptions{
		LocalNodeEndpoint: a.Config.NodeosAPIAddress,
		ConfigDir:         a.Config.NodeosConfigDir,
		BinPath:           a.Config.NodeosBinPath,
		DataDir:           a.Config.NodeosDataDir,
		Hostname:          hostname,
		NoBlocksLog:       a.Config.NoBlocksLog,
		ProducerHostname:  a.Config.ProducerHostname,
		TrustedProducer:   a.Config.TrustedProducer,
		AdditionalArgs:    a.Config.NodeosExtraArgs,
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
		NumberOfSnapshotsToKeep:    a.Config.NumberOfSnapshotsToKeep,
		EnableSupervisorMonitoring: false,
		Profiler:                   p,
		ReadyFunc:                  a.ReadyFunc,
	})
	//if a.Config.StartFailureHandlerFunc != nil {
	//	chainOperator.RegisterStartFailureHandler(a.Config.StartFailureHandlerFunc)
	//}

	if err != nil {
		return fmt.Errorf("unable to create chain operator: %w", err)
	}

	chainOperator.ConfigureAutoBackup(a.Config.AutoBackupPeriod, a.Config.AutoBackupModulo, a.Config.AutoBackupHostnameMatch, hostname)
	chainOperator.ConfigureAutoSnapshot(a.Config.AutoSnapshotPeriod, a.Config.AutoSnapshotModulo, a.Config.AutoSnapshotHostnameMatch, hostname)

	gs := dgrpc.NewServer(dgrpc.WithLogger(zlog))

	zlog.Info("launching mindreader plugin")
	mindreaderLogPlugin, err := mindreader.RunMindReaderPlugin(
		a.Config.ArchiveStoreURL,
		a.Config.MergeArchiveStoreURL,
		a.Config.MergeUploadDirectly,
		a.Config.DiscardAfterStopBlock,
		a.Config.WorkingDir,
		nodeosMindreader.BlockFileNamer,
		a.modules.ConsoleReaderFactory,
		a.modules.ConsoleReaderTransformer,
		gs,
		a.Config.StartBlockNum,
		a.Config.StopBlockNum,
		a.Config.MindReadBlocksChanCapacity,
		metricsAndReadinessManager.UpdateHeadBlock,
		chainOperator.SetMaintenance,
		func() {
			chainOperator.Shutdown(nil)
		},
		a.Config.FailOnNonContinuousBlocks,
	)
	if err != nil {
		return err
	}

	err = mindreader.RunGRPCServer(gs, a.Config.GRPCAddr)
	if err != nil {
		return err
	}

	chainSuperviser.RegisterLogPlugin(mindreaderLogPlugin)
	if a.Config.FailOnNonContinuousBlocks {
		chainSuperviser.RegisterPostRestoreHandler(mindreaderLogPlugin.ContinuityChecker.Reset)
	}

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

	startNodeosOnLaunch := true
	var httpOptions []operator.HTTPOption

	if a.Config.FailOnNonContinuousBlocks {
		if mindreaderLogPlugin.ContinuityChecker.IsLocked() {
			zlog.Error("continuity checker shows that a hole was previously detected. NOT STARTING PROCESS WITHOUT MANUAL reset_cc or restore")
			startNodeosOnLaunch = false
		}

		httpOptions = append(httpOptions, func(r *mux.Router) {
			r.HandleFunc("/v1/reset_cc", func(w http.ResponseWriter, r *http.Request) {
				mindreaderLogPlugin.ContinuityChecker.Reset()
				w.Write([]byte("ok"))
			})
		})
	}

	zlog.Info("launching operator")
	go metricsAndReadinessManager.Launch()
	go a.Shutdown(chainOperator.Launch(startNodeosOnLaunch, a.Config.ManagerAPIAddress, httpOptions...))

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
