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

	"github.com/dfuse-io/dmetrics"
	"github.com/dfuse-io/manageos"

	"github.com/dfuse-io/dgrpc"
	"github.com/dfuse-io/manageos/metrics"
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
	MetricID            string
	ManagerAPIAddress   string
	NodeosAPIAddress    string
	ConnectionWatchdog  bool
	NodeosConfigDir     string
	NodeosBinPath       string
	NodeosDataDir       string
	NoBlocksLog         bool
	ProducerHostname    string
	TrustedProducer     string
	ReadinessMaxLatency time.Duration

	NodeosExtraArgs []string

	// Common Flags
	BackupStoreURL string

	// Backup Flags
	BackupTag        string
	AutoBackupModulo int
	AutoBackupPeriod time.Duration

	// Snapshot Flags
	AutoSnapshotModulo      int
	AutoSnapshotPeriod      time.Duration
	NumberOfSnapshotsToKeep int

	BootstrapDataURL    string
	DebugDeepMind       bool
	LogToZap            bool
	AutoRestoreSource   string
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

	chainOperator.ConfigureAutoBackup(a.Config.AutoBackupPeriod, a.Config.AutoBackupModulo)
	chainOperator.ConfigureAutoSnapshot(a.Config.AutoSnapshotPeriod, a.Config.AutoSnapshotModulo)

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
		metricsAndReadinessManager.UpdateHeadBlock,
		chainOperator.SetMaintenance,
		func() {
			chainOperator.Shutdown(nil)
		},
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

	zlog.Info("launching operator")
	go metricsAndReadinessManager.Launch()
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
