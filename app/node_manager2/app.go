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

package nodemanager

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/dfuse-io/bstream/blockstream"
	"github.com/dfuse-io/dgrpc"
	"github.com/dfuse-io/dmetrics"
	nodeManager "github.com/dfuse-io/node-manager"
	"github.com/dfuse-io/node-manager/metrics"
	"github.com/dfuse-io/node-manager/mindreader"
	"github.com/dfuse-io/node-manager/operator"
	"github.com/dfuse-io/shutter"
	"github.com/gorilla/mux"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

type Config struct {
	GRPCAddr string
	HTTPAddr string

	// Backup Flags
	AutoBackupModulo        int
	AutoBackupPeriod        time.Duration
	AutoBackupHostnameMatch string // If non-empty, will only apply autobackup if we have that hostname

	// Snapshot Flags
	AutoSnapshotModulo        int
	AutoSnapshotPeriod        time.Duration
	AutoSnapshotHostnameMatch string // If non-empty, will only apply autosnapshot if we have that hostname

	StartupDelay       time.Duration
	ConnectionWatchdog bool
}

type Modules struct {
	Operator                     *operator.Operator
	MetricsAndReadinessManager   *nodeManager.MetricsAndReadinessManager
	LaunchConnectionWatchdogFunc func(terminating <-chan struct{})
	MindreaderPlugin             *mindreader.MindReaderPlugin
	RegisterGRPCService          func(server *grpc.Server) error
	StartFailureHandlerFunc      func()
}

type App struct {
	*shutter.Shutter
	config  *Config
	modules *Modules
	zlogger *zap.Logger
}

func New(config *Config, modules *Modules, zlogger *zap.Logger) *App {
	return &App{
		Shutter: shutter.New(),
		config:  config,
		modules: modules,
		zlogger: zlogger,
	}
}

func (a *App) Run() error {
	hasMindreader := a.modules.MindreaderPlugin != nil
	a.zlogger.Info("running nodeos manager app", zap.Reflect("config", a.config), zap.Bool("mindreader", hasMindreader))

	hostname, _ := os.Hostname()
	a.zlogger.Info("retrieved hostname from os", zap.String("hostname", hostname))

	dmetrics.Register(metrics.NodeosMetricset)
	dmetrics.Register(metrics.Metricset)

	if a.config.AutoBackupPeriod != 0 || a.config.AutoBackupModulo != 0 {
		a.modules.Operator.ConfigureAutoBackup(a.config.AutoBackupPeriod, a.config.AutoBackupModulo, a.config.AutoBackupHostnameMatch, hostname)
	}

	if a.config.AutoSnapshotPeriod != 0 || a.config.AutoSnapshotModulo != 0 {
		a.modules.Operator.ConfigureAutoSnapshot(a.config.AutoSnapshotPeriod, a.config.AutoSnapshotModulo, a.config.AutoSnapshotHostnameMatch, hostname)
	}

	// FIXME: The ownership relationship of how stuff should close in node-manager is really badly
	//        managed. We can see here that we are trying to wired the shutdown sequence of every
	//        component so all cases work as expected (logs are fully drain in mindreader, all blocks
	//        as been produced, superviser has exited, operator as exited) and we need to ensure
	//        that flow works as expected. We also need to ensure that if a really low level
	//        component like the Mindreader Plugin errors out, the full application tree is correctly
	//        tear-down.
	//
	//        The logical relationship within the code is Operator owns a Superviser that owns
	//        0..N plugins.
	//
	//        When a shutdown happens, we want the following sequence of events to be respected:
	//        - Operator stops accepting new commands and gracefully complete active commands (returning errors for on-going commands might be the safest)
	//        - Superviser stops the process
	//        - (optional) Mindreader plugin consume the remaining standard input of Superviser process
	//        - (optional) Mindreader plugin consume the standard input lines and produce all possible blocks
	//        - (optional) Mindreader plugin closes
	//        - Superviser closes
	//        - Operator closes
	//
	//        I feel that a better relationship behavior between the tree of ownership and correctly crafted
	//        shutdown sequence across tree of ownership would solve all cases and and make the code much
	//        clearer to reason about.

	a.OnTerminating(func(err error) {
		a.modules.Operator.Shutdown(err)
		if hasMindreader {
			<-a.modules.MindreaderPlugin.Terminated()
		}
	})
	a.modules.Operator.OnTerminating(func(err error) {
		// maintenance is set from operator cmd control flow
		a.modules.Operator.Superviser.Shutdown(err)
	})

	a.modules.Operator.Superviser.OnTerminating(func(err error) {
		// FIXME: Not sure why node-manager & node-mindreader were diverging here, most probably to ensure correctly shutdown flow
		if hasMindreader {
			a.modules.MindreaderPlugin.Shutdown(err)
		} else {
			a.modules.Operator.Superviser.Stop()

			a.zlogger.Info("chain operator terminated, shutting down app")
			a.Shutdown(err)
		}
	})

	// FIXME: The node-manager app didn't had this call, probably because invoked in the `a.modules.Operator.Superviser.OnTerminating` above in case of plain node-manager
	if hasMindreader {
		a.modules.MindreaderPlugin.OnTerminated(a.Shutdown)
		a.modules.Operator.OnTerminated(func(err error) {
			a.zlogger.Info("chain operator terminated, shutting down app")
			a.Shutdown(err)
		})
	}

	if a.config.StartupDelay != 0 {
		time.Sleep(a.config.StartupDelay)
	}

	var httpOptions []operator.HTTPOption
	if hasMindreader {
		if err := a.startMindreader(); err != nil {
			return fmt.Errorf("unable to start mindreader: %w", err)
		}

		if a.modules.MindreaderPlugin.HasContinuityChecker() {
			httpOptions = append(httpOptions, func(r *mux.Router) {
				r.HandleFunc("/v1/reset_cc", func(w http.ResponseWriter, _ *http.Request) {
					a.modules.MindreaderPlugin.ResetContinuityChecker()
					w.Write([]byte("ok"))
				})
			})
		}
	}

	a.zlogger.Info("launching operator")
	go a.modules.MetricsAndReadinessManager.Launch()
	go a.Shutdown(a.modules.Operator.Launch(true, a.config.HTTPAddr, httpOptions...))

	if a.config.ConnectionWatchdog {
		go a.modules.LaunchConnectionWatchdogFunc(a.Terminating())
	}

	return nil
}

func (a *App) IsReady() bool {
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	url := fmt.Sprintf("http://%s/healthz", a.config.HTTPAddr)
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

func (a *App) startMindreader() error {
	a.zlogger.Info("starting mindreader gRPC server")
	gs := dgrpc.NewServer(dgrpc.WithLogger(a.zlogger))

	// It's important that this call goes prior running gRPC server since it's doing
	// some service registration. If it's call later on, the overall application exits.
	server := blockstream.NewServer(gs, blockstream.ServerOptionWithLogger(a.zlogger))

	if a.modules.RegisterGRPCService != nil {
		err := a.modules.RegisterGRPCService(gs)
		if err != nil {
			return fmt.Errorf("register extra grpc service: %w", err)
		}
	}

	err := mindreader.RunGRPCServer(gs, a.config.GRPCAddr, a.zlogger)
	if err != nil {
		return err
	}

	a.zlogger.Info("launching mindreader plugin")
	go a.modules.MindreaderPlugin.Launch(server)
	return nil
}
