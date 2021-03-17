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

	"github.com/dfuse-io/bstream/blockstream"

	"google.golang.org/grpc"

	"github.com/dfuse-io/dmetrics"
	nodeManager "github.com/dfuse-io/node-manager"
	"github.com/dfuse-io/node-manager/metrics"
	"github.com/dfuse-io/node-manager/mindreader"
	"github.com/dfuse-io/node-manager/operator"
	"github.com/dfuse-io/shutter"
	"go.uber.org/zap"
)

type Config struct {
	ManagerAPIAddress  string
	ConnectionWatchdog bool

	// Backup Flags
	AutoBackupModulo        int
	AutoBackupPeriod        time.Duration
	AutoBackupHostnameMatch string // If non-empty, will only apply autobackup if we have that hostname

	// Snapshot Flags
	AutoSnapshotModulo        int
	AutoSnapshotPeriod        time.Duration
	AutoSnapshotHostnameMatch string // If non-empty, will only apply autosnapshot if we have that hostname

	GRPCAddr string
}

type Modules struct {
	Operator                     *operator.Operator
	MetricsAndReadinessManager   *nodeManager.MetricsAndReadinessManager
	MindreaderPlugin             *mindreader.MindReaderPlugin
	LaunchConnectionWatchdogFunc func(terminating <-chan struct{})
	StartFailureHandlerFunc      func()
	//Done: move to dfuse
	//RegisterGRPCService          func(server *grpc.Server) error
	GrpcServer *grpc.Server
}

type App struct {
	*shutter.Shutter
	config  *Config
	modules *Modules
	zlogger *zap.Logger
}

func New(c *Config, modules *Modules, zlogger *zap.Logger) *App {
	n := &App{
		Shutter: shutter.New(),
		config:  c,
		modules: modules,
		zlogger: zlogger,
	}
	return n
}

func (a *App) Run() error {
	a.zlogger.Info("launching nodeos mindreader", zap.Reflect("config", a.config))

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

	//Done: move to dfuse
	//gs := dgrpc.NewServer(dgrpc.WithLogger(a.zlogger))

	// It's important that this call goes prior running gRPC server since it's doing
	// some service registration. If it's call later on, the overall application exits.
	blockstream.NewServer(a.modules.GrpcServer, blockstream.ServerOptionWithLogger(a.zlogger))

	err := mindreader.RunGRPCServer(a.modules.GrpcServer, a.config.GRPCAddr, a.zlogger)
	if err != nil {
		return err
	}

	a.OnTerminating(func(err error) {
		a.modules.Operator.Shutdown(err)

		//done: wait for the operator to terminate
		//done: operator should wait for the supervisor to terminate
		//Done: supervisor should wait for the plugins to terminate

		//wait for Operator to terminate, Operator will wait for supervisor, supervisor will wait for plugins to terminate
		<-a.modules.Operator.Terminated()
	})

	//Done: let's move this code to operator
	//a.modules.Operator.OnTerminating(func(err error) {
	//	// maintenance is set from operator cmd control flow
	//	a.modules.Operator.Superviser.Shutdown(err)
	//})

	//Done: move to superviser
	//a.modules.Operator.Superviser.OnTerminating(func(err error) {
	//
	//	a.modules.MindreaderPlugin.Shutdown(err)
	//})

	//Done: plugins should shutdown supervisor
	//Done: supervisor should shutdown operator
	//Done: operator should shutdown app
	//Done: remove next line
	//a.modules.MindreaderPlugin.OnTerminated(a.Shutdown)

	a.modules.Operator.OnTerminated(func(err error) {
		a.zlogger.Info("chain operator terminated shutting down mindreader app")
		a.Shutdown(err)
	})

	if a.config.ConnectionWatchdog {
		go a.modules.LaunchConnectionWatchdogFunc(a.modules.Operator.Terminating())
	}

	a.zlogger.Info("launching mindreader plugin")
	//Done: move to dfuse
	//go a.modules.MindreaderPlugin.Launch()

	a.zlogger.Info("launching operator")
	go a.modules.MetricsAndReadinessManager.Launch()

	var httpOptions []operator.HTTPOption
	go a.Shutdown(a.modules.Operator.Launch(true, a.config.ManagerAPIAddress, httpOptions...))

	return nil
}

func (a *App) IsReady() bool {
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	url := fmt.Sprintf("http://%s/healthz", a.config.ManagerAPIAddress)
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
