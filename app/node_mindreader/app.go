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

	"go.uber.org/zap"

	dgrpcserver "github.com/streamingfast/dgrpc/server"
	"github.com/streamingfast/dmetrics"
	nodeManager "github.com/streamingfast/node-manager"
	"github.com/streamingfast/node-manager/metrics"
	"github.com/streamingfast/node-manager/operator"
	"github.com/streamingfast/shutter"
)

type Config struct {
	ManagerAPIAddress  string
	ConnectionWatchdog bool

	GRPCAddr string
}

type Modules struct {
	Operator                   *operator.Operator
	MetricsAndReadinessManager *nodeManager.MetricsAndReadinessManager

	LaunchConnectionWatchdogFunc func(terminating <-chan struct{})
	StartFailureHandlerFunc      func()
	DGrpcServer                  dgrpcserver.Server
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
	a.zlogger.Info("launching reader", zap.Reflect("config", a.config))

	hostname, _ := os.Hostname()
	a.zlogger.Info("retrieved hostname from os", zap.String("hostname", hostname))

	a.OnTerminating(func(err error) {
		a.modules.Operator.Shutdown(err)
		<-a.modules.Operator.Terminated()
	})

	dmetrics.Register(metrics.Metricset)
	a.modules.DGrpcServer.OnTerminated(a.Shutdown)

	a.zlogger.Info("about to launch grpc server", zap.String("grpc_addr", a.config.GRPCAddr))
	go a.modules.DGrpcServer.Launch(a.config.GRPCAddr)
	a.zlogger.Info("grpc server launch", zap.String("grpc_addr", a.config.GRPCAddr))

	a.modules.Operator.OnTerminated(func(err error) {
		a.zlogger.Info("chain operator terminated shutting down mindreader app")
		a.Shutdown(err)
	})

	// TODO remove the flag, the watchdog could be part of the operator itself.
	if a.config.ConnectionWatchdog {
		go a.modules.LaunchConnectionWatchdogFunc(a.modules.Operator.Terminating())
	}

	a.zlogger.Info("launching metrics and readinessManager")
	go a.modules.MetricsAndReadinessManager.Launch()

	var httpOptions []operator.HTTPOption
	a.zlogger.Info("launching operator")
	go a.Shutdown(a.modules.Operator.Launch(a.config.ManagerAPIAddress, httpOptions...))

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
