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

package node_manager

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/streamingfast/dmetrics"
	nodeManager "github.com/streamingfast/node-manager"
	"github.com/streamingfast/node-manager/metrics"
	"github.com/streamingfast/node-manager/operator"
	"github.com/streamingfast/shutter"
	"go.uber.org/zap"
)

type Config struct {
	ManagerAPIAddress string
	StartupDelay      time.Duration
}

type Modules struct {
	Operator                   *operator.Operator
	MetricsAndReadinessManager *nodeManager.MetricsAndReadinessManager
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
	a.zlogger.Info("running nodeos manager app", zap.Reflect("config", a.config))

	dmetrics.Register(metrics.Metricset)

	a.OnTerminating(func(err error) {
		a.modules.Operator.Shutdown(err)
		<-a.modules.Operator.Terminated()
	})

	a.modules.Operator.OnTerminated(func(err error) {
		a.zlogger.Info("chain operator terminated shutting down mindreader app")
		a.Shutdown(err)
	})

	if a.config.StartupDelay != 0 {
		time.Sleep(a.config.StartupDelay)
	}

	a.zlogger.Info("launching operator")
	go a.modules.MetricsAndReadinessManager.Launch()
	go a.Shutdown(a.modules.Operator.Launch(a.config.ManagerAPIAddress))

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
