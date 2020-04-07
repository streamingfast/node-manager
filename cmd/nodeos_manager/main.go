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

package main

import (
	"flag"
	"fmt"
	"net/http"
	_ "net/http/pprof"
	"time"

	"github.com/dfuse-io/derr"
	"github.com/dfuse-io/manageos"
	nodeosManagerApp "github.com/dfuse-io/manageos/app/nodeos_manager"
	"github.com/dfuse-io/manageos/cli"
	"github.com/dfuse-io/manageos/metrics"
	"github.com/spf13/viper"
	"go.uber.org/zap"
)

var flagManagerAPIAddress = flag.String("manager-api-addr", ":8080", "eos-manager API address")
var flagNodeosAPIAddress = flag.String("api-addr", "http://localhost:8888", "Target API address")
var flagConnectionWatchdog = flag.Bool("connection-watchdog", true, "Force-reconnect dead peers automatically")
var flagNodeosConfigDir = flag.String("config-dir", "/etc/nodeos", "Directory for config files")
var flagNodeosBinPath = flag.String("nodeos-path", "/opt/eosio/bin/nodeos", "Path to nodeos")
var flagNodeosDataDir = flag.String("data-dir", "/nodeos-data", "Directory for data (blocks)")
var flagProducerHostname = flag.String("producer-hostname", "", "Hostname that will produce block (other will be paused)")
var flagTrustedProducer = flag.String("trusted-producer", "", "The EOS account name of the Block Producer we trust all blocks from")
var flagReadinessMaxLatency = flag.Duration("readiness-max-latency", 5*time.Second, "/healthz will return error until nodeos head block time is within that duration to now")
var flagDisableProfiler = flag.Bool("disable-profiler", false, "Disables the manageos profiler")

func main() {
	cli.RegisterCommonFlags()
	cli.RegisterBackupFlags()
	cli.RegisterSnapshotFlags()

	flag.Parse()
	nodeosExtraArgs := flag.Args()

	err := manageos.AugmentStackSizeLimit()
	derr.Check("unable to augment stack size limit", err)

	go func() {
		listenAddr := "localhost:6060"
		err := http.ListenAndServe(listenAddr, nil)
		if err != nil {
			zlog.Error("unable to start profiling server", zap.Error(err), zap.String("listen_addr", listenAddr))
		}
	}()

	// Run prometheus HTTP server
	go metrics.ServeMetrics()

	conf := &nodeosManagerApp.Config{
		ManagerAPIAddress:   *flagManagerAPIAddress,
		NodeosAPIAddress:    *flagNodeosAPIAddress,
		ConnectionWatchdog:  *flagConnectionWatchdog,
		NodeosConfigDir:     *flagNodeosConfigDir,
		NodeosBinPath:       *flagNodeosBinPath,
		NodeosDataDir:       *flagNodeosDataDir,
		ProducerHostname:    *flagProducerHostname,
		TrustedProducer:     *flagTrustedProducer,
		ReadinessMaxLatency: *flagReadinessMaxLatency,
		BackupStoreURL:      *cli.FlagBackupStoreURL,
		BootstrapDataURL:    *cli.FlagBootstrapDataURL,
		DebugDeepMind:       *cli.FlagDebugDeepMind,
		AutoRestoreLatest:   *cli.FlagAutoRestoreLatest,
		RestoreBackupName:   *cli.FlagRestoreBackupName,
		RestoreSnapshotName: *cli.FlagRestoreSnapshotName,
		SnapshotStoreURL:    *cli.FlagSnapshotStoreURL,
		ShutdownDelay:       *cli.FlagShutdownDelay,
		BackupTag:           *cli.FlagBackupTag,
		AutoBackupModulo:    *cli.FlagAutoBackupModulo,
		AutoBackupPeriod:    *cli.FlagAutoBackupPeriod,
		AutoSnapshotModulo:  *cli.FlagAutoSnapshotModulo,
		AutoSnapshotPeriod:  *cli.FlagAutoSnapshotPeriod,
		NodeosExtraArgs:     nodeosExtraArgs,
		DisableProfiler:     *flagDisableProfiler,
	}
	app := nodeosManagerApp.New(conf)

	// err := nodeosManagerApp.Run()
	// derr.Check("launching nodeos_manager app", err)

	// select {
	// case <-derr.SetupSignalHandler(conf.ShutdownDelay):
	// 	nodeosManagerApp.Shutdown(nil)
	// case <-nodeosManagerApp.Done():
	// }

	// if err := nodeosManagerApp.Err(); err != nil {
	// 	zlog.Error("operator did not complete cleanly", zap.Error(err))
	// }

	zlog.Info("running nodeos_manager application")
	derr.Check("error starting nodeos_manager", app.Run())

	select {
	case <-app.Terminated():
		if err := app.Err(); err != nil {
			zlog.Error("nodeos_manager shutdown with error", zap.Error(err))
		}
	case sig := <-derr.SetupSignalHandler(viper.GetDuration("global-shutdown-drain-delay")):
		zlog.Info("terminating through system signal", zap.Reflect("sig", sig))
		// TODO: should this propagate the signal errr
		err := fmt.Errorf("received signal %s", sig)
		app.Shutdown(err)
	}
}
