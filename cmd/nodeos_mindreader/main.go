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
	"net/http"
	_ "net/http/pprof"
	"os"
	"time"

	"github.com/dfuse-io/derr"
	"github.com/dfuse-io/manageos"
	nodeosMindreaderApp "github.com/dfuse-io/manageos/app/nodeos_mindreader"
	"github.com/dfuse-io/manageos/cli"
	"github.com/dfuse-io/manageos/metrics"
	"go.uber.org/zap"
)

var flagManagerAPIAddress = flag.String("manager-api-addr", ":8080", "eos-manager API address")
var flagAPIAddress = flag.String("api-addr", "http://localhost:8888", "Target API address")
var flagConnectionWatchdog = flag.Bool("connection-watchdog", true, "Force-reconnect dead peers automatically")
var flagNodeosConfigDir = flag.String("config-dir", "/etc/nodeos", "Directory for config files")
var flagNodeosBinPath = flag.String("nodeos-path", "/opt/eosio/bin/nodeos", "Path to nodeos")
var flagNodeosDataDir = flag.String("data-dir", "/nodeos-data", "Directory for data (blocks)")
var flagProducerHostname = flag.String("producer-hostname", "", "Hostname that will produce block (other will be paused)")
var flagTrustedProducer = flag.String("trusted-producer", "", "The EOS account name of the Block Producer we trust all blocks from")
var flagReadinessMaxLatency = flag.Duration("readiness-max-latency", 5*time.Second, "/healthz will return error until nodeos head block time is within that duration to now")
var flagDisableProfiler = flag.Bool("disable-profiler", false, "Disables the manageos profiler")

func init() {
	cli.RegisterCommonFlags()
	cli.RegisterMindreaderFlags()
	flag.Parse()
}

func main() {
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

	conf := &nodeosMindreaderApp.Config{
		ManagerAPIAddress:          *flagManagerAPIAddress,
		NodeosAPIAddress:           *flagAPIAddress,
		ConnectionWatchdog:         *flagConnectionWatchdog,
		NodeosConfigDir:            *flagNodeosConfigDir,
		NodeosBinPath:              *flagNodeosBinPath,
		NodeosDataDir:              *flagNodeosDataDir,
		ProducerHostname:           *flagProducerHostname,
		TrustedProducer:            *flagTrustedProducer,
		ReadinessMaxLatency:        *flagReadinessMaxLatency,
		NodeosExtraArgs:            nodeosExtraArgs,
		BackupStoreURL:             *cli.FlagBackupStoreURL,
		BackupTag:                  *cli.FlagBackupTag,
		BootstrapDataURL:           *cli.FlagBootstrapDataURL,
		DebugDeepMind:              *cli.FlagDebugDeepMind,
		AutoRestoreLatest:          *cli.FlagAutoRestoreLatest,
		RestoreBackupName:          *cli.FlagRestoreBackupName,
		RestoreSnapshotName:        *cli.FlagRestoreSnapshotName,
		SnapshotStoreURL:           *cli.FlagSnapshotStoreURL,
		ShutdownDelay:              *cli.FlagShutdownDelay,
		ArchiveStoreURL:            cli.ArchiveStoreURL(),
		MergeUploadDirectly:        *cli.FlagMergeUploadDirectly,
		GRPCAddr:                   *cli.FlagGRPCAddr,
		StartBlockNum:              uint64(*cli.FlagStartBlockNum),
		StopBlockNum:               uint64(*cli.FlagStopBlockNum),
		MindReadBlocksChanCapacity: *cli.FlagMindReadBlocksChanCapacity,
		WorkingDir:                 *cli.FlagWorkingDir,
		DisableProfiler:            *flagDisableProfiler,
	}
	app := nodeosMindreaderApp.New(conf)

	err = app.Run()
	derr.Check("launching nodeos_mindreader app", err)

	go metrics.ServeMetrics()

	select {
	case <-app.Terminated():
		zlog.Info("app terminated")
		if err := app.Err(); err != nil {
			zlog.Error("unsuccessful app termination", zap.Error(err))
			os.Exit(1)
		}
	case <-derr.SetupSignalHandler(conf.ShutdownDelay):
		zlog.Info("app received termination signal")
		app.Shutdown(nil)
	}

	zlog.Info("succesful app termination")
}
