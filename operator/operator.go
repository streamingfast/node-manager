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

package operator

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/dfuse-io/dstore"
	"github.com/dfuse-io/manageos"
	"github.com/dfuse-io/manageos/profiler"
	"github.com/dfuse-io/shutter"
	"go.uber.org/zap"
)

type Operator struct {
	*shutter.Shutter
	options              *Options
	lastStartCommand     time.Time
	attemptedAutoRestore bool

	commandChan    chan *Command
	httpServer     *http.Server
	superviser     manageos.ChainSuperviser
	chainReadiness manageos.Readiness
	snapshotStore  dstore.Store
	zlog           *zap.Logger
}

type Options struct {
	BackupTag            string
	BackupStoreURL       string
	SnapshotStoreURL     string
	VolumeSnapshotAppVer string
	Namespace            string //k8s namespace
	Pod                  string //k8s podname
	PVCPrefix            string
	Project              string //gcp project

	BootstrapDataURL        string
	AutoRestoreSource       string
	NumberOfSnapshotsToKeep int
	RestoreBackupName       string
	RestoreSnapshotName     string
	Profiler                *profiler.Profiler
	StartFailureHandlerFunc func()

	EnableSupervisorMonitoring bool

	// Delay before shutting manager when sigterm received
	ShutdownDelay time.Duration
}

type Command struct {
	cmd      string
	params   map[string]string
	returnch chan error
	closer   sync.Once
	logger   *zap.Logger
}

func New(zlog *zap.Logger, chainSuperviser manageos.ChainSuperviser, chainReadiness manageos.Readiness, options *Options) (*Operator, error) {
	//logging.Register("github.com/dfuse-io/manageos/operator/"+loggerName, &zlog)

	o := &Operator{
		Shutter:        shutter.New(),
		chainReadiness: chainReadiness,
		commandChan:    make(chan *Command, 10),
		options:        options,
		superviser:     chainSuperviser,
		zlog:           zlog,
	}

	if options.SnapshotStoreURL != "" {
		var err error
		o.snapshotStore, err = dstore.NewSimpleStore(options.SnapshotStoreURL)
		if err != nil {
			return nil, fmt.Errorf("unable to create snapshot store from url %q: %s", options.SnapshotStoreURL, err)
		}
	}

	return o, nil
}

func (o *Operator) Launch(startOnLaunch bool, httpListenAddr string, options ...HTTPOption) error {
	o.zlog.Info("starting chain operator")
	o.OnTerminating(func(_ error) {
		o.zlog.Info("chain operator terminating")
		o.cleanUp()
	})

	o.zlog.Info("launching operator HTTP server", zap.String("http_listen_addr", httpListenAddr))
	o.httpServer = o.RunHTTPServer(httpListenAddr, options...)

	if o.options.EnableSupervisorMonitoring {
		if monitorable, ok := o.superviser.(manageos.MonitorableChainSuperviser); ok {
			go monitorable.Monitor()
		}
	}

	err := o.bootstrap()
	if err != nil {
		return fmt.Errorf("unable to bootstrap chain: %s", err)
	}

	if startOnLaunch {
		o.zlog.Debug("sending initial start command")
		o.commandChan <- &Command{cmd: "start", logger: o.zlog}
	}

	for {
		o.zlog.Info("operator ready to receive commands")
		select {
		case <-o.superviser.Stopped(): // stopped outside of a command that was expecting it
			if o.attemptedAutoRestore || time.Since(o.lastStartCommand) > 10*time.Second {
				o.Shutdown(fmt.Errorf("Instance `%s` stopped (exit code: %d). Shutting down.", o.superviser.GetName(), o.superviser.LastExitCode()))
				if o.options.StartFailureHandlerFunc != nil {
					o.options.StartFailureHandlerFunc()
				}
				break
			}
			o.zlog.Warn("Instance stopped. Attempting restore from snapshot", zap.String("command", o.superviser.GetCommand()))
			o.attemptedAutoRestore = true
			switch o.options.AutoRestoreSource {
			case "backup":
				if err := o.runCommand(&Command{
					cmd:    "restore",
					logger: o.zlog,
				}); err != nil {
					o.Shutdown(fmt.Errorf("attempted restore failed"))
					if o.options.StartFailureHandlerFunc != nil {
						o.options.StartFailureHandlerFunc()
					}
				}
			case "snapshot":
				if err := o.runCommand(&Command{
					cmd:    "snapshot_restore",
					logger: o.zlog,
				}); err != nil {
					o.Shutdown(fmt.Errorf("attempted restore failed"))
					if o.options.StartFailureHandlerFunc != nil {
						o.options.StartFailureHandlerFunc()
					}
				}
			}

		case <-o.Terminating():
			o.zlog.Info("operator terminating, ending run/loop")
			o.runCommand(&Command{cmd: "maintenance"})
			o.zlog.Info("operator run maintenance command")
			return nil

		case cmd := <-o.commandChan:
			if cmd.cmd == "start" { // start 'sub' commands after a restore do NOT come through here
				o.lastStartCommand = time.Now()
				o.attemptedAutoRestore = false
			}
			err := o.runCommand(cmd)
			cmd.Return(err)
			if err != nil {
				if err == ErrCleanExit {
					return nil
				}
				return fmt.Errorf("command %v execution failed: %v", cmd.cmd, err)
			}
		}
	}
}

func (o *Operator) cleanUp() {
	o.zlog.Info("chain operator shutting down")

	wg := &sync.WaitGroup{}
	wg.Add(2)

	go func() {
		// We give `(shutown_delay / 2)` time for http server to quit
		ctx, cancel := context.WithTimeout(context.Background(), time.Duration(float64(o.options.ShutdownDelay)/2.0))
		defer cancel()

		if o.httpServer != nil {
			if err := o.httpServer.Shutdown(ctx); err != nil {
				o.zlog.Error("unable to close http server gracefully", zap.Error(err))
			}
		}

		wg.Done()
	}()

	go func() {
		err := o.superviser.Stop()
		if err != nil {
			o.zlog.Error("unable to close superviser gracefully", zap.Error(err))
		}

		wg.Done()
	}()

	// FIXME: How could we have a timeout or so we do not wait forever!
	o.zlog.Info("chain operator wait on group")
	wg.Wait()
	o.zlog.Info("chain operator clean up done")
}

func (o *Operator) runSubCommand(name string, parentCmd *Command) error {
	return o.runCommand(&Command{cmd: name, returnch: parentCmd.returnch, logger: o.zlog})
}

// runCommand does its work, and returns an error for irrecoverable states.
func (o *Operator) runCommand(cmd *Command) error {
	o.zlog.Info("received operator command", zap.String("command", cmd.cmd), zap.Reflect("params", cmd.params))
	switch cmd.cmd {
	case "maintenance":
		o.zlog.Info("preparing to stop process")
		if err := o.superviser.Stop(); err != nil {
			return err
		}

		// Careful, we are now "stopped". Every other case can handle that state.
		o.zlog.Info("successfully put in maintenance")

	case "restore":
		o.zlog.Info("preparing for restore")
		backupable, ok := o.superviser.(manageos.BackupableChainSuperviser)
		if !ok {
			cmd.Return(errors.New("the chain superviser does not support backups"))
			return nil
		}

		o.zlog.Info("asking chain superviser to restore a backup")
		if err := o.superviser.Stop(); err != nil {
			return err
		}

		backupName := "latest"
		if b, ok := cmd.params["backupName"]; ok {
			backupName = b
		}

		if err := backupable.RestoreBackup(backupName, o.options.BackupTag, o.options.BackupStoreURL); err != nil {
			return err
		}

		return o.runSubCommand("start", cmd)

	case "volumesnapshot":
		o.zlog.Info("preparing for volumesnapshot")
		volumesnapshotable, ok := o.superviser.(manageos.VolumeSnapshotableChainSuperviser)
		if !ok {
			cmd.Return(errors.New("the chain superviser does not support volume snapshot"))
			return nil
		}

		lastBlockSeen := o.superviser.LastSeenBlockNum()
		if lastBlockSeen == 0 {
			cmd.Return(errors.New("volumesnapshot: invalid lastBlockSeen 0. Cowardly refusing to take a snapshot."))
			return nil
		}

		o.zlog.Info("asking chain superviser to take a volume snapshot")
		if err := o.superviser.Stop(); err != nil {
			return err
		}

		if err := volumesnapshotable.TakeVolumeSnapshot(o.options.VolumeSnapshotAppVer, o.options.Project, o.options.Namespace, o.options.Pod, o.options.PVCPrefix, lastBlockSeen); err != nil {
			cmd.Return(err)
			// restart geth even if snapshot failed...
		}

		return o.runSubCommand("start", cmd)

	case "backup":
		o.zlog.Info("preparing for backup")
		backupable, ok := o.superviser.(manageos.BackupableChainSuperviser)
		if !ok {
			cmd.Return(errors.New("the chain superviser does not support backups"))
			return nil
		}

		o.zlog.Info("asking chain superviser to take a backup")
		if err := o.superviser.Stop(); err != nil {
			return err
		}

		if err := backupable.TakeBackup(o.options.BackupTag, o.options.BackupStoreURL); err != nil {
			return err
		}

		return o.runSubCommand("start", cmd)

	case "snapshot":
		o.zlog.Info("preparing for snapshot")
		snapshotable, ok := o.superviser.(manageos.SnapshotableChainSuperviser)
		if !ok {
			cmd.Return(fmt.Errorf("the chain superviser does not support snapshots"))
			return nil
		}

		if err := snapshotable.TakeSnapshot(o.getSnapshotStore(), o.options.NumberOfSnapshotsToKeep); err != nil {
			cmd.Return(fmt.Errorf("unable to take snapshot: %s", err))
			return nil
		}

		o.zlog.Info("snapshot completed")

	case "snapshot_restore":
		o.zlog.Info("preparing for performing a snapshot restore")
		snapshotable, ok := o.superviser.(manageos.SnapshotableChainSuperviser)
		if !ok {
			cmd.Return(errors.New("the chain superviser does not support snapshots"))
			return nil
		}

		o.zlog.Info("asking chain superviser to stop due to snapshot restore command")
		if err := o.superviser.Stop(); err != nil {
			return err
		}

		snapshotName := "latest"
		if b, ok := cmd.params["snapshotName"]; ok {
			snapshotName = b
		}

		err := o.restoreSnapshot(snapshotable, snapshotName)
		if err != nil {
			return err
		}

		o.zlog.Warn("restarting node from snapshot, the restart will perform the actual snapshot restoration")
		return o.runSubCommand("start", cmd)

	case "reload":
		o.zlog.Info("preparing for reload")
		if err := o.superviser.Stop(); err != nil {
			return err
		}

		return o.runSubCommand("start", cmd)

	case "safely_resume_production":
		o.zlog.Info("preparing for safely resume production")
		producer, ok := o.superviser.(manageos.ProducerChainSuperviser)
		if !ok {
			cmd.Return(fmt.Errorf("the chain superviser does not support producing blocks"))
			return nil
		}

		isProducing, err := producer.IsProducing()
		if err != nil {
			cmd.Return(fmt.Errorf("unable to check if producing: %s", err))
			return nil
		}

		if !isProducing {
			o.zlog.Info("resuming production of blocks")
			err := producer.ResumeProduction()
			if err != nil {
				cmd.Return(fmt.Errorf("error resuming production of blocks: %s", err))
				return nil
			}

			o.zlog.Info("successfully resumed producer")

		} else {
			o.zlog.Info("block production was already running, doing nothing")
		}

		o.zlog.Info("successfully resumed block production")

	case "safely_pause_production":
		o.zlog.Info("preparing for safely pause production")
		producer, ok := o.superviser.(manageos.ProducerChainSuperviser)
		if !ok {
			cmd.Return(fmt.Errorf("the chain superviser does not support producing blocks"))
			return nil
		}

		isProducing, err := producer.IsProducing()
		if err != nil {
			cmd.Return(fmt.Errorf("unable to check if producing: %s", err))
			return nil
		}

		if !isProducing {
			o.zlog.Info("block production is already paused, command is a no-op")
			return nil
		}

		o.zlog.Info("waiting to pause the producer")
		err = producer.WaitUntilEndOfNextProductionRound(3 * time.Minute)
		if err != nil {
			cmd.Return(fmt.Errorf("timeout waiting for production round: %s", err))
			return nil
		}

		o.zlog.Info("pausing block production")
		err = producer.PauseProduction()
		if err != nil {
			cmd.Return(fmt.Errorf("unable to pause production correctly: %s", err))
			return nil
		}

		o.zlog.Info("successfully paused block production")

	case "safely_reload":
		o.zlog.Info("preparing for safely reload")
		producer, ok := o.superviser.(manageos.ProducerChainSuperviser)
		if ok && producer.IsActiveProducer() {
			o.zlog.Info("waiting right after production round")
			err := producer.WaitUntilEndOfNextProductionRound(3 * time.Minute)
			if err != nil {
				cmd.Return(fmt.Errorf("timeout waiting for production round: %s", err))
				return nil
			}
		}

		o.zlog.Info("issuing 'reload' now")
		emptied := false
		for !emptied {
			select {
			case interimCmd := <-o.commandChan:
				o.zlog.Info("emptying command queue while safely_reload was running, dropped", zap.Any("interim_cmd", interimCmd))
			default:
				emptied = true
			}
		}

		return o.runSubCommand("reload", cmd)

	case "start", "resume":
		o.zlog.Info("preparing for start")
		if o.superviser.IsRunning() {
			o.zlog.Info("chain is already running")
			return nil
		}

		o.zlog.Info("preparing to start chain")

		var options []manageos.StartOption
		if value := cmd.params["debug-deep-mind"]; value != "" {
			if value == "true" {
				options = append(options, manageos.EnableDebugDeepmindOption)
			} else {
				options = append(options, manageos.DisableDebugDeepmindOption)
			}
		}

		if err := o.superviser.Start(options...); err != nil {
			return fmt.Errorf("error starting chain superviser: %s", err)
		}

		o.zlog.Info("successfully start service")

	case "shutdown":
		o.zlog.Info("preparing for shutdown")
		if err := o.superviser.Stop(); err != nil {
			o.zlog.Error("stopping nodeos failed, continuing shutdown anyway", zap.Error(err))
		}

		return ErrCleanExit
	}

	return nil
}

func (c *Command) Return(err error) {
	c.closer.Do(func() {
		if err != nil && err != ErrCleanExit {
			c.logger.Error("command failed", zap.String("cmd", c.cmd), zap.Error(err))
		}

		if c.returnch != nil {
			c.returnch <- err
		}
	})
}

func (o *Operator) bootstrap() error {
	// forcing restore here
	if o.options.RestoreBackupName != "" {
		o.zlog.Info("Performing Bootstrap from Backup")
		return o.bootstrapFromBackup(o.options.RestoreBackupName)
	}
	if o.options.RestoreSnapshotName != "" {
		o.zlog.Info("Performing Bootstrap from Snapshot")
		return o.bootstrapFromSnapshot(o.options.RestoreSnapshotName)
	}

	if o.superviser.HasData() {
		return nil
	}

	if o.options.BootstrapDataURL != "" {
		o.zlog.Info("chain has no prior data and bootstrapDataURL is set. Attempting bootstrap from URL")
		err := o.bootstrapFromDataURL(o.options.BootstrapDataURL)
		if err != nil {
			o.zlog.Warn("could not bootstrap from URL", zap.Error(err))
		} else {
			o.zlog.Info("success bootstrap from URL")
			return nil
		}
	}

	// TODO: use BootstrapDataURL here to support backup:///pitreos-backupname, snapshot:///snapshotname, ...
	// using AutoRestore as a bootstrap source is confusing
	//switch o.options.AutoRestoreSource {
	//case "backup":
	//	o.zlog.Info("chain has no prior data and autoRestoreMethod is set to backup. Attempting restore from backup")
	//	err := o.bootstrapFromBackup("latest")
	//	o.zlog.Warn("could not bootstrap from Backup", zap.Error(err))

	//case "snapshot":
	//	o.zlog.Info("chain has no prior data and autoRestoreMethod is set to snapshot. Attempting restore from snapshot")
	//	err := o.bootstrapFromSnapshot("latest")
	//	o.zlog.Info("could not bootstrap from snapshot", zap.Error(err))

	//}

	return nil
}

func (o *Operator) bootstrapFromDataURL(dataURL string) error {
	o.zlog.Debug("bootstraping from pre-existing data prior starting process")
	bootstrapable, ok := o.superviser.(manageos.BootstrapableChainSuperviser)
	if !ok {
		return errors.New("the chain superviser does not support bootstrap")
	}

	u, err := url.Parse(dataURL)
	if err != nil {
		return fmt.Errorf("unable to parse URL: %s", err)
	}

	storeURL := fmt.Sprintf("%s://%s", u.Scheme, u.Hostname())
	dataStore, err := dstore.NewSimpleStore(storeURL)
	if err != nil {
		return fmt.Errorf("unable to create store: %s", err)
	}

	err = bootstrapable.Bootstrap(strings.TrimLeft(u.Path, "/"), dataStore)
	if err != nil {
		return fmt.Errorf("unable to bootstrap from data URL %q: %s", dataURL, err)
	}

	return nil
}

func (o *Operator) bootstrapFromSnapshot(snapshotName string) error {
	o.zlog.Debug("restoring snapshot prior starting process")
	snapshotable, ok := o.superviser.(manageos.SnapshotableChainSuperviser)
	if !ok {
		return errors.New("the chain superviser does not support snapshots")
	}

	return o.restoreSnapshot(snapshotable, snapshotName)
}

func (o *Operator) bootstrapFromBackup(backupName string) error {
	o.zlog.Debug("restoring backup prior starting process")
	backupable, ok := o.superviser.(manageos.BackupableChainSuperviser)
	if !ok {
		return errors.New("the chain superviser does not support backups")
	}

	err := backupable.RestoreBackup(backupName, o.options.BackupTag, o.options.BackupStoreURL)
	if err != nil {
		return fmt.Errorf("unable to restore backup %q: %s", backupName, err)
	}

	return nil
}
func (o *Operator) SetMaintenance() {
	o.zlog.Info("setting maintenance mode")
	o.commandChan <- &Command{cmd: "maintenance", logger: o.zlog}
}

func (o *Operator) restoreSnapshot(snapshotable manageos.SnapshotableChainSuperviser, snapshotName string) error {
	if o.snapshotStore == nil {
		o.Shutdown(errors.New("trying to get snapshot store, but instance is nil, have you provided --snapshot-store-url flag?"))
	}

	if err := snapshotable.RestoreSnapshot(snapshotName, o.snapshotStore); err != nil {
		return fmt.Errorf("unable to restore snapshot %q: %s", snapshotName, err)
	}

	return nil
}

func (o *Operator) getSnapshotStore() dstore.Store {
	if o.snapshotStore == nil {
		o.Shutdown(errors.New("trying to get snapshot store, but instance is nil, have you provided --snapshot-store-url flag?"))
	}

	return o.snapshotStore
}

func (o *Operator) ConfigureAutoBackup(autoBackupInterval time.Duration, autoBackupBlockFrequency int) {
	if autoBackupInterval != 0 {
		go o.RunEveryPeriod(autoBackupInterval, "backup")
	}

	if autoBackupBlockFrequency != 0 {
		go o.RunEveryXBlock(uint32(autoBackupBlockFrequency), "backup")
	}
}

func (o *Operator) ConfigureAutoSnapshot(autoSnapshotInterval time.Duration, autoSnapshotBlockFrequency int) {
	if autoSnapshotInterval != 0 {
		go o.RunEveryPeriod(autoSnapshotInterval, "snapshot")
	}

	if autoSnapshotBlockFrequency != 0 {
		go o.RunEveryXBlock(uint32(autoSnapshotBlockFrequency), "snapshot")
	}
}

func (o *Operator) ConfigureAutoVolumeSnapshot(autoVolumeSnapshotInterval time.Duration, autoVolumeSnapshotBlockFrequency int, autoVolumeSnapshotSpecificBlocks []uint64) {
	if autoVolumeSnapshotInterval != 0 {
		go o.RunEveryPeriod(autoVolumeSnapshotInterval, "volumesnapshot")
	}

	if autoVolumeSnapshotBlockFrequency != 0 {
		go o.RunEveryXBlock(uint32(autoVolumeSnapshotBlockFrequency), "volumesnapshot")
	}

	if len(autoVolumeSnapshotSpecificBlocks) > 0 {
		go o.RunAtSpecificBlocks(autoVolumeSnapshotSpecificBlocks, "volumesnapshot")
	}
}

// RunEveryPeriod will skip a run if Nodeos is NOT alive when period expired.
func (o *Operator) RunEveryPeriod(period time.Duration, commandName string) {
	for {
		time.Sleep(1)
		if o.superviser.IsRunning() {
			break
		}
	}

	ticker := time.NewTicker(period)
	for {
		select {
		case <-ticker.C:
			if o.superviser.IsRunning() {
				o.commandChan <- &Command{cmd: commandName, logger: o.zlog}
			}
		}
	}
}

func (o *Operator) RunAtSpecificBlocks(specificBlocks []uint64, commandName string) {
	o.zlog.Info("Scheduled for running a job a specific blocks", zap.String("command_name", commandName), zap.Any("specific_blocks", specificBlocks))
	sort.Slice(specificBlocks, func(i, j int) bool { return specificBlocks[i] < specificBlocks[j] })
	nextIndex := 0
	for {
		time.Sleep(1 * time.Second)
		head := o.superviser.LastSeenBlockNum()
		if head == 0 {
			continue
		}

		if head > specificBlocks[nextIndex] {
			o.commandChan <- &Command{cmd: commandName, logger: o.zlog}
			for {
				nextIndex++
				if nextIndex >= len(specificBlocks) {
					return
				}
				if head < specificBlocks[nextIndex] {
					break
				}
			}
		}
	}
}

func (o *Operator) RunEveryXBlock(freq uint32, commandName string) {
	var lastHeadReference uint64
	for {
		time.Sleep(1 * time.Second)
		lastSeenBlockNum := o.superviser.LastSeenBlockNum()
		if lastSeenBlockNum == 0 {
			continue
		}

		if lastHeadReference == 0 {
			lastHeadReference = lastSeenBlockNum
		}

		if lastSeenBlockNum > lastHeadReference+uint64(freq) {
			o.commandChan <- &Command{cmd: commandName, logger: o.zlog}
			lastHeadReference = lastSeenBlockNum
		}
	}
}
