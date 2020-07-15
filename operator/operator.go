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
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/dfuse-io/dstore"
	nodeManager "github.com/dfuse-io/node-manager"
	"github.com/dfuse-io/node-manager/profiler"
	"github.com/dfuse-io/shutter"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

type Operator struct {
	*shutter.Shutter
	options              *Options
	lastStartCommand     time.Time
	attemptedAutoRestore bool

	commandChan    chan *Command
	httpServer     *http.Server
	superviser     nodeManager.ChainSuperviser
	chainReadiness nodeManager.Readiness
	aboutToStop    *atomic.Bool
	snapshotStore  dstore.Store
	zlogger        *zap.Logger
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
	StartFailureHandlerFunc CallbackFunc

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

type CallbackFunc func()

// MarshalJSON our func type so that zap JSON reflection works correctly
func (c CallbackFunc) MarshalJSON() ([]byte, error) {
	return []byte(`"func()"`), nil
}

func New(zlogger *zap.Logger, chainSuperviser nodeManager.ChainSuperviser, chainReadiness nodeManager.Readiness, options *Options) (*Operator, error) {
	zlogger.Info("creating operator", zap.Reflect("options", options))

	o := &Operator{
		Shutter:        shutter.New(),
		chainReadiness: chainReadiness,
		commandChan:    make(chan *Command, 10),
		options:        options,
		superviser:     chainSuperviser,
		aboutToStop:    atomic.NewBool(false),
		zlogger:        zlogger,
	}

	if options.SnapshotStoreURL != "" {
		var err error
		o.snapshotStore, err = dstore.NewSimpleStore(options.SnapshotStoreURL)
		if err != nil {
			return nil, fmt.Errorf("unable to create snapshot store from url %q: %w", options.SnapshotStoreURL, err)
		}
	}

	return o, nil
}

func (o *Operator) Launch(startOnLaunch bool, httpListenAddr string, options ...HTTPOption) error {
	o.zlogger.Info("starting chain operator")
	o.OnTerminating(func(_ error) {
		o.zlogger.Info("chain operator terminating")
		o.waitForReadFlowToComplete()
	})

	o.zlogger.Info("launching operator HTTP server", zap.String("http_listen_addr", httpListenAddr))
	o.httpServer = o.RunHTTPServer(httpListenAddr, options...)

	if o.options.EnableSupervisorMonitoring {
		if monitorable, ok := o.superviser.(nodeManager.MonitorableChainSuperviser); ok {
			go monitorable.Monitor()
		}
	}

	err := o.bootstrap()
	if err != nil {
		return fmt.Errorf("unable to bootstrap chain: %w", err)
	}

	if startOnLaunch {
		o.zlogger.Debug("sending initial start command")
		o.commandChan <- &Command{cmd: "start", logger: o.zlogger}
	}

	for {
		o.zlogger.Info("operator ready to receive commands")
		select {
		case <-o.superviser.Stopped(): // stopped outside of a command that was expecting it
			if o.attemptedAutoRestore || time.Since(o.lastStartCommand) > 10*time.Second {
				o.Shutdown(fmt.Errorf("instance %q stopped (exit code: %d), shutting down", o.superviser.GetName(), o.superviser.LastExitCode()))
				if o.options.StartFailureHandlerFunc != nil {
					o.options.StartFailureHandlerFunc()
				}
				break
			}

			o.zlogger.Warn("instance stopped, attempting restore from source", zap.String("source", o.options.AutoRestoreSource), zap.String("command", o.superviser.GetCommand()))
			o.attemptedAutoRestore = true
			switch o.options.AutoRestoreSource {
			case "backup":
				if err := o.runCommand(&Command{
					cmd:    "restore",
					logger: o.zlogger,
				}); err != nil {
					o.Shutdown(fmt.Errorf("attempted restore failed"))
					if o.options.StartFailureHandlerFunc != nil {
						o.options.StartFailureHandlerFunc()
					}
				}
			case "snapshot":
				if err := o.runCommand(&Command{
					cmd:    "snapshot_restore",
					logger: o.zlogger,
				}); err != nil {
					o.Shutdown(fmt.Errorf("attempted restore failed"))
					if o.options.StartFailureHandlerFunc != nil {
						o.options.StartFailureHandlerFunc()
					}
				}
			}

		case <-o.Terminating():
			o.zlogger.Info("operator terminating, ending run/loop")
			o.runCommand(&Command{cmd: "maintenance"})
			o.zlogger.Info("operator run maintenance command")
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

func (o *Operator) waitForReadFlowToComplete() {
	o.zlogger.Info("chain operator shutting down")

	wg := &sync.WaitGroup{}
	wg.Add(2)

	go func() {
		// We give `(shutown_delay / 2)` time for http server to quit
		ctx, cancel := context.WithTimeout(context.Background(), time.Duration(float64(o.options.ShutdownDelay)/2.0))
		defer cancel()

		if o.httpServer != nil {
			if err := o.httpServer.Shutdown(ctx); err != nil {
				o.zlogger.Error("unable to close http server gracefully", zap.Error(err))
			}
		}

		wg.Done()
	}()

	go func() {
		err := o.superviser.Stop()
		if err != nil {
			o.zlogger.Error("unable to close superviser gracefully", zap.Error(err))
		}

		wg.Done()
	}()

	// FIXME: How could we have a timeout or so we do not wait forever!
	o.zlogger.Info("chain operator wait on group")
	wg.Wait()
	o.zlogger.Info("chain operator clean up done")
}

func (o *Operator) runSubCommand(name string, parentCmd *Command) error {
	return o.runCommand(&Command{cmd: name, returnch: parentCmd.returnch, logger: o.zlogger})
}

// runCommand does its work, and returns an error for irrecoverable states.
func (o *Operator) runCommand(cmd *Command) error {
	o.zlogger.Info("received operator command", zap.String("command", cmd.cmd), zap.Reflect("params", cmd.params))
	switch cmd.cmd {
	case "maintenance":
		o.zlogger.Info("preparing to stop process")

		o.aboutToStop.Store(true)
		defer o.aboutToStop.Store(false)
		if o.options.ShutdownDelay != 0 {
			o.zlogger.Info("marked as not_ready, waiting delay before actually stopping for maintenance", zap.Duration("delay", o.options.ShutdownDelay))
			time.Sleep(o.options.ShutdownDelay)
		}

		if err := o.superviser.Stop(); err != nil {
			return err
		}

		// Careful, we are now "stopped". Every other case can handle that state.
		o.zlogger.Info("successfully put in maintenance")

	case "restore":
		o.zlogger.Info("preparing for restore")
		backupable, ok := o.superviser.(nodeManager.BackupableChainSuperviser)
		if !ok {
			cmd.Return(errors.New("the chain superviser does not support backups"))
			return nil
		}

		if err := o.superviser.Stop(); err != nil {
			return err
		}

		o.zlogger.Info("asking chain superviser to restore a backup")
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
		o.zlogger.Info("preparing for volumesnapshot")
		volumesnapshotable, ok := o.superviser.(nodeManager.VolumeSnapshotableChainSuperviser)
		if !ok {
			cmd.Return(errors.New("the chain superviser does not support volume snapshot"))
			return nil
		}
		o.aboutToStop.Store(true)
		defer o.aboutToStop.Store(false)
		if o.options.ShutdownDelay != 0 {
			o.zlogger.Info("marked as not_ready, waiting delay before actually stopping for volume snapshot", zap.Duration("delay", o.options.ShutdownDelay))
			time.Sleep(o.options.ShutdownDelay)
		}

		lastBlockSeen := o.superviser.LastSeenBlockNum()
		if lastBlockSeen == 0 {
			cmd.Return(errors.New("volumesnapshot: invalid lastBlockSeen 0. Cowardly refusing to take a snapshot."))
			return nil
		}

		o.zlogger.Info("asking chain superviser to take a volume snapshot")
		if err := o.superviser.Stop(); err != nil {
			return err
		}

		if err := volumesnapshotable.TakeVolumeSnapshot(o.options.VolumeSnapshotAppVer, o.options.Project, o.options.Namespace, o.options.Pod, o.options.PVCPrefix, lastBlockSeen); err != nil {
			cmd.Return(err)
			// restart geth even if snapshot failed...
		}

		return o.runSubCommand("start", cmd)

	case "backup":
		o.zlogger.Info("preparing for backup")
		backupable, ok := o.superviser.(nodeManager.BackupableChainSuperviser)
		if !ok {
			cmd.Return(errors.New("the chain superviser does not support backups"))
			return nil
		}

		o.aboutToStop.Store(true)
		defer o.aboutToStop.Store(false)
		if o.options.ShutdownDelay != 0 {
			o.zlogger.Info("marked as not_ready, waiting delay before actually stopping for backup", zap.Duration("delay", o.options.ShutdownDelay))
			time.Sleep(o.options.ShutdownDelay)
		}

		o.zlogger.Info("asking chain superviser to take a backup")
		if err := o.superviser.Stop(); err != nil {
			return err
		}

		if err := backupable.TakeBackup(o.options.BackupTag, o.options.BackupStoreURL); err != nil {
			return err
		}

		return o.runSubCommand("start", cmd)

	case "snapshot":
		o.zlogger.Info("preparing for snapshot")
		snapshotable, ok := o.superviser.(nodeManager.SnapshotableChainSuperviser)
		if !ok {
			cmd.Return(fmt.Errorf("the chain superviser does not support snapshots"))
			return nil
		}

		o.aboutToStop.Store(true)
		defer o.aboutToStop.Store(false)
		if o.options.ShutdownDelay != 0 {
			o.zlogger.Info("marked as not_ready, waiting delay before actually taking snapshot", zap.Duration("delay", o.options.ShutdownDelay))
			time.Sleep(o.options.ShutdownDelay)
		}

		if err := snapshotable.TakeSnapshot(o.getSnapshotStore(), o.options.NumberOfSnapshotsToKeep); err != nil {
			cmd.Return(fmt.Errorf("unable to take snapshot: %w", err))
			return nil
		}

		o.zlogger.Info("snapshot completed")

	case "snapshot_restore":
		o.zlogger.Info("preparing for performing a snapshot restore")
		snapshotable, ok := o.superviser.(nodeManager.SnapshotableChainSuperviser)
		if !ok {
			cmd.Return(errors.New("the chain superviser does not support snapshots"))
			return nil
		}

		o.zlogger.Info("asking chain superviser to stop due to snapshot restore command")
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

		o.zlogger.Warn("restarting node from snapshot, the restart will perform the actual snapshot restoration")
		return o.runSubCommand("start", cmd)

	case "reload":
		o.zlogger.Info("preparing for reload")
		if err := o.superviser.Stop(); err != nil {
			return err
		}

		return o.runSubCommand("start", cmd)

	case "safely_resume_production":
		o.zlogger.Info("preparing for safely resume production")
		producer, ok := o.superviser.(nodeManager.ProducerChainSuperviser)
		if !ok {
			cmd.Return(fmt.Errorf("the chain superviser does not support producing blocks"))
			return nil
		}

		isProducing, err := producer.IsProducing()
		if err != nil {
			cmd.Return(fmt.Errorf("unable to check if producing: %w", err))
			return nil
		}

		if !isProducing {
			o.zlogger.Info("resuming production of blocks")
			err := producer.ResumeProduction()
			if err != nil {
				cmd.Return(fmt.Errorf("error resuming production of blocks: %w", err))
				return nil
			}

			o.zlogger.Info("successfully resumed producer")

		} else {
			o.zlogger.Info("block production was already running, doing nothing")
		}

		o.zlogger.Info("successfully resumed block production")

	case "safely_pause_production":
		o.zlogger.Info("preparing for safely pause production")
		producer, ok := o.superviser.(nodeManager.ProducerChainSuperviser)
		if !ok {
			cmd.Return(fmt.Errorf("the chain superviser does not support producing blocks"))
			return nil
		}

		isProducing, err := producer.IsProducing()
		if err != nil {
			cmd.Return(fmt.Errorf("unable to check if producing: %w", err))
			return nil
		}

		if !isProducing {
			o.zlogger.Info("block production is already paused, command is a no-op")
			return nil
		}

		o.zlogger.Info("waiting to pause the producer")
		err = producer.WaitUntilEndOfNextProductionRound(3 * time.Minute)
		if err != nil {
			cmd.Return(fmt.Errorf("timeout waiting for production round: %w", err))
			return nil
		}

		o.zlogger.Info("pausing block production")
		err = producer.PauseProduction()
		if err != nil {
			cmd.Return(fmt.Errorf("unable to pause production correctly: %w", err))
			return nil
		}

		o.zlogger.Info("successfully paused block production")

	case "safely_reload":
		o.zlogger.Info("preparing for safely reload")
		producer, ok := o.superviser.(nodeManager.ProducerChainSuperviser)
		if ok && producer.IsActiveProducer() {
			o.zlogger.Info("waiting right after production round")
			err := producer.WaitUntilEndOfNextProductionRound(3 * time.Minute)
			if err != nil {
				cmd.Return(fmt.Errorf("timeout waiting for production round: %w", err))
				return nil
			}
		}

		o.zlogger.Info("issuing 'reload' now")
		emptied := false
		for !emptied {
			select {
			case interimCmd := <-o.commandChan:
				o.zlogger.Info("emptying command queue while safely_reload was running, dropped", zap.Any("interim_cmd", interimCmd))
			default:
				emptied = true
			}
		}

		return o.runSubCommand("reload", cmd)

	case "start", "resume":
		o.zlogger.Info("preparing for start")
		if o.superviser.IsRunning() {
			o.zlogger.Info("chain is already running")
			return nil
		}

		o.zlogger.Info("preparing to start chain")

		var options []nodeManager.StartOption
		if value := cmd.params["debug-deep-mind"]; value != "" {
			if value == "true" {
				options = append(options, nodeManager.EnableDebugDeepmindOption)
			} else {
				options = append(options, nodeManager.DisableDebugDeepmindOption)
			}
		}

		if err := o.superviser.Start(options...); err != nil {
			return fmt.Errorf("error starting chain superviser: %w", err)
		}

		o.zlogger.Info("successfully start service")

	case "shutdown":
		o.zlogger.Info("preparing for shutdown")
		o.aboutToStop.Store(true)
		defer o.aboutToStop.Store(false)
		if o.options.ShutdownDelay != 0 {
			o.zlogger.Info("marked as not_ready, waiting delay before actually stopping for shutdown", zap.Duration("delay", o.options.ShutdownDelay))
			time.Sleep(o.options.ShutdownDelay)
		}

		if err := o.superviser.Stop(); err != nil {
			o.zlogger.Error("stopping nodeos failed, continuing shutdown anyway", zap.Error(err))
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
	// Forcing restore here
	if o.options.RestoreBackupName != "" {
		o.zlogger.Info("performing bootstrap from backup")
		return o.bootstrapFromBackup(o.options.RestoreBackupName)
	}
	if o.options.RestoreSnapshotName != "" {
		o.zlogger.Info("performing bootstrap from snapshot")
		return o.bootstrapFromSnapshot(o.options.RestoreSnapshotName)
	}

	if o.superviser.HasData() {
		return nil
	}

	if o.options.BootstrapDataURL != "" {
		o.zlogger.Info("chain has no prior data and bootstrap data url is set, attempting bootstrap from URL")
		err := o.bootstrapFromDataURL(o.options.BootstrapDataURL)
		if err != nil {
			o.zlogger.Warn("could not bootstrap from URL", zap.Error(err))
		} else {
			o.zlogger.Info("success bootstrap from URL")
			return nil
		}
	}

	return nil
}

func (o *Operator) bootstrapFromDataURL(dataURL string) error {
	o.zlogger.Debug("bootstraping from pre-existing data prior starting process")
	bootstrapable, ok := o.superviser.(nodeManager.BootstrapableChainSuperviser)
	if !ok {
		return errors.New("the chain superviser does not support bootstrap")
	}

	u, err := url.Parse(dataURL)
	if err != nil {
		return fmt.Errorf("unable to parse URL: %w", err)
	}

	storeURL := fmt.Sprintf("%s://%s", u.Scheme, u.Hostname())
	dataStore, err := dstore.NewSimpleStore(storeURL)
	if err != nil {
		return fmt.Errorf("unable to create store: %w", err)
	}

	err = bootstrapable.Bootstrap(strings.TrimLeft(u.Path, "/"), dataStore)
	if err != nil {
		return fmt.Errorf("unable to bootstrap from data URL %q: %w", dataURL, err)
	}

	return nil
}

func (o *Operator) bootstrapFromSnapshot(snapshotName string) error {
	o.zlogger.Debug("restoring snapshot prior starting process")
	snapshotable, ok := o.superviser.(nodeManager.SnapshotableChainSuperviser)
	if !ok {
		return errors.New("the chain superviser does not support snapshots")
	}

	return o.restoreSnapshot(snapshotable, snapshotName)
}

func (o *Operator) bootstrapFromBackup(backupName string) error {
	o.zlogger.Debug("restoring backup prior starting process")
	backupable, ok := o.superviser.(nodeManager.BackupableChainSuperviser)
	if !ok {
		return errors.New("the chain superviser does not support backups")
	}

	err := backupable.RestoreBackup(backupName, o.options.BackupTag, o.options.BackupStoreURL)
	if err != nil {
		return fmt.Errorf("unable to restore backup %q: %w", backupName, err)
	}

	return nil
}
func (o *Operator) SetMaintenance() {
	o.zlogger.Info("setting maintenance mode")
	o.commandChan <- &Command{cmd: "maintenance", logger: o.zlogger}
}

func (o *Operator) restoreSnapshot(snapshotable nodeManager.SnapshotableChainSuperviser, snapshotName string) error {
	store := o.getSnapshotStore()

	o.zlogger.Debug("checking if snapshot exists, mayber performing local override if it doesn't", zap.String("snapshot_name", snapshotName))
	if exists, err := store.FileExists(context.Background(), snapshotName); err == nil && !exists {
		newName, newStore := o.maybeSnapshotFromLocalFile(snapshotName)
		if newName != "" && newStore != nil {
			o.zlogger.Info("snapshot name is local file, override snapshot store to point to local file")
			store = newStore
			snapshotName = newName
		}
	} else if err != nil {
		o.zlogger.Debug("unable to check if name exists in snapshot store", zap.Error(err))
	}

	if err := snapshotable.RestoreSnapshot(snapshotName, store); err != nil {
		return fmt.Errorf("unable to restore snapshot %q: %w", snapshotName, err)
	}

	return nil
}

func (o *Operator) maybeSnapshotFromLocalFile(snapshotName string) (newName string, newStore dstore.Store) {
	o.zlogger.Debug("snapshot not found in store, checking if it's a local file")
	localFile, err := filepath.Abs(filepath.Clean(snapshotName))
	if err != nil {
		o.zlogger.Debug("snapshot name does not appear to be a valid local file, continuing without local override")
		return
	}

	if stat, err := os.Stat(localFile); err != nil || stat.IsDir() {
		o.zlogger.Debug("local snapshot does not seems to be a local file (or lookup failed), continue without local override")
		return
	}

	store, err := dstore.NewSimpleStore("file://" + filepath.Dir(localFile))
	if err != nil {
		o.zlogger.Debug("unable to create local snapshot store override, continue without local override")
		return
	}

	return filepath.Base(localFile), store
}

func (o *Operator) getSnapshotStore() dstore.Store {
	if o.snapshotStore == nil {
		o.Shutdown(errors.New("trying to get snapshot store, but instance is nil, have you provided --snapshot-store-url flag?"))
	}

	return o.snapshotStore
}

func (o *Operator) ConfigureAutoBackup(autoBackupInterval time.Duration, autoBackupBlockFrequency int, expectedHostname, hostname string) {
	if expectedHostname != "" && hostname != expectedHostname {
		o.zlogger.Info("not setting auto-backup because hostname does not match expected value", zap.String("hostname", hostname), zap.String("expected_hostname", expectedHostname))
		return
	}

	if autoBackupInterval != 0 {
		go o.RunEveryPeriod(autoBackupInterval, "backup")
	}

	if autoBackupBlockFrequency != 0 {
		go o.RunEveryXBlock(uint32(autoBackupBlockFrequency), "backup")
	}
}

func (o *Operator) ConfigureAutoSnapshot(autoSnapshotInterval time.Duration, autoSnapshotBlockFrequency int, expectedHostname, hostname string) {
	if expectedHostname != "" && hostname != expectedHostname {
		o.zlogger.Info("not setting auto-snapshot because hostname does not match expected value", zap.String("hostname", hostname), zap.String("expected_hostname", expectedHostname))
		return
	}

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
				o.commandChan <- &Command{cmd: commandName, logger: o.zlogger}
			}
		}
	}
}

func (o *Operator) RunAtSpecificBlocks(specificBlocks []uint64, commandName string) {
	o.zlogger.Info("scheduled for running a job a specific blocks", zap.String("command_name", commandName), zap.Any("specific_blocks", specificBlocks))
	sort.Slice(specificBlocks, func(i, j int) bool { return specificBlocks[i] < specificBlocks[j] })
	nextIndex := 0
	for {
		time.Sleep(1 * time.Second)
		head := o.superviser.LastSeenBlockNum()
		if head == 0 {
			continue
		}

		if head > specificBlocks[nextIndex] {
			o.commandChan <- &Command{cmd: commandName, logger: o.zlogger}
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
			o.commandChan <- &Command{cmd: commandName, logger: o.zlogger}
			lastHeadReference = lastSeenBlockNum
		}
	}
}
