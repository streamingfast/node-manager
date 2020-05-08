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
	ReadyFunc            func()

	logger         *zap.Logger
	commandChan    chan *Command
	httpServer     *http.Server
	superviser     manageos.ChainSuperviser
	chainReadiness manageos.Readiness
	snapshotStore  dstore.Store
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

	ReadyFunc func()
}

type Command struct {
	cmd      string
	params   map[string]string
	returnch chan error
	closer   sync.Once
	logger   *zap.Logger
}

func New(logger *zap.Logger, chainSuperviser manageos.ChainSuperviser, chainReadiness manageos.Readiness, options *Options) (*Operator, error) {
	m := &Operator{
		Shutter:        shutter.New(),
		chainReadiness: chainReadiness,
		logger:         logger,
		commandChan:    make(chan *Command, 10),
		options:        options,
		superviser:     chainSuperviser,
		ReadyFunc:      options.ReadyFunc,
	}

	if options.SnapshotStoreURL != "" {
		var err error
		m.snapshotStore, err = dstore.NewSimpleStore(options.SnapshotStoreURL)
		if err != nil {
			return nil, fmt.Errorf("unable to create snapshot store from url %q: %s", options.SnapshotStoreURL, err)
		}
	}

	return m, nil
}

func (m *Operator) Launch(startOnLaunch bool, httpListenAddr string, options ...HTTPOption) error {
	m.logger.Info("starting chain operator")
	m.OnTerminating(func(_ error) {
		m.logger.Info("chain operator terminating")
		m.cleanUp()
	})

	m.logger.Info("launching operator HTTP server", zap.String("http_listen_addr", httpListenAddr))
	m.httpServer = m.RunHTTPServer(httpListenAddr, options...)

	if m.options.EnableSupervisorMonitoring {
		if monitorable, ok := m.superviser.(manageos.MonitorableChainSuperviser); ok {
			go monitorable.Monitor()
		}
	}

	err := m.bootstrap()
	if err != nil {
		return fmt.Errorf("unable to bootstrap chain: %s", err)
	}

	if startOnLaunch {
		m.logger.Debug("sending initial start command")
		m.commandChan <- &Command{cmd: "start", logger: m.logger}
	}

	for {
		m.logger.Info("operator ready to receive commands")
		select {
		case <-m.superviser.Stopped(): // stopped outside of a command that was expecting it
			if m.attemptedAutoRestore || time.Since(m.lastStartCommand) > 10*time.Second {
				m.Shutdown(fmt.Errorf("Instance `%s` stopped (exit code: %d). Shutting down.", m.superviser.GetName(), m.superviser.LastExitCode()))
				if m.options.StartFailureHandlerFunc != nil {
					m.options.StartFailureHandlerFunc()
				}
				break
			}
			m.logger.Warn("Instance stopped. Attempting restore from snapshot", zap.String("command", m.superviser.GetCommand()))
			m.attemptedAutoRestore = true
			switch m.options.AutoRestoreSource {
			case "backup":
				if err := m.runCommand(&Command{
					cmd:    "restore",
					logger: m.logger,
				}); err != nil {
					m.Shutdown(fmt.Errorf("attempted restore failed"))
					if m.options.StartFailureHandlerFunc != nil {
						m.options.StartFailureHandlerFunc()
					}
				}
			case "snapshot":
				if err := m.runCommand(&Command{
					cmd:    "snapshot_restore",
					logger: m.logger,
				}); err != nil {
					m.Shutdown(fmt.Errorf("attempted restore failed"))
					if m.options.StartFailureHandlerFunc != nil {
						m.options.StartFailureHandlerFunc()
					}
				}
			}

		case <-m.Terminating():
			m.logger.Info("operator terminating, ending run/loop")
			m.runCommand(&Command{cmd: "maintenance"})
			m.logger.Info("operator run maintenance command")
			return nil

		case cmd := <-m.commandChan:
			if cmd.cmd == "start" { // start 'sub' commands after a restore do NOT come through here
				m.lastStartCommand = time.Now()
				m.attemptedAutoRestore = false
			}
			err := m.runCommand(cmd)
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

func (m *Operator) cleanUp() {
	m.logger.Info("chain operator shutting down")

	wg := &sync.WaitGroup{}
	wg.Add(2)

	go func() {
		// We give `(shutown_delay / 2)` time for http server to quit
		ctx, cancel := context.WithTimeout(context.Background(), time.Duration(float64(m.options.ShutdownDelay)/2.0))
		defer cancel()

		if m.httpServer != nil {
			if err := m.httpServer.Shutdown(ctx); err != nil {
				m.logger.Error("unable to close http server gracefully", zap.Error(err))
			}
		}

		wg.Done()
	}()

	go func() {
		err := m.superviser.Stop()
		if err != nil {
			m.logger.Error("unable to close superviser gracefully", zap.Error(err))
		}

		wg.Done()
	}()

	// FIXME: How could we have a timeout or so we do not wait forever!
	m.logger.Info("chain operator wait on group")
	wg.Wait()
	m.logger.Info("chain operator clean up done")
}

func (m *Operator) runSubCommand(name string, parentCmd *Command) error {
	return m.runCommand(&Command{cmd: name, returnch: parentCmd.returnch, logger: m.logger})
}

// runCommand does its work, and returns an error for irrecoverable states.
func (m *Operator) runCommand(cmd *Command) error {
	m.logger.Info("received operator command", zap.String("command", cmd.cmd), zap.Reflect("params", cmd.params))
	switch cmd.cmd {
	case "maintenance":
		m.logger.Info("preparing to stop process")
		if err := m.superviser.Stop(); err != nil {
			return err
		}

		// Careful, we are now "stopped". Every other case can handle that state.
		m.logger.Info("successfully put in maintenance")

	case "restore":
		m.logger.Info("preparing for restore")
		backupable, ok := m.superviser.(manageos.BackupableChainSuperviser)
		if !ok {
			cmd.Return(errors.New("the chain superviser does not support backups"))
			return nil
		}

		m.logger.Info("asking chain superviser to restore a backup")
		if err := m.superviser.Stop(); err != nil {
			return err
		}

		backupName := "latest"
		if b, ok := cmd.params["backupName"]; ok {
			backupName = b
		}

		if err := backupable.RestoreBackup(backupName, m.options.BackupTag, m.options.BackupStoreURL); err != nil {
			return err
		}

		return m.runSubCommand("start", cmd)

	case "volumesnapshot":
		m.logger.Info("preparing for volumesnapshot")
		volumesnapshotable, ok := m.superviser.(manageos.VolumeSnapshotableChainSuperviser)
		if !ok {
			cmd.Return(errors.New("the chain superviser does not support volume snapshot"))
			return nil
		}

		lastBlockSeen := m.superviser.LastSeenBlockNum()
		if lastBlockSeen == 0 {
			cmd.Return(errors.New("volumesnapshot: invalid lastBlockSeen 0. Cowardly refusing to take a snapshot."))
			return nil
		}

		m.logger.Info("asking chain superviser to take a volume snapshot")
		if err := m.superviser.Stop(); err != nil {
			return err
		}

		if err := volumesnapshotable.TakeVolumeSnapshot(m.options.VolumeSnapshotAppVer, m.options.Project, m.options.Namespace, m.options.Pod, m.options.PVCPrefix, lastBlockSeen); err != nil {
			cmd.Return(err)
			// restart geth even if snapshot failed...
		}

		return m.runSubCommand("start", cmd)

	case "backup":
		m.logger.Info("preparing for backup")
		backupable, ok := m.superviser.(manageos.BackupableChainSuperviser)
		if !ok {
			cmd.Return(errors.New("the chain superviser does not support backups"))
			return nil
		}

		m.logger.Info("asking chain superviser to take a backup")
		if err := m.superviser.Stop(); err != nil {
			return err
		}

		if err := backupable.TakeBackup(m.options.BackupTag, m.options.BackupStoreURL); err != nil {
			return err
		}

		return m.runSubCommand("start", cmd)

	case "snapshot":
		m.logger.Info("preparing for snapshot")
		snapshotable, ok := m.superviser.(manageos.SnapshotableChainSuperviser)
		if !ok {
			cmd.Return(fmt.Errorf("the chain superviser does not support snapshots"))
			return nil
		}

		if err := snapshotable.TakeSnapshot(m.getSnapshotStore(), m.options.NumberOfSnapshotsToKeep); err != nil {
			cmd.Return(fmt.Errorf("unable to take snapshot: %s", err))
			return nil
		}

		m.logger.Info("snapshot completed")

	case "snapshot_restore":
		m.logger.Info("preparing for performing a snapshot restore")
		snapshotable, ok := m.superviser.(manageos.SnapshotableChainSuperviser)
		if !ok {
			cmd.Return(errors.New("the chain superviser does not support snapshots"))
			return nil
		}

		m.logger.Info("asking chain superviser to stop due to snapshot restore command")
		if err := m.superviser.Stop(); err != nil {
			return err
		}

		snapshotName := "latest"
		if b, ok := cmd.params["snapshotName"]; ok {
			snapshotName = b
		}

		err := m.restoreSnapshot(snapshotable, snapshotName)
		if err != nil {
			return err
		}

		m.logger.Warn("restarting node from snapshot, the restart will perform the actual snapshot restoration")
		return m.runSubCommand("start", cmd)

	case "reload":
		m.logger.Info("preparing for reload")
		if err := m.superviser.Stop(); err != nil {
			return err
		}

		return m.runSubCommand("start", cmd)

	case "safely_resume_production":
		m.logger.Info("preparing for safely resume production")
		producer, ok := m.superviser.(manageos.ProducerChainSuperviser)
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
			m.logger.Info("resuming production of blocks")
			err := producer.ResumeProduction()
			if err != nil {
				cmd.Return(fmt.Errorf("error resuming production of blocks: %s", err))
				return nil
			}

			m.logger.Info("successfully resumed producer")

		} else {
			m.logger.Info("block production was already running, doing nothing")
		}

		m.logger.Info("successfully resumed block production")

	case "safely_pause_production":
		m.logger.Info("preparing for safely pause production")
		producer, ok := m.superviser.(manageos.ProducerChainSuperviser)
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
			m.logger.Info("block production is already paused, command is a no-op")
			return nil
		}

		m.logger.Info("waiting to pause the producer")
		err = producer.WaitUntilEndOfNextProductionRound(3 * time.Minute)
		if err != nil {
			cmd.Return(fmt.Errorf("timeout waiting for production round: %s", err))
			return nil
		}

		m.logger.Info("pausing block production")
		err = producer.PauseProduction()
		if err != nil {
			cmd.Return(fmt.Errorf("unable to pause production correctly: %s", err))
			return nil
		}

		m.logger.Info("successfully paused block production")

	case "safely_reload":
		m.logger.Info("preparing for safely reload")
		producer, ok := m.superviser.(manageos.ProducerChainSuperviser)
		if ok && producer.IsActiveProducer() {
			m.logger.Info("waiting right after production round")
			err := producer.WaitUntilEndOfNextProductionRound(3 * time.Minute)
			if err != nil {
				cmd.Return(fmt.Errorf("timeout waiting for production round: %s", err))
				return nil
			}
		}

		m.logger.Info("issuing 'reload' now")
		emptied := false
		for !emptied {
			select {
			case interimCmd := <-m.commandChan:
				m.logger.Info("emptying command queue while safely_reload was running, dropped", zap.Any("interim_cmd", interimCmd))
			default:
				emptied = true
			}
		}

		return m.runSubCommand("reload", cmd)

	case "start", "resume":
		m.logger.Info("preparing for start")
		if m.superviser.IsRunning() {
			m.logger.Info("chain is already running")
			return nil
		}

		m.logger.Info("preparing to start chain")

		var options []manageos.StartOption
		if value := cmd.params["debug-deep-mind"]; value != "" {
			if value == "true" {
				options = append(options, manageos.EnableDebugDeepmindOption)
			} else {
				options = append(options, manageos.DisableDebugDeepmindOption)
			}
		}

		if err := m.superviser.Start(options...); err != nil {
			return fmt.Errorf("error starting chain superviser: %s", err)
		}

		m.logger.Info("successfully start service")
		m.ReadyFunc()

	case "shutdown":
		m.logger.Info("preparing for shutdown")
		if err := m.superviser.Stop(); err != nil {
			m.logger.Error("stopping nodeos failed, continuing shutdown anyway", zap.Error(err))
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

func (m *Operator) bootstrap() error {
	// forcing restore here
	if m.options.RestoreBackupName != "" {
		m.logger.Info("Performing Bootstrap from Backup")
		return m.bootstrapFromBackup(m.options.RestoreBackupName)
	}
	if m.options.RestoreSnapshotName != "" {
		m.logger.Info("Performing Bootstrap from Snapshot")
		return m.bootstrapFromSnapshot(m.options.RestoreSnapshotName)
	}

	if m.superviser.HasData() {
		return nil
	}

	if m.options.BootstrapDataURL != "" {
		m.logger.Info("chain has no prior data and bootstrapDataURL is set. Attempting bootstrap from URL")
		err := m.bootstrapFromDataURL(m.options.BootstrapDataURL)
		if err != nil {
			m.logger.Warn("could not bootstrap from URL", zap.Error(err))
		} else {
			m.logger.Info("success bootstrap from URL")
			return nil
		}
	}

	switch m.options.AutoRestoreSource {
	case "backup":
		m.logger.Info("chain has no prior data and autoRestoreMethod is set to backup. Attempting restore from backup")
		err := m.bootstrapFromBackup("latest")
		m.logger.Warn("could not bootstrap from Backup", zap.Error(err))

	case "snapshot":
		m.logger.Info("chain has no prior data and autoRestoreMethod is set to snapshot. Attempting restore from snapshot")
		err := m.bootstrapFromSnapshot("latest")
		m.logger.Info("could not bootstrap from snapshot", zap.Error(err))

	}

	return nil
}

func (m *Operator) bootstrapFromDataURL(dataURL string) error {
	m.logger.Debug("bootstraping from pre-existing data prior starting process")
	bootstrapable, ok := m.superviser.(manageos.BootstrapableChainSuperviser)
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

func (m *Operator) bootstrapFromSnapshot(snapshotName string) error {
	m.logger.Debug("restoring snapshot prior starting process")
	snapshotable, ok := m.superviser.(manageos.SnapshotableChainSuperviser)
	if !ok {
		return errors.New("the chain superviser does not support snapshots")
	}

	return m.restoreSnapshot(snapshotable, snapshotName)
}

func (m *Operator) bootstrapFromBackup(backupName string) error {
	m.logger.Debug("restoring backup prior starting process")
	backupable, ok := m.superviser.(manageos.BackupableChainSuperviser)
	if !ok {
		return errors.New("the chain superviser does not support backups")
	}

	err := backupable.RestoreBackup(backupName, m.options.BackupTag, m.options.BackupStoreURL)
	if err != nil {
		return fmt.Errorf("unable to restore backup %q: %s", backupName, err)
	}

	return nil
}
func (m *Operator) SetMaintenance() {
	m.logger.Info("setting maintenance mode")
	m.commandChan <- &Command{cmd: "maintenance", logger: m.logger}
}

func (m *Operator) restoreSnapshot(snapshotable manageos.SnapshotableChainSuperviser, snapshotName string) error {
	if m.snapshotStore == nil {
		m.Shutdown(errors.New("trying to get snapshot store, but instance is nil, have you provided --snapshot-store-url flag?"))
	}

	if err := snapshotable.RestoreSnapshot(snapshotName, m.snapshotStore); err != nil {
		return fmt.Errorf("unable to restore snapshot %q: %s", snapshotName, err)
	}

	return nil
}

func (m *Operator) getSnapshotStore() dstore.Store {
	if m.snapshotStore == nil {
		m.Shutdown(errors.New("trying to get snapshot store, but instance is nil, have you provided --snapshot-store-url flag?"))
	}

	return m.snapshotStore
}

func (m *Operator) ConfigureAutoBackup(autoBackupInterval time.Duration, autoBackupBlockFrequency int) {
	if autoBackupInterval != 0 {
		go m.RunEveryPeriod(autoBackupInterval, "backup")
	}

	if autoBackupBlockFrequency != 0 {
		go m.RunEveryXBlock(uint32(autoBackupBlockFrequency), "backup")
	}
}

func (m *Operator) ConfigureAutoSnapshot(autoSnapshotInterval time.Duration, autoSnapshotBlockFrequency int) {
	if autoSnapshotInterval != 0 {
		go m.RunEveryPeriod(autoSnapshotInterval, "snapshot")
	}

	if autoSnapshotBlockFrequency != 0 {
		go m.RunEveryXBlock(uint32(autoSnapshotBlockFrequency), "snapshot")
	}
}

func (m *Operator) ConfigureAutoVolumeSnapshot(autoVolumeSnapshotInterval time.Duration, autoVolumeSnapshotBlockFrequency int, autoVolumeSnapshotSpecificBlocks []uint64) {
	if autoVolumeSnapshotInterval != 0 {
		go m.RunEveryPeriod(autoVolumeSnapshotInterval, "volumesnapshot")
	}

	if autoVolumeSnapshotBlockFrequency != 0 {
		go m.RunEveryXBlock(uint32(autoVolumeSnapshotBlockFrequency), "volumesnapshot")
	}

	if len(autoVolumeSnapshotSpecificBlocks) > 0 {
		go m.RunAtSpecificBlocks(autoVolumeSnapshotSpecificBlocks, "volumesnapshot")
	}
}

// RunEveryPeriod will skip a run if Nodeos is NOT alive when period expired.
func (m *Operator) RunEveryPeriod(period time.Duration, commandName string) {
	for {
		time.Sleep(1)
		if m.superviser.IsRunning() {
			break
		}
	}

	ticker := time.NewTicker(period)
	for {
		select {
		case <-ticker.C:
			if m.superviser.IsRunning() {
				m.commandChan <- &Command{cmd: commandName, logger: m.logger}
			}
		}
	}
}

func (m *Operator) RunAtSpecificBlocks(specificBlocks []uint64, commandName string) {
	m.logger.Info("Scheduled for running a job a specific blocks", zap.String("command_name", commandName), zap.Any("specific_blocks", specificBlocks))
	sort.Slice(specificBlocks, func(i, j int) bool { return specificBlocks[i] < specificBlocks[j] })
	nextIndex := 0
	for {
		time.Sleep(1 * time.Second)
		head := m.superviser.LastSeenBlockNum()
		if head == 0 {
			continue
		}

		if head > specificBlocks[nextIndex] {
			m.commandChan <- &Command{cmd: commandName, logger: m.logger}
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

func (m *Operator) RunEveryXBlock(freq uint32, commandName string) {
	var lastHeadReference uint64
	for {
		time.Sleep(1 * time.Second)
		lastSeenBlockNum := m.superviser.LastSeenBlockNum()
		if lastSeenBlockNum == 0 {
			continue
		}

		if lastHeadReference == 0 {
			lastHeadReference = lastSeenBlockNum
		}

		if lastSeenBlockNum > lastHeadReference+uint64(freq) {
			m.commandChan <- &Command{cmd: commandName, logger: m.logger}
			lastHeadReference = lastSeenBlockNum
		}
	}
}
