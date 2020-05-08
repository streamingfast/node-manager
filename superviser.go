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

package manageos

import (
	"time"

	"github.com/dfuse-io/dstore"
)

type StartOption string

var EnableDebugDeepmindOption = StartOption("enable-debug-deep-mind")
var DisableDebugDeepmindOption = StartOption("disable-debug-deep-mind")

type ChainSuperviser interface {
	GetCommand() string
	GetName() string
	LastExitCode() int

	RegisterPostRestoreHandler(func())

	Start(options ...StartOption) error
	Stop() error

	HasData() bool
	IsRunning() bool
	Stopped() <-chan struct{}
	ServerID() (string, error)

	LastSeenBlockNum() uint64
}

type BootstrapableChainSuperviser interface {
	Bootstrap(dataName string, dataStore dstore.Store) error
}

type MonitorableChainSuperviser interface {
	Monitor()
}

type ProducerChainSuperviser interface {
	IsProducing() (bool, error)
	IsActiveProducer() bool

	ResumeProduction() error
	PauseProduction() error

	WaitUntilEndOfNextProductionRound(timeout time.Duration) error
}

type BackupableChainSuperviser interface {
	TakeBackup(backupTag string, backupStoreURL string) error
	RestoreBackup(backupName, backupTag string, backupStoreURL string) error
}

type SnapshotableChainSuperviser interface {
	TakeSnapshot(snapshotStore dstore.Store) error
	RestoreSnapshot(snapshotName string, snapshotStore dstore.Store) error
}

type VolumeSnapshotableChainSuperviser interface {
	TakeVolumeSnapshot(volumeSnapshotTag, project, namespace, pod, prefix string, lastSeenBlockNum uint64) error
}

type ProductionState int

const (
	StatePre       ProductionState = iota // Just before we produce, don't restart
	StateProducing                        // We're producing right now
	StatePost                             // Right after production
	StateStale                            // We haven't produced for 12 minutes
)

func (s ProductionState) String() string {
	switch s {
	case StatePre:
		return "pre"
	case StateProducing:
		return "producing"
	case StatePost:
		return "post"
	case StateStale:
		return "stale"
	default:
		return "unknown"
	}
}

type ProductionEvent int

const (
	EventProduced ProductionEvent = iota
	EventReceived
)
