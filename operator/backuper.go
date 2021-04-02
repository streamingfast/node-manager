package operator

import (
	"fmt"
	"time"
)

// maybe we need a FailureManager
// maybe we also need a BootstrapManager

//	NewPitreos(config map[string]string, logger *zap.Logger)

//ex: "backup": {...}
//    "snapshot": ...
//var BackupModules map[string]BackupModuleInstance

//type BackupManager struct {
//	Schedules []BackupSchedule
//	Modules   map[string]BackupModuleInstance
//}
//

func (o *Operator) RegisterBackupModule(name string, mod BackupModule) error {
	if o.backupModules == nil {
		o.backupModules = make(map[string]BackupModule)
	}
	if _, ok := o.backupModules[name]; ok {
		return fmt.Errorf("backup module %scannot be registered twice", name)
	}
	o.backupModules[name] = mod
	return nil
}

func (o *Operator) RegisterBackupSchedule(sched *BackupSchedule) {
	o.backupSchedules = append(o.backupSchedules, sched)
}

type BackupModule interface {
	RequiresStop() bool
	Backup(lastSeenBlockNum uint32) (string, error)
}
type ListableBackupModule interface {
	List(params map[string]string) ([]string, error)
}
type RestorableBackupModule interface {
	Restore(name string) error
}

// app.Register()...
// in dfuse-ethereum app generation, flag parsing, etc.

// pitreosModule := NewPitreosBackuper(blockstoreAddr, skippedFiles, etc.)
// err := Operator.RegisterBackuper(name string, pitreosModule Backuper)
// err := Operator.ScheduleAutomaticBackup(schedule, pitreosModule) error --> here, we will lookup the name and save the actual backup command from module in schedule

// BOB calls http:///localhost:8080/backup?name=pitreos

// backup(name string)

// for _, backuper := range backupers {
// if backuper.name == name
//}

// registry:  backupers
//
// schedule:

// modules: map[string name] BackupModule
// schedule: [name, scheduleExpression

// on operator run: for _, sched := range schedules {
//  go run sched.Run()
//

type BackupSchedule struct {
	BlocksBetweenRuns     int
	TimeBetweenRuns       time.Duration
	RequiredHostnameMatch string // will not run backup if not empty env.Hostname != HostnameMatch
	Backuper              BackupModule
}

//
//// example module
//type PitreosModule struct {
//	Tag      string
//	StoreURL string
//}
//
//func (*PitreosModule) RequiresStop() bool {
//	return true
//}
//
//type PVCSnapshotModule struct {
//	Tag      string
//	StoreURL string
//
//	AppVer    string
//	Namespace string //k8s namespace
//	Pod       string //k8s podname
//	PVCPrefix string
//	Project   string //gcp project
//}
//
//type StateSnapshotModule struct {
//	Tag      string
//	StoreURL string
//}
//
//func (*StateSnapshotModule) RequiresStop() bool {
//	return false
//}

//func ParseBackupConfigDefs(configStrings []string) (*BackupConfig, error) {
//	bc := BackupConfig{}
//	for _, cd := range configString {
//		err := applyConfigDef(cd, &bc)
//		if err != nil {
//			return nil, err
//		}
//	}
//	return &bc
//}
//
