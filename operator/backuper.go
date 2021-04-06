package operator

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

func (o *Operator) RegisterBackupModule(name string, mod BackupModule) error {
	if o.backupModules == nil {
		o.backupModules = make(map[string]BackupModule)
	}
	if _, ok := o.backupModules[name]; ok {
		return fmt.Errorf("backup module %scannot be registered twice", name)
	}
	o.backupModules[name] = mod
	if o.backupModules == nil {
		o.backupModules = make(map[string]BackupModule)
	}
	return nil
}

func (o *Operator) RegisterBackupSchedule(sched *BackupSchedule) {
	o.backupSchedules = append(o.backupSchedules, sched)
}

func selectBackupModule(mods map[string]BackupModule, optionalName string) (BackupModule, error) {
	if len(mods) == 0 {
		return nil, fmt.Errorf("no registered backup modules")
	}

	if optionalName != "" {
		chosen, ok := mods[optionalName]
		if !ok {
			return nil, fmt.Errorf("invalid backup module: %s", optionalName)
		}
		return chosen, nil
	}

	if len(mods) > 1 {
		var modNames []string
		for k := range mods {
			modNames = append(modNames, k)
		}
		return nil, fmt.Errorf("more than one module registered, and none specified (%s)", strings.Join(modNames, ","))
	}

	for _, mod := range mods { // single element in map
		return mod, nil
	}
	return nil, fmt.Errorf("impossible path")

}

func selectRestoreModule(choices map[string]BackupModule, optionalName string) (RestorableBackupModule, error) {
	mods := restorable(choices)
	if len(mods) == 0 {
		return nil, fmt.Errorf("none of the registered backup modules support 'restore'")
	}

	if optionalName != "" {
		chosen, ok := mods[optionalName]
		if !ok {
			return nil, fmt.Errorf("invalid restorable backup module: %s", optionalName)
		}
		return chosen, nil
	}

	if len(mods) > 1 {
		var modNames []string
		for k := range mods {
			modNames = append(modNames, k)
		}
		return nil, fmt.Errorf("more than one restorable module registered, and none specified (%s)", strings.Join(modNames, ","))
	}

	for _, mod := range mods { // single element in map
		return mod, nil
	}
	return nil, fmt.Errorf("impossible path")

}

func selectListableBackupModule(choices map[string]BackupModule, optionalName string) (ListableBackupModule, error) {
	mods := listable(choices)
	if len(mods) == 0 {
		return nil, fmt.Errorf("none of the registered backup modules support 'list'")
	}

	if optionalName != "" {
		chosen, ok := mods[optionalName]
		if !ok {
			return nil, fmt.Errorf("invalid listable backup module: %s", optionalName)
		}
		return chosen, nil
	}

	if len(mods) > 1 {
		var modNames []string
		for k := range mods {
			modNames = append(modNames, k)
		}
		return nil, fmt.Errorf("more than one listable module registered, and none specified (%s)", strings.Join(modNames, ","))
	}

	for _, mod := range mods { // single element in map
		return mod, nil
	}
	return nil, fmt.Errorf("impossible path")

}

func restorable(in map[string]BackupModule) map[string]RestorableBackupModule {
	out := make(map[string]RestorableBackupModule)
	for k, v := range in {
		if rest, ok := v.(RestorableBackupModule); ok {
			out[k] = rest
		}
	}
	return out
}

func listable(in map[string]BackupModule) map[string]ListableBackupModule {
	out := make(map[string]ListableBackupModule)
	for k, v := range in {
		if listable, ok := v.(ListableBackupModule); ok {
			out[k] = listable
		}
	}
	return out
}

type BackupModule interface {
	RequiresStop() bool
	Backup(lastSeenBlockNum uint32) (string, error)
}
type ListableBackupModule interface {
	BackupModule
	List(params map[string]string) ([]string, error)
}
type RestorableBackupModule interface {
	BackupModule
	Restore(name string) error
}

type BackupSchedule struct {
	BlocksBetweenRuns     int
	TimeBetweenRuns       time.Duration
	RequiredHostnameMatch string // will not run backup if !empty env.Hostname != HostnameMatch
	BackuperName          string // must match id of backupModule
}

func NewBackupSchedule(freqBlocks, freqTime, requiredHostname, backuperName string) (*BackupSchedule, error) {

	switch {
	case freqBlocks != "":
		freqUint, err := strconv.ParseUint(freqBlocks, 10, 64)
		if err != nil || freqUint == 0 {
			return nil, fmt.Errorf("invalid value for freq_block in backup schedule (err: %w)", err)
		}
		return &BackupSchedule{
			BlocksBetweenRuns:     int(freqUint),
			RequiredHostnameMatch: requiredHostname,
			BackuperName:          backuperName,
		}, nil

	case freqTime != "":
		freqTime, err := time.ParseDuration(freqTime)
		if err != nil || freqTime < time.Minute {
			return nil, fmt.Errorf("invalid value for freq_time in backup schedule(duration: %s, err: %w)", freqTime, err)
		}
		return &BackupSchedule{
			TimeBetweenRuns:       freqTime,
			RequiredHostnameMatch: requiredHostname,
			BackuperName:          backuperName,
		}, nil
	}
	return nil, fmt.Errorf("schedule created without any frequency value")

}
