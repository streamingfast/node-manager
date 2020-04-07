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

package superviser

import (
	"os"
	"strconv"
	"strings"
	"time"

	pitreos "github.com/eoscanada/pitreos/lib"
	"go.uber.org/zap"
)

func ListPitreosBackup(logger *zap.Logger, backupTag, backupStoreURL, prefix string, limit, offset int) ([]*pitreos.ListableBackup, error) {
	p, err := GetPitreos(logger, backupStoreURL)
	if err != nil {
		return nil, err
	}

	var out []*pitreos.ListableBackup
	for {
		backups, err := p.ListBackups(limit, offset, prefix, true)
		if err != nil {
			return out, err
		}
		for _, b := range backups {
			if strings.HasSuffix(b.Name, backupTag) {
				out = append(out, b)
			}
		}
		if len(out) >= limit {
			break
		}
	}
	return out, nil
}

func GetPitreos(logger *zap.Logger, backupURL string, appendOnlyFiles ...string) (*pitreos.PITR, error) {
	storage, err := pitreos.SetupStorage(backupURL)
	if err != nil {
		return nil, err
	}

	timeout := 5 * time.Minute
	if newTimeout := os.Getenv("PITREOS_TIMEOUT"); newTimeout != "" {
		dur, err := time.ParseDuration(newTimeout)
		if err != nil {
			logger.Info("invalid PITREOS_TIMEOUT", zap.Error(err))
		} else {
			logger.Info("setting new timeout based on PITREOS_TIMEOUT", zap.Duration("timeout", dur))
			timeout = dur
		}
	}

	threads := 16
	if newThreads := os.Getenv("PITREOS_THREADS"); newThreads != "" {
		num, err := strconv.ParseInt(newThreads, 10, 32)
		if err != nil {
			logger.Info("invalid PITREOS_THREADS", zap.Error(err))
		} else {
			logger.Info("setting new threads on PITREOS_THREADS", zap.Int64("threads", num))
			threads = int(num)
		}
	}

	p := pitreos.New(50, threads, timeout, storage)

	p.AppendonlyFiles = append(p.AppendonlyFiles, appendOnlyFiles...)

	return p, nil
}
