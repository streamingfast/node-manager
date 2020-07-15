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

package mindreader

import (
	"encoding/binary"
	"fmt"
	"io/ioutil"
	"os"

	"github.com/google/renameio"
	"go.uber.org/zap"
)

type ContinuityChecker interface {
	IsLocked() bool
	Reset()
	Write(lastSeenBlockNum uint64) error
}

func NewContinuityChecker(filePath string, zlogger *zap.Logger) (*continuityChecker, error) {
	cc := &continuityChecker{
		filePath: filePath,
		zlogger:  zlogger,
	}
	err := cc.load()
	if err != nil {
		return nil, err
	}

	return cc, nil
}

type continuityChecker struct {
	highestSeenBlock uint64
	locked           bool
	filePath         string
	zlogger          *zap.Logger
}

func (cc *continuityChecker) IsLocked() bool {
	return cc.locked
}

func (cc *continuityChecker) Reset() {
	cc.zlogger.Info("resetting continuity checker")
	cc.highestSeenBlock = 0
	cc.locked = false

	err := os.Remove(cc.filePath)
	if err != nil && !os.IsNotExist(err) {
		cc.zlogger.Error("cannot remove continuity file", zap.String("lock_file_path", cc.filePath), zap.Error(err))
	}

	err = os.Remove(cc.lockFilePath())
	if err != nil && !os.IsNotExist(err) {
		cc.zlogger.Error("cannot remove lock file", zap.String("lock_file_path", cc.lockFilePath()), zap.Error(err))
	}
}

func (cc *continuityChecker) load() error {
	if _, err := os.Stat(cc.lockFilePath()); err == nil {
		cc.locked = true
	}

	defer cc.zlogger.Info("loading continuity checker info", zap.Bool("locked", cc.locked), zap.Uint64("highest_seen_block", cc.highestSeenBlock))

	b, err := ioutil.ReadFile(cc.filePath)
	if err != nil {
		if !os.IsNotExist(err) {
			return fmt.Errorf("ontinuity checker cannot read file: %s :%w", cc.filePath, err)
		}
		return nil
	}
	cc.highestSeenBlock = binary.LittleEndian.Uint64(b)
	return nil
}

func (cc *continuityChecker) lockFilePath() string {
	return cc.filePath + ".broken"
}
func (cc *continuityChecker) setLock() {
	cc.locked = true
	_, err := os.Create(cc.lockFilePath())
	if err != nil {
		cc.zlogger.Error("cannot create lock file", zap.String("lock_file_path", cc.lockFilePath()), zap.Error(err))
	}
}

// Write checks that the either:
// val =< highestSeenBlock OR val == highestSeenBlock+1 OR highestSeenBlock == 0
// it then updates the highestSeenBlock value if it needs to changed (on the cc and on disk)
// In case the value does not match these 3 conditions, (that block would create a hole
// in the continuity), the checker becomes locked, a lock file is written to disk, and an error
// is returned.
func (cc *continuityChecker) Write(val uint64) error {
	if cc.locked {
		return fmt.Errorf("ontinuity checker already locked")
	}
	if val <= cc.highestSeenBlock {
		return nil
	}
	if cc.highestSeenBlock != 0 && val > cc.highestSeenBlock+1 {
		cc.setLock()
		return fmt.Errorf("ontinuity checker failed: block %d would creates a hole after highest seen block: %d", val, cc.highestSeenBlock)
	}
	cc.highestSeenBlock = val
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, uint64(val))
	cc.zlogger.Debug("writing through ontinuity checker", zap.Uint64("highest_seen_block", cc.highestSeenBlock))
	return renameio.WriteFile(cc.filePath, b, os.FileMode(0644))
}
