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

package nodeos

import (
	"context"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"

	"github.com/dfuse-io/dstore"

	eos "github.com/eoscanada/eos-go"
	"github.com/dfuse-io/manageos/metrics"
	"go.uber.org/zap"
)

func (s *NodeosSuperviser) TakeSnapshot(snapshotStore dstore.Store) error {
	s.Logger.Info("asking nodeos API to create a snapshot")
	api := s.api
	snapshot, err := api.CreateSnapshot(context.Background())
	if err != nil {
		return fmt.Errorf("api call failed: %s", err)
	}

	filename := fmt.Sprintf("%010d-%s-snapshot.bin", eos.BlockNum(snapshot.HeadBlockID), snapshot.HeadBlockID)

	s.Logger.Info("saving state snapshot", zap.String("destination", filename))
	fileReader, err := os.Open(snapshot.SnapshotName)
	if err != nil {
		return fmt.Errorf("cannot open snapshot file: %s", err)
	}
	defer fileReader.Close()

	err = snapshotStore.WriteObject(filename, fileReader)
	if err != nil {
		return fmt.Errorf("cannot write snapshot to store: %s", err)
	}

	metrics.NodeosSuccessfulSnapshots.Inc()
	return os.Remove(snapshot.SnapshotName)
}

func (s *NodeosSuperviser) RestoreSnapshot(snapshotName string, snapshotStore dstore.Store) error {
	s.Logger.Info("getting snapshot from store", zap.String("snapshot_name", snapshotName))
	reader, err := snapshotStore.OpenObject(snapshotName)
	if err != nil {
		return fmt.Errorf("cannot get snapshot from gstore: %s", err)
	}
	defer reader.Close()

	os.MkdirAll(s.snapshotsDir, 0755)
	snapshotFile := filepath.Join(s.snapshotsDir, snapshotName)
	w, err := os.Create(snapshotFile)
	if err != nil {
		return err
	}
	defer w.Close()

	_, err = io.Copy(w, reader)
	if err != nil {
		return err
	}

	stateDir := path.Join(s.options.DataDir, "state")
	err = os.RemoveAll(stateDir)
	if err != nil {
		return fmt.Errorf("unable to delete %q directory: %s", stateDir, err)
	}

	reversibleState := path.Join(s.options.DataDir, "blocks", "reversible")
	err = os.RemoveAll(reversibleState)
	if err != nil {
		return fmt.Errorf("unable to delete %q directory: %s", reversibleState, err)
	}

	s.snapshotRestoreOnNextStart = true
	s.snapshotRestoreFilename = snapshotFile

	if s.HandlePostRestore != nil {
		s.HandlePostRestore()
	}

	return nil
}
