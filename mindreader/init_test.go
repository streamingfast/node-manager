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
	"os"
	"testing"
	"time"

	"github.com/dfuse-io/bstream"
	"github.com/streamingfast/logging"
)

func init() {
	if os.Getenv("DEBUG") != "" {
		zlog = logging.MustCreateLogger()
	}
}

type TestStore struct {
	blocks        []*bstream.Block
	receivedBlock chan *bstream.Block
}

func NewTestStore() *TestStore {
	return &TestStore{
		blocks:        []*bstream.Block{},
		receivedBlock: make(chan *bstream.Block),
	}
}

func (s *TestStore) init() error {
	return nil
}

func (s *TestStore) cleanup() {
}

func (s *TestStore) storeBlock(block *bstream.Block) error {
	s.blocks = append(s.blocks, block)
	s.receivedBlock <- block
	return nil
}

func (s *TestStore) consumeBlockFromChannel(t *testing.T, timeout time.Duration) *bstream.Block {
	t.Helper()

	select {
	case blk := <-s.receivedBlock:
		return blk
	case <-time.After(timeout):
		t.Errorf("should have read a block after %s", timeout)
	}

	return nil
}

func (s *TestStore) uploadFiles() error {
	return nil
}
