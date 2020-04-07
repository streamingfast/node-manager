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
	"fmt"
	"math/rand"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func tempFileName() string {
	return fmt.Sprintf("tmp_%d", rand.Int())
}

func TestContinuityChecker(t *testing.T) {
	tmp := tempFileName()

	cc, err := newContinuityChecker(tmp)
	require.NoError(t, err)

	defer func() {
		os.Remove(tmp)
		os.Remove(fmt.Sprintf("%s.broken", tmp))
	}()

	cc.Reset()

	require.NoError(t, cc.Write(10))
	assert.NoError(t, cc.Write(11))
	assert.NoError(t, cc.Write(9))
	assert.EqualValues(t, 11, cc.highestSeenBlock)
	assert.False(t, cc.locked)
	assert.Error(t, cc.Write(13))
	assert.True(t, cc.locked)

	cc2, err := newContinuityChecker(tmp)
	require.NoError(t, err)
	assert.True(t, cc2.locked)
	assert.Error(t, cc2.Write(10))

}
