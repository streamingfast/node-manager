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

package logplugin

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

// Some of our currently supported binaries log formats
// - `nodeos` - `info  2020-10-05T13:53:11.749 thread-0  http_plugin.cpp:895           add_handler          ] add api url: /v1/db_size/get (log_plugin/to_zap_log_plugin.go:107)`
// - `geth` -   `INFO [10-05|09:54:00.585] IPC endpoint closed                      url=/app/dfuse-data/miner/data/geth.ipc (log_plugin/to_zap_log_plugin.go:107)`
func TestToZapLogPlugin_KeepLastNLine(t *testing.T) {
	tests := []struct {
		name    string
		in      []string
		maxLine int
		out     []string
	}{
		{"empty", []string{}, 3, nil},
		{"single, not reached", []string{"a"}, 3, []string{"a"}},
		{"flush, not reached", []string{"a", "b", "c"}, 3, []string{"a", "b", "c"}},
		{"over, count", []string{"a", "b", "c", "d"}, 3, []string{"b", "c", "d"}},
		{"multiple over count", []string{"a", "b", "c", "d", "e", "f", "g"}, 3, []string{"e", "f", "g"}},

		{"max count 0 keeps nothing", []string{"a", "b", "c", "d", "e", "f", "g"}, 0, nil},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			plugin := NewToZapLogPlugin(false, zap.NewNop(), ToZapLogPluginKeepLastNLine(test.maxLine))

			for _, line := range test.in {
				plugin.LogLine(line)
			}

			assert.Equal(t, test.out, plugin.LastLines())
		})
	}
}
