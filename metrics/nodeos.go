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

package metrics

import "github.com/dfuse-io/dmetrics"

var NodeosMetricset = dmetrics.NewSet()

var NodeosCurrentStatus = NodeosMetricset.NewGauge("nodeos_current_status", "Current status for nodeos")
var NodeosSuccessfulSnapshots = NodeosMetricset.NewCounter("nodeos_successful_snapshots", "This counter increments every time that a state snapshot is completed successfully")
var NodeosIsBlockProducer = NodeosMetricset.NewGauge("nodeos_is_block_producer_total", "Is block producer (bp_mode and is not paused)")
var NodeosConnectedPeers = NodeosMetricset.NewGauge("nodeos_connected_peers_total", "Number of connected peers")
var NodeosDBSizeInfo = NodeosMetricset.NewGaugeVec("nodeos_db_size_info_bytes", []string{"metric"}, "DB size from Nodeos")

func SetNodeosIsBlockProducer(isProducer bool) {
	set := float64(0)
	if isProducer {
		set = 1
	}
	NodeosIsBlockProducer.SetFloat64(set)
}
