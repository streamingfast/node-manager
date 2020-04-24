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

import (
	"os"

	"github.com/dfuse-io/dmetrics"
)

var metricset = dmetrics.NewSet()

//var HeadBlockTimeDrift = metricset.NewHeadTimeDrift("manager")
//var HeadBlockNumber = metricset.NewHeadBlockNumber("manager")
var SuccessfulBackups = metricset.NewCounter("successful_backups", "This counter increments every time that a backup is completed successfully")

func NewHeadBlockTimeDrift(serviceName string) *dmetrics.HeadTimeDrift {
	return metricset.NewHeadTimeDrift(serviceName)
}

func NewHeadBlockNumber(serviceName string) *dmetrics.HeadBlockNum {
	return metricset.NewHeadBlockNumber(serviceName)
}

func RegisterMetrics() {
	if _, err := os.Stat("/.dockerenv"); !os.IsNotExist(err) {
		// occurs whilte in docker env
		metricset.Register()
	}
}

func ServeMetrics() {
	dmetrics.Serve(":9102")
}
