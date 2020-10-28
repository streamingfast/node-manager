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
	"fmt"
	"os"
	"strconv"
	"strings"
)

var DebugLineLength = int64(1000)

func init() {
	if os.Getenv("DEBUG_LINE_LENGTH") != "" {
		DebugLineLength, _ = strconv.ParseInt(os.Getenv("DEBUG_LINE_LENGTH"), 10, 64)
	}
}

// ToConsoleLogPlugin takes a line, and if it's not a DMLOG line or
// if we are actively debugging deep mind, will print the line to the
// standard output
type ToConsoleLogPlugin struct {
	debugDeepMind bool
}

func NewToConsoleLogPlugin(debugDeepMind bool) *ToConsoleLogPlugin {
	return &ToConsoleLogPlugin{
		debugDeepMind: debugDeepMind,
	}
}

func (p *ToConsoleLogPlugin) DebugDeepMind(enabled bool) {
	p.debugDeepMind = enabled
}

func (p *ToConsoleLogPlugin) Close(_ error) {
}

func (p *ToConsoleLogPlugin) LogLine(in string) {
	if p.debugDeepMind || !strings.HasPrefix(in, "DMLOG ") {
		logLineLength := int64(len(in))

		// We really want to write lines to stdout and not through our logger, it's the purpose of our plugin!
		if logLineLength > DebugLineLength {
			fmt.Printf("%s ... bytes: %d\n", in[:DebugLineLength], (logLineLength - DebugLineLength))
		} else {
			fmt.Println(in)
		}
	}
}
