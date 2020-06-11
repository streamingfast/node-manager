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
	"strings"

	"go.uber.org/zap"
)

// ToConsoleLogPlugin takes a line, and if it's not a DMLOG line or
// if we are actively debugging deep mind, will print the line to the
// standard output
type ToZapLogPlugin struct {
	logger        *zap.Logger
	debugDeepMind bool
}

func NewToZapLogPlugin(logger *zap.Logger, debugDeepMind bool) *ToZapLogPlugin {
	return &ToZapLogPlugin{
		logger:        logger,
		debugDeepMind: debugDeepMind,
	}
}

func (p *ToZapLogPlugin) DebugDeepMind(enabled bool) {
	p.debugDeepMind = enabled
}

func (p *ToZapLogPlugin) LogLine(in string) {
	if p.debugDeepMind || !strings.HasPrefix(in, "DMLOG ") {
		if strings.HasPrefix(in, "<6>info") || strings.HasPrefix(in, "info") {
			p.logger.Info(in)
			return
		}
		if strings.HasPrefix(in, "<3>error") || strings.HasPrefix(in, "error") {
			p.logger.Error(in)
			return
		}
		if strings.HasPrefix(in, "<4>warn") || strings.HasPrefix(in, "warn") {
			p.logger.Warn(in)
			return
		}
		p.logger.Debug(in)
	}
}
