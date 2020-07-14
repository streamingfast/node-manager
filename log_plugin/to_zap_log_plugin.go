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

// ToZapLogPlugin takes a line, and if it's not a DMLOG line or
// if we are actively debugging deep mind, will print the line to received
// logger instance.
type ToZapLogPlugin struct {
	logger        *zap.Logger
	debugDeepMind bool
}

func NewToZapLogPlugin(debugDeepMind bool, logger *zap.Logger) *ToZapLogPlugin {
	return &ToZapLogPlugin{
		debugDeepMind: debugDeepMind,
		logger:        logger,
	}
}

func (p *ToZapLogPlugin) DebugDeepMind(enabled bool) {
	p.debugDeepMind = enabled
}

func (p *ToZapLogPlugin) Close(_ error) {
}

func (p *ToZapLogPlugin) LogLine(in string) {
	if strings.HasPrefix(in, "DMLOG ") {
		if p.debugDeepMind {
			p.logger.Debug(in)
		}

		return
	}

	level := zap.DebugLevel
	if strings.HasPrefix(in, "<6>info") || strings.HasPrefix(in, "info") {
		level = zap.InfoLevel
	} else if strings.HasPrefix(in, "<3>error") || strings.HasPrefix(in, "error") {
		level = zap.ErrorLevel
	} else if strings.HasPrefix(in, "<4>warn") || strings.HasPrefix(in, "warn") {
		level = zap.WarnLevel
	}

	p.logger.Check(level, in).Write()
}
