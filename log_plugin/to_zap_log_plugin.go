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
	"regexp"
	"strings"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var NoDisplay = zapcore.Level(zap.FatalLevel + 10)

type ToZapLogPluginOption interface {
	apply(p *ToZapLogPlugin)
}

type toZapLogPluginOptionFunc func(p *ToZapLogPlugin)

func (s toZapLogPluginOptionFunc) apply(p *ToZapLogPlugin) {
	s(p)
}

func ToZapLogPluginAdjustLevels(mappings map[string]zapcore.Level) ToZapLogPluginOption {
	return toZapLogPluginOptionFunc(func(p *ToZapLogPlugin) {
		if len(mappings) > 0 {
			p.levelAdjustements = make(map[*regexp.Regexp]zapcore.Level)
			for regexString, adjustedLevel := range mappings {
				p.levelAdjustements[regexp.MustCompile(regexString)] = adjustedLevel
			}
		}
	})
}

func ToZapLogPluginKeepLastNLine(count int) ToZapLogPluginOption {
	return toZapLogPluginOptionFunc(func(p *ToZapLogPlugin) {
		p.lastLines.maxCount = count
	})
}

// ToZapLogPlugin takes a line, and if it's not a DMLOG line or
// if we are actively debugging deep mind, will print the line to received
// logger instance.
type ToZapLogPlugin struct {
	logger        *zap.Logger
	debugDeepMind bool

	lastLines         *lineRingBuffer
	levelAdjustements map[*regexp.Regexp]zapcore.Level
}

func NewToZapLogPlugin(debugDeepMind bool, logger *zap.Logger, options ...ToZapLogPluginOption) *ToZapLogPlugin {
	plugin := &ToZapLogPlugin{
		debugDeepMind: debugDeepMind,
		logger:        logger,
		lastLines:     &lineRingBuffer{},
	}

	for _, opt := range options {
		opt.apply(plugin)
	}

	return plugin
}

func (p *ToZapLogPlugin) LastLines() []string {
	return p.lastLines.lines()
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

	// TODO: Should we **not** record the line when the level adjustement skips it?
	p.lastLines.append(in)

	for lineRegex, adjustedLevel := range p.levelAdjustements {
		if lineRegex.MatchString(in) {
			if adjustedLevel == NoDisplay {
				// This is know ignored
				return
			}

			level = adjustedLevel
		}
	}

	p.logger.Check(level, in).Write()
}

type bufferElement struct {
	previous *bufferElement
	next     *bufferElement
	line     string
}

type lineRingBuffer struct {
	maxCount int

	count int
	tail  *bufferElement
	head  *bufferElement
}

func (b *lineRingBuffer) lines() (out []string) {
	if b.count == 0 {
		return nil
	}

	if b.count == 1 {
		return []string{b.head.line}
	}

	i := 0
	out = make([]string, b.count)
	for current := b.tail; current != nil; current = current.next {
		out[i] = current.line
		i++
	}

	return
}

func (b *lineRingBuffer) append(line string) {
	// If we keep nothing, there is nothing to do here
	if b.maxCount == 0 {
		return
	}

	oldHead := b.head
	b.head = &bufferElement{line: line, previous: oldHead}

	if oldHead != nil {
		oldHead.next = b.head
	}

	if b.tail == nil {
		b.tail = b.head
	}

	if b.count == b.maxCount {
		// We are full, we need to rotate stuff a bit
		b.tail = b.tail.next
	} else {
		// We are not full, let's just append a new line (so only update count)
		b.count++
	}
}
