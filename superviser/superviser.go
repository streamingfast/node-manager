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

package superviser

import (
	"sync"

	"github.com/ShinyTrinkets/overseer"
	"github.com/dfuse-io/manageos"
	logplugin "github.com/dfuse-io/manageos/log_plugin"
	"go.uber.org/zap"
)

func New(logger *zap.Logger, binary string, arguments []string) *Superviser {
	return &Superviser{
		Binary:    binary,
		Arguments: arguments,
		Logger:    logger,
	}
}

type Superviser struct {
	Binary    string
	Arguments []string
	Logger    *zap.Logger

	cmd     *overseer.Cmd
	cmdLock sync.Mutex

	logPlugins             []logplugin.LogPlugin
	logPluginsLock         sync.RWMutex
	handleStartFailureOnce func()
	HandlePostRestore      func()

	enableDeepMind bool
}

// RegisterPostRestoreHandler adds a function called after a restore from backup or from snapshot
func (s *Superviser) RegisterPostRestoreHandler(f func()) {
	s.HandlePostRestore = f
}

func (s *Superviser) RegisterStartFailureHandler(f func()) {
	s.handleStartFailureOnce = f
}

func (s *Superviser) RegisterLogPlugin(plugin logplugin.LogPlugin) {
	s.logPluginsLock.Lock()
	defer s.logPluginsLock.Unlock()

	s.logPlugins = append(s.logPlugins, plugin)
	s.Logger.Info("registered log plugin", zap.Int("plugin count", len(s.logPlugins)))
}

func (s *Superviser) GetLogPlugins() []logplugin.LogPlugin {
	s.logPluginsLock.RLock()
	defer s.logPluginsLock.RUnlock()

	return s.logPlugins
}

func (s *Superviser) setDeepMindDebug(enabled bool) {
	s.Logger.Info("setting deep mind debug mode", zap.Bool("enabled", enabled))
	for _, logPlugin := range s.logPlugins {
		if v, ok := logPlugin.(manageos.DeepMindDebuggable); ok {
			v.DebugDeepMind(enabled)
		}
	}
}

func (s *Superviser) Start(options ...manageos.StartOption) error {
	for _, opt := range options {
		if opt == manageos.EnableDebugDeepmindOption {
			s.setDeepMindDebug(true)
		}
		if opt == manageos.DisableDebugDeepmindOption {
			s.setDeepMindDebug(false)
		}
	}

	s.cmdLock.Lock()
	defer s.cmdLock.Unlock()

	if s.cmd != nil {
		if s.cmd.State == overseer.STARTING || s.cmd.State == overseer.RUNNING {
			s.Logger.Info("underlying process already running, nothing to do")
			return nil
		}

		if s.cmd.State == overseer.STOPPING {
			s.Logger.Info("underlying process is currently stopping, waiting for it to finish")
			<-s.cmd.Done()
		}
	}

	s.Logger.Info("creating new command instance and launch read loop", zap.String("binary", s.Binary), zap.Strings("arguments", s.Arguments))
	s.cmd = overseer.NewCmd(s.Binary, s.Arguments)

	go s.start(s.cmd)

	return nil
}

func (s *Superviser) Stop() error {
	s.cmdLock.Lock()
	defer s.cmdLock.Unlock()

	s.Logger.Info("supervisor received a stop request")

	if !s.isRunning() {
		s.Logger.Info("underlying process is not running, nothing to do")
		return nil
	}

	if s.cmd.State == overseer.STARTING || s.cmd.State == overseer.RUNNING {
		s.Logger.Info("stopping underlying process")
		err := s.cmd.Stop()
		if err != nil {
			return err
		}
	}

	// Blocks until command finished completely
	s.Logger.Debug("blocking until command actually ends")
	<-s.cmd.Done()
	s.cmd = nil

	return nil
}

func (s *Superviser) IsRunning() bool {
	s.cmdLock.Lock()
	defer s.cmdLock.Unlock()

	return s.isRunning()
}

// This one assuming the lock is properly held already
func (s *Superviser) isRunning() bool {
	if s.cmd == nil {
		return false
	}

	return s.cmd.State == overseer.STARTING || s.cmd.State == overseer.RUNNING || s.cmd.State == overseer.STOPPING
}

func (s *Superviser) start(cmd *overseer.Cmd) {
	statusChan := cmd.Start()

	for {
		select {
		case status := <-statusChan:
			s.Logger.Info("command terminated", zap.Any("status", status))
			if (status.Exit == -1 || (status.Exit > 0 && status.Runtime < 10)) && s.handleStartFailureOnce != nil {
				s.Logger.Warn("notifying starting failure handler because process exited with non-success code within 10 seconds")
				s.handleStartFailureOnce()
				s.handleStartFailureOnce = nil
			}
			break
		case line := <-cmd.Stdout:
			s.processLogLine(line)
		case line := <-cmd.Stderr:
			s.processLogLine(line)
		}
	}
}

func (s *Superviser) processLogLine(line string) {
	s.logPluginsLock.RLock()
	defer s.logPluginsLock.RUnlock()

	for _, plugin := range s.logPlugins {
		plugin.LogLine(line)
	}
}
