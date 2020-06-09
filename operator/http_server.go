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

package operator

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/dfuse-io/manageos/superviser"
	"github.com/gorilla/mux"
	"go.uber.org/zap"
)

type HTTPOption func(r *mux.Router)

func (m *Operator) RunHTTPServer(httpListenAddr string, options ...HTTPOption) *http.Server {
	r := mux.NewRouter()
	r.HandleFunc("/v1/ping", m.pingHandler).Methods("GET")
	r.HandleFunc("/healthz", m.healthzHandler).Methods("GET")
	r.HandleFunc("/v1/healthz", m.healthzHandler).Methods("GET")
	r.HandleFunc("/v1/server_id", m.serverIDHandler).Methods("GET")
	r.HandleFunc("/v1/is_running", m.isRunningHandler).Methods("GET")
	r.HandleFunc("/v1/start_command", m.startcommandHandler).Methods("GET")
	r.HandleFunc("/v1/maintenance", m.maintenanceHandler).Methods("POST")
	r.HandleFunc("/v1/resume", m.resumeHandler).Methods("POST")
	r.HandleFunc("/v1/backup", m.backupHandler).Methods("POST")
	r.HandleFunc("/v1/restore", m.restoreHandler).Methods("POST")
	r.HandleFunc("/v1/list_backups", m.listBackupsHandler).Methods("GET")
	r.HandleFunc("/v1/volumesnapshot", m.volumeSnapshotHandler).Methods("POST")
	r.HandleFunc("/v1/snapshot", m.snapshotHandler).Methods("POST")
	r.HandleFunc("/v1/snapshot_restore", m.snapshotRestoreHandler).Methods("POST")
	r.HandleFunc("/v1/profiler/perf", m.perfProfilerHandler).Methods("POST")
	r.HandleFunc("/v1/reload", m.reloadHandler).Methods("POST")
	r.HandleFunc("/v1/safely_reload", m.safelyReloadHandler).Methods("POST")
	r.HandleFunc("/v1/safely_pause_production", m.safelyPauseProdHandler).Methods("POST")
	r.HandleFunc("/v1/safely_resume_production", m.safelyResumeProdHandler).Methods("POST")

	for _, opt := range options {
		opt(r)
	}

	m.logger.Info("starting webserver", zap.String("http_addr", httpListenAddr))
	err := r.Walk(func(route *mux.Route, router *mux.Router, ancestors []*mux.Route) error {
		pathTemplate, err := route.GetPathTemplate()
		if err == nil {
			methodsTmp, err := route.GetMethods()
			var methods string
			if err == nil {
				methods = strings.Join(methodsTmp, ",")
			} else {
				methods = "GET"
			}

			m.logger.Debug("walked route methods", zap.String("methods", methods), zap.String("path_template", pathTemplate))
		}
		return nil
	})

	if err != nil {
		m.logger.Error("walking route methods", zap.Error(err))
	}

	srv := &http.Server{Addr: httpListenAddr, Handler: r}
	go func() {
		if err := srv.ListenAndServe(); err != http.ErrServerClosed {
			m.logger.Info("http server did not close correctly")
			m.Shutdown(err)
		}
	}()

	return srv
}

func (m *Operator) pingHandler(w http.ResponseWriter, r *http.Request) {
	w.Write([]byte("pong\n"))
}

func (m *Operator) startcommandHandler(w http.ResponseWriter, r *http.Request) {
	command := "Command:\n" + m.superviser.GetCommand() + "\n"
	w.Write([]byte(command))
}

func (m *Operator) isRunningHandler(w http.ResponseWriter, r *http.Request) {
	w.Write([]byte(fmt.Sprintf(`{"is_running":%t}`, m.superviser.IsRunning())))
}

func (m *Operator) serverIDHandler(w http.ResponseWriter, r *http.Request) {
	id, err := m.superviser.ServerID()
	if err != nil {
		http.Error(w, "not ready", http.StatusServiceUnavailable)
		return
	}

	w.Write([]byte(id))
}

func (m *Operator) healthzHandler(w http.ResponseWriter, r *http.Request) {
	if !m.superviser.IsRunning() {
		http.Error(w, "not ready: chain is not running", http.StatusServiceUnavailable)
		return
	}

	if !m.chainReadiness.IsReady() {
		http.Error(w, "not ready: chain is not ready", http.StatusServiceUnavailable)
		return
	}

	w.Write([]byte("ready\n"))
}

func (m *Operator) reloadHandler(w http.ResponseWriter, r *http.Request) {
	m.triggerWebCommand("reload", nil, w, r)
}

func (m *Operator) safelyReloadHandler(w http.ResponseWriter, r *http.Request) {
	m.triggerWebCommand("safely_reload", nil, w, r)
}

func (m *Operator) safelyResumeProdHandler(w http.ResponseWriter, r *http.Request) {
	m.triggerWebCommand("safely_resume_production", nil, w, r)
}

func (m *Operator) safelyPauseProdHandler(w http.ResponseWriter, r *http.Request) {
	m.triggerWebCommand("safely_pause_production", nil, w, r)
}

func (m *Operator) restoreHandler(w http.ResponseWriter, r *http.Request) {
	params := make(map[string]string)
	for _, p := range []string{"backupName", "backupTag", "forceVerify"} {
		val := r.FormValue(p)
		if val != "" {
			params[p] = val
		}
	}
	m.triggerWebCommand("restore", params, w, r)
}

func (m *Operator) listBackupsHandler(w http.ResponseWriter, r *http.Request) {
	prefix := r.FormValue("prefix")
	backupTag := r.FormValue("backupTag")
	if backupTag == "" {
		backupTag = m.options.BackupTag
	}

	limit, err := strconv.ParseInt(r.FormValue("limit"), 10, 64)
	if err != nil || limit == 0 {
		limit = 20
	}
	offset, err := strconv.ParseInt(r.FormValue("offset"), 10, 64)

	backups, err := superviser.ListPitreosBackup(m.logger, backupTag, m.options.BackupStoreURL, prefix, int(limit), int(offset))
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(fmt.Sprintf("ERROR: listBackups failed: %s\n", err)))
		return
	}

	answer := ""
	for _, b := range backups {
		if b.Meta != nil {
			cnt, _ := json.Marshal(b.Meta) // no worries
			answer += fmt.Sprintf("- %s\t%s\n", b.Name, string(cnt))
		} else {
			answer += fmt.Sprintf("- %s\n", b.Name)
		}
	}
	w.Write([]byte(answer))
}

func (m *Operator) backupHandler(w http.ResponseWriter, r *http.Request) {
	m.triggerWebCommand("backup", nil, w, r)
}

func (m *Operator) volumeSnapshotHandler(w http.ResponseWriter, r *http.Request) {
	m.triggerWebCommand("volumesnapshot", nil, w, r)
}

func (m *Operator) snapshotHandler(w http.ResponseWriter, r *http.Request) {
	m.triggerWebCommand("snapshot", nil, w, r)
}

func (m *Operator) snapshotRestoreHandler(w http.ResponseWriter, r *http.Request) {
	params := make(map[string]string)
	for _, p := range []string{"snapshotName"} {
		val := r.FormValue(p)
		if val != "" {
			params[p] = val
		}
	}

	m.triggerWebCommand("snapshot_restore", params, w, r)
}

func (m *Operator) perfProfilerHandler(w http.ResponseWriter, r *http.Request) {
	if m.options.Profiler == nil {
		w.WriteHeader(http.StatusNotImplemented)
	}

	result, err := m.options.Profiler.Run()
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(err.Error()))
	}

	w.Write([]byte(result))
}

func (m *Operator) maintenanceHandler(w http.ResponseWriter, r *http.Request) {
	m.triggerWebCommand("maintenance", nil, w, r)
}

func (m *Operator) resumeHandler(w http.ResponseWriter, r *http.Request) {
	params := map[string]string{
		"debug-deep-mind": r.FormValue("debug-deep-mind"),
	}

	if params["debug-deep-mind"] == "" {
		params["debug-deep-mind"] = "false"
	}

	m.triggerWebCommand("resume", params, w, r)
}

func (m *Operator) triggerWebCommand(cmdName string, params map[string]string, w http.ResponseWriter, r *http.Request) {
	c := &Command{cmd: cmdName, logger: m.logger}
	c.params = params
	sync := r.FormValue("sync")
	if sync == "true" {
		m.sendCommandSync(c, w)
	} else {
		m.sendCommandAsync(c, w)
	}
}

func (m *Operator) sendCommandAsync(c *Command, w http.ResponseWriter) {
	m.commandChan <- c
	w.WriteHeader(http.StatusCreated)
	w.Write([]byte(fmt.Sprintf("%s command submitted\n", c.cmd)))
}

func (m *Operator) sendCommandSync(c *Command, w http.ResponseWriter) {
	c.returnch = make(chan error)
	m.commandChan <- c
	err := <-c.returnch
	if err == nil {
		w.Write([]byte(fmt.Sprintf("Success: %s completed\n", c.cmd)))
	} else {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(fmt.Sprintf("ERROR: %s failed: %s \n", c.cmd, err)))
	}

}
