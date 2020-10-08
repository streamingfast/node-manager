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

	"github.com/dfuse-io/derr"
	"github.com/dfuse-io/node-manager/superviser"
	"github.com/gorilla/mux"
	"go.uber.org/zap"
)

type HTTPOption func(r *mux.Router)

func (o *Operator) RunHTTPServer(httpListenAddr string, options ...HTTPOption) *http.Server {
	r := mux.NewRouter()
	r.HandleFunc("/v1/ping", o.pingHandler).Methods("GET")
	r.HandleFunc("/healthz", o.healthzHandler).Methods("GET")
	r.HandleFunc("/v1/healthz", o.healthzHandler).Methods("GET")
	r.HandleFunc("/v1/server_id", o.serverIDHandler).Methods("GET")
	r.HandleFunc("/v1/is_running", o.isRunningHandler).Methods("GET")
	r.HandleFunc("/v1/start_command", o.startcommandHandler).Methods("GET")
	r.HandleFunc("/v1/maintenance", o.maintenanceHandler).Methods("POST")
	r.HandleFunc("/v1/resume", o.resumeHandler).Methods("POST")
	r.HandleFunc("/v1/backup", o.backupHandler).Methods("POST")
	r.HandleFunc("/v1/restore", o.restoreHandler).Methods("POST")
	r.HandleFunc("/v1/list_backups", o.listBackupsHandler).Methods("GET")
	r.HandleFunc("/v1/volumesnapshot", o.volumeSnapshotHandler).Methods("POST")
	r.HandleFunc("/v1/snapshot", o.snapshotHandler).Methods("POST")
	r.HandleFunc("/v1/snapshot_restore", o.snapshotRestoreHandler).Methods("POST")
	r.HandleFunc("/v1/profiler/perf", o.perfProfilerHandler).Methods("POST")
	r.HandleFunc("/v1/reload", o.reloadHandler).Methods("POST")
	r.HandleFunc("/v1/safely_reload", o.safelyReloadHandler).Methods("POST")
	r.HandleFunc("/v1/safely_pause_production", o.safelyPauseProdHandler).Methods("POST")
	r.HandleFunc("/v1/safely_resume_production", o.safelyResumeProdHandler).Methods("POST")

	for _, opt := range options {
		opt(r)
	}

	o.zlogger.Info("starting webserver", zap.String("http_addr", httpListenAddr))
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

			o.zlogger.Debug("walked route methods", zap.String("methods", methods), zap.String("path_template", pathTemplate))
		}
		return nil
	})

	if err != nil {
		o.zlogger.Error("walking route methods", zap.Error(err))
	}

	srv := &http.Server{Addr: httpListenAddr, Handler: r}
	go func() {
		if err := srv.ListenAndServe(); err != http.ErrServerClosed {
			o.zlogger.Info("http server did not close correctly")
			o.Shutdown(err)
		}
	}()

	return srv
}

func (o *Operator) pingHandler(w http.ResponseWriter, _ *http.Request) {
	_, _ = w.Write([]byte("pong\n"))
}

func (o *Operator) startcommandHandler(w http.ResponseWriter, _ *http.Request) {
	command := "Command:\n" + o.Superviser.GetCommand() + "\n"
	_, _ = w.Write([]byte(command))
}

func (o *Operator) isRunningHandler(w http.ResponseWriter, _ *http.Request) {
	_, _ = w.Write([]byte(fmt.Sprintf(`{"is_running":%t}`, o.Superviser.IsRunning())))
}

func (o *Operator) serverIDHandler(w http.ResponseWriter, _ *http.Request) {
	id, err := o.Superviser.ServerID()
	if err != nil {
		http.Error(w, "not ready", http.StatusServiceUnavailable)
		return
	}

	_, _ = w.Write([]byte(id))
}

func (o *Operator) healthzHandler(w http.ResponseWriter, _ *http.Request) {
	if !o.Superviser.IsRunning() {
		http.Error(w, "not ready: chain is not running", http.StatusServiceUnavailable)
		return
	}

	if !o.chainReadiness.IsReady() {
		http.Error(w, "not ready: chain is not ready", http.StatusServiceUnavailable)
		return
	}

	if o.aboutToStop.Load() || derr.IsShuttingDown() {
		http.Error(w, "not ready: chain about to stop", http.StatusServiceUnavailable)
		return
	}

	w.Write([]byte("ready\n"))
}

func (o *Operator) reloadHandler(w http.ResponseWriter, r *http.Request) {
	o.triggerWebCommand("reload", nil, w, r)
}

func (o *Operator) safelyReloadHandler(w http.ResponseWriter, r *http.Request) {
	o.triggerWebCommand("safely_reload", nil, w, r)
}

func (o *Operator) safelyResumeProdHandler(w http.ResponseWriter, r *http.Request) {
	o.triggerWebCommand("safely_resume_production", nil, w, r)
}

func (o *Operator) safelyPauseProdHandler(w http.ResponseWriter, r *http.Request) {
	o.triggerWebCommand("safely_pause_production", nil, w, r)
}

func (o *Operator) restoreHandler(w http.ResponseWriter, r *http.Request) {
	params := make(map[string]string)
	for _, p := range []string{"backupName", "backupTag", "forceVerify"} {
		val := r.FormValue(p)
		if val != "" {
			params[p] = val
		}
	}
	o.triggerWebCommand("restore", params, w, r)
}

func (o *Operator) listBackupsHandler(w http.ResponseWriter, r *http.Request) {
	prefix := r.FormValue("prefix")
	backupTag := r.FormValue("backupTag")
	if backupTag == "" {
		backupTag = o.options.BackupTag
	}

	limit, err := strconv.ParseInt(r.FormValue("limit"), 10, 64)
	if err != nil || limit == 0 {
		limit = 20
	}
	offset, err := strconv.ParseInt(r.FormValue("offset"), 10, 64)

	backups, err := superviser.ListPitreosBackup(o.zlogger, backupTag, o.options.BackupStoreURL, prefix, int(limit), int(offset))
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(fmt.Sprintf("ERROR: listBackups failed: %s\n", err)))
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
	_, _ = w.Write([]byte(answer))
}

func (o *Operator) backupHandler(w http.ResponseWriter, r *http.Request) {
	o.triggerWebCommand("backup", nil, w, r)
}

func (o *Operator) volumeSnapshotHandler(w http.ResponseWriter, r *http.Request) {
	o.triggerWebCommand("volumesnapshot", nil, w, r)
}

func (o *Operator) snapshotHandler(w http.ResponseWriter, r *http.Request) {
	o.triggerWebCommand("snapshot", nil, w, r)
}

func (o *Operator) snapshotRestoreHandler(w http.ResponseWriter, r *http.Request) {
	params := make(map[string]string)
	for _, p := range []string{"snapshotName"} {
		val := r.FormValue(p)
		if val != "" {
			params[p] = val
		}
	}

	o.triggerWebCommand("snapshot_restore", params, w, r)
}

func (o *Operator) perfProfilerHandler(w http.ResponseWriter, _ *http.Request) {
	if o.options.Profiler == nil {
		w.WriteHeader(http.StatusNotImplemented)
	}

	result, err := o.options.Profiler.Run()
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(err.Error()))
	}

	_, _ = w.Write([]byte(result))
}

func (o *Operator) maintenanceHandler(w http.ResponseWriter, r *http.Request) {
	o.triggerWebCommand("maintenance", nil, w, r)
}

func (o *Operator) resumeHandler(w http.ResponseWriter, r *http.Request) {
	params := map[string]string{
		"debug-deep-mind": r.FormValue("debug-deep-mind"),
	}

	if params["debug-deep-mind"] == "" {
		params["debug-deep-mind"] = "false"
	}

	o.triggerWebCommand("resume", params, w, r)
}

func (o *Operator) triggerWebCommand(cmdName string, params map[string]string, w http.ResponseWriter, r *http.Request) {
	c := &Command{cmd: cmdName, logger: o.zlogger}
	c.params = params
	sync := r.FormValue("sync")
	if sync == "true" {
		o.sendCommandSync(c, w)
	} else {
		o.sendCommandAsync(c, w)
	}
}

func (o *Operator) sendCommandAsync(c *Command, w http.ResponseWriter) {
	o.commandChan <- c
	w.WriteHeader(http.StatusCreated)
	_, _ = w.Write([]byte(fmt.Sprintf("%s command submitted\n", c.cmd)))
}

func (o *Operator) sendCommandSync(c *Command, w http.ResponseWriter) {
	c.returnch = make(chan error)
	o.commandChan <- c
	err := <-c.returnch
	if err == nil {
		w.Write([]byte(fmt.Sprintf("Success: %s completed\n", c.cmd)))
	} else {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(fmt.Sprintf("ERROR: %s failed: %s \n", c.cmd, err)))
	}

}
