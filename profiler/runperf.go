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

package profiler

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"time"

	"cloud.google.com/go/storage"
	"github.com/matishsiao/goInfo"
	"go.uber.org/zap"
)

const perfPath = "/usr/local/bin/perf"
const perfArchivePath = "/usr/local/bin/perf-archive"

func (p *Profiler) Prepare() error {
	if p.prepared {
		return nil
	}

	// if needed, download perf with our kernel version
	if _, err := os.Stat(perfPath); os.IsNotExist(err) {
		kernelVersion, err := kernelVersion()
		if err != nil {
			return err
		}
		err = p.downloadFile(fmt.Sprintf("perf/v%s/perf", kernelVersion), perfPath)
		if err != nil {
			if err.Error() == "storage: object doesn't exist" {
				err = p.downloadFile("perf/default/perf", perfPath)
				if err != nil {
					return err
				}
			}
			return err
		}
	}
	err := os.Chmod(perfPath, 0755)
	if err != nil {
		return err
	}

	// if needed, download perf-archive with our kernel version
	if _, err := os.Stat(perfArchivePath); os.IsNotExist(err) {
		kernelVersion, err := kernelVersion()
		if err != nil {
			return err
		}
		err = p.downloadFile(fmt.Sprintf("perf/v%s/perf-archive", kernelVersion), perfArchivePath)
		if err != nil {
			if err.Error() == "storage: object doesn't exist" {
				err = p.downloadFile("perf/default/perf-archive", perfArchivePath)
				if err != nil {
					return err
				}
			}
			return err
		}
	}
	err = os.Chmod(perfArchivePath, 0755)
	return err
}

func (p *Profiler) Test() error {
	return p.Prepare()
}

func (p *Profiler) Run() (string, error) {
	err := p.Prepare()
	if err != nil {
		return "", err
	}

	tmpDir, err := ioutil.TempDir("", "perf")
	defer os.RemoveAll(tmpDir)
	if err != nil {
		return "", err
	}

	os.Chdir(tmpDir)

	cmd := exec.Command("pidof", "nodeos")
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return "", err
	}
	if err := cmd.Start(); err != nil {
		return "", err
	}
	nodeosPid := ""
	s := bufio.NewScanner(stdout)
	for s.Scan() {
		nodeosPid = s.Text()
	}
	if err != nil {
		return "", err
	}
	if err := cmd.Wait(); err != nil {
		return "", err
	}

	name := perfPath
	arguments := []string{"record", "-p", nodeosPid, "--call-graph", "dwarf", "sleep", "10"}
	zlog.Info("executing command", zap.String("name", name), zap.Strings("arguments", arguments))
	runperf := exec.Command(name, arguments...)
	err = runperf.Run()
	if err != nil {
		return "", err
	}

	runperfArchive := exec.Command(perfArchivePath)
	err = runperfArchive.Run()
	if err != nil {
		return "", err
	}

	err = os.MkdirAll("symbols/opt/eosio/bin", 0755)
	if err != nil {
		return "", err
	}
	err = os.MkdirAll("symbols/.debug", 0755)
	if err != nil {
		return "", err
	}

	err = copyFile("/opt/eosio/bin/nodeos", "symbols/opt/eosio/bin/nodeos")
	if err != nil {
		return "", err
	}

	untar := exec.Command("tar", "xvf", "perf.data.tar.bz2", "-C", "symbols/.debug")
	err = untar.Run()
	if err != nil {
		return "", err
	}

	err = os.Remove("perf.data.tar.bz2")
	if err != nil {
		return "", err
	}

	hostname, err := os.Hostname()
	if err != nil {
		return "", err
	}

	newTarBall := fmt.Sprintf("perf.%s.%s.tar.gz", hostname, time.Now().Format("2006-01-02_15h04m"))
	retar := exec.Command("tar", "cvf", filepath.Join("/tmp", newTarBall), ".")
	err = retar.Run()
	if err != nil {
		return "", err
	}

	err = p.writeFileToGoogleStorage(filepath.Join("/tmp", newTarBall), filepath.Join(hostname, newTarBall))
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("Sent to gs://%s/%s\nGet it with 'gsutil cp ...', untar and run the following command:\n perf report --symfs symbols", p.reportsBucketName, filepath.Join(hostname, newTarBall)), nil

}

func New() (*Profiler, error) {
	p := Profiler{
		toolsBucketName:   "eoscanada-profiling-tools",
		reportsBucketName: "eoscanada-profiling-reports",
	}
	err := p.SetupStorage()
	return &p, err
}

func MaybeNew() *Profiler {
	p, err := New()
	if err != nil {
		zlog.Warn("unable to create profiler", zap.Error(err))
		return nil
	}

	return p
}

func (p *Profiler) SetupStorage() (err error) {
	ctx := context.Background()
	client, err := storage.NewClient(ctx)
	if err != nil {
		return err
	}
	p.toolsStorageBucket = client.Bucket(p.toolsBucketName)
	p.reportsStorageBucket = client.Bucket(p.reportsBucketName)
	return nil
}

func (p *Profiler) downloadFile(srcpath, destpath string) error {
	f, err := os.OpenFile(destpath, os.O_CREATE|os.O_WRONLY, 0755)
	if err != nil {
		return err
	}
	defer f.Close()

	d, err := p.readFromToolsGoogleStorage(srcpath)
	if err != nil {
		return err
	}

	n, err := f.Write(d)
	if err != nil {
		return err
	}
	zlog.Info("wrote some data to file: bytes", zap.String("destpath", destpath), zap.Int("n", n))
	return nil

}

func (p *Profiler) readFromToolsGoogleStorage(path string) (data []byte, err error) {

	if p.toolsStorageBucket == nil {
		return nil, fmt.Errorf("storage bucket not initialized")
	}
	ctx := context.Background()

	r, err := p.toolsStorageBucket.Object(path).NewReader(ctx)
	if err != nil {
		return nil, err
	}
	defer r.Close()

	return ioutil.ReadAll(r)
}

func (p *Profiler) writeFileToGoogleStorage(localFile, remoteFile string) error {
	if p.reportsStorageBucket == nil {
		return fmt.Errorf("storage bucket not initialized")
	}

	f, err := os.Open(localFile)
	if err != nil {
		return fmt.Errorf("opening local file %q: %s", localFile, err)
	}
	defer f.Close()

	ctx := context.Background()

	storageObj := p.reportsStorageBucket.Object(remoteFile)
	w := storageObj.NewWriter(ctx)
	defer w.Close()

	w.CacheControl = "public, max-age=86400"
	w.ContentType = "application/jsonl+gzip"

	if _, err := io.Copy(w, f); err != nil {
		f.Close()
		zlog.Error("writing to gs, removing object", zap.Error(err))
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = storageObj.Delete(ctx)
		return err
	}

	// flush the buffer to Google Storage
	if err := w.Close(); err != nil {
		return fmt.Errorf("flushing/closing google storage writer: %s", err)
	}

	_ = f.Close() // before deleting it
	if err = os.Remove(localFile); err != nil {
		return fmt.Errorf("error removing local file %q: %s", localFile, err)
	}
	return nil
}

func copyFile(fromFile, toFile string) error {
	from, err := os.Open(fromFile)
	if err != nil {
		return err
	}
	defer from.Close()

	to, err := os.OpenFile(toFile, os.O_RDWR|os.O_CREATE, 0755)
	if err != nil {
		return err
	}
	defer to.Close()

	_, err = io.Copy(to, from)
	return err
}

func kernelVersion() (string, error) {
	gi := goInfo.GetInfo()
	if gi.Kernel != "Linux" {
		return "", fmt.Errorf("This is not Linux...")
	}
	return gi.Core, nil
}

type Profiler struct {
	toolsBucketName      string
	reportsBucketName    string
	toolsStorageBucket   *storage.BucketHandle
	reportsStorageBucket *storage.BucketHandle
	prepared             bool
}
