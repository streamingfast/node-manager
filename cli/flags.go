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

package cli

import (
	"flag"
	"os"
	"time"
)

// Common Flags

var FlagBackupStoreURL *string
var FlagBackupTag *string
var FlagBootstrapDataURL *string
var FlagDebugDeepMind *bool
var FlagAutoRestoreLatest *bool
var FlagRestoreBackupName *string
var FlagRestoreSnapshotName *string
var FlagSnapshotStoreURL *string
var FlagPodName *string
var FlagNamespace *string
var FlagProject *string
var FlagPVCPrefix *string
var FlagShutdownDelay *time.Duration

func RegisterCommonFlags() {
	FlagBootstrapDataURL = flag.String("bootstrap-data-url", "", "The bootstrap data URL containing specific chain data used to initialized it.")
	FlagDebugDeepMind = flag.Bool("debug-deep-mind", false, "Wether to print all Deep Mind log lines or not")

	FlagAutoRestoreLatest = flag.Bool("auto-restore", false, "If true, will restore from the latest backup on boot if there is no blocks.log or if nodeos cannot start at all. Do not use on a single BP node")
	FlagBackupStoreURL = flag.String("backup-store-url", "", "Storage bucket with path prefix where backups should be done")
	FlagBackupTag = flag.String("backup-tag", "default", "tag to identify the backup")

	FlagRestoreBackupName = flag.String("restore-backup-name", "", "If non-empty, the node will be restored from that backup every time it starts.")
	FlagRestoreSnapshotName = flag.String("restore-snapshot-name", "", "If non-empty, the node will be restored from that snapshot when it starts.")

	FlagSnapshotStoreURL = flag.String("snapshot-store-url", "", "Storage bucket with path prefix where state snapshots should be done. Ex: gs://example/snapshots")

	FlagShutdownDelay = flag.Duration("shutdown-delay", 0*time.Second, "Delay before shutting manager when sigterm received")
	FlagPodName = flag.String("pod-name", os.Getenv("HOSTNAME"), "pod name, taken from env hostname by default. Used for snapshots")
	FlagPVCPrefix = flag.String("pvc-prefix", "datadir", "Prefix of the Persistent Volume Claim to match when taking volume snapshots")
	FlagNamespace = flag.String("namespace", os.Getenv("NAMESPACE"), "k8s namespace, taken from env by default. Used for snapshots")
	FlagProject = flag.String("project", os.Getenv("PROJECT"), "gcp project, taken from env by default. Used for snapshots")
}

// Backup Flags

var FlagAutoBackupModulo *int
var FlagAutoBackupPeriod *time.Duration
var FlagAutoVolumeSnapshotPeriod *time.Duration
var FlagAutoVolumeSnapshotModulo *int
var FlagVolumeSnapshotAppVer *string
var FlagAutoVolumeSnapshotSpecific *string

func RegisterBackupFlags() {
	FlagAutoBackupModulo = flag.Int("auto-backup-modulo", 0, "If non-zero, a backup will be taken every {auto-backup-modulo} block.")
	FlagAutoBackupPeriod = flag.Duration("auto-backup-period", 0, "If non-zero, a backup will be taken every period of {auto-backup-period}. Specify 1h, 2h...")
	FlagVolumeSnapshotAppVer = flag.String("volume-snapshot-appver", "geth-v1", "[application]-v[version_number], used for persistentVolume snapshots")
	FlagAutoVolumeSnapshotPeriod = flag.Duration("auto-volume-snapshot-period", 0, "If non-zero, a volume snapshot will be taken every period of {auto-volume-snapshot-period}. Specify 1h, 2h...")
	FlagAutoVolumeSnapshotModulo = flag.Int("auto-volume-snapshot-modulo", 0, "If non-zero, a volume snapshot will be taken every {auto-volume-snapshot-modulo} blocks. Ex: 500000")
	FlagAutoVolumeSnapshotSpecific = flag.String("target-volume-snapshot-specific", "", "Comma-separated list of block numbers where volume snapshots will be done automatically")
}

// Snapshot Flags

var FlagAutoSnapshotModulo *int
var FlagAutoSnapshotPeriod *time.Duration

func RegisterSnapshotFlags() {
	FlagAutoSnapshotModulo = flag.Int("auto-snapshot-modulo", 0, "If non-zero, a snapshot will be taken every {auto-snapshot-modulo} block.")
	FlagAutoSnapshotPeriod = flag.Duration("auto-snapshot-period", 0, "If non-zero, a snapshot will be taken every period of {auto-snapshot-period}. Specify 1h, 2h...")
}

// Mindreader Flags
var FlagGRPCAddr *string
var FlagBlockStoreURL *string
var FlagOneBlockStoreURL *string
var FlagMergedBlocksStoreURL *string
var FlagMergeUploadDirectly *bool
var FlagStartBlockNum *uint
var FlagStopBlockNum *uint
var FlagMindReadBlocksChanCapacity *int
var FlagWorkingDir *string

func RegisterMindreaderFlags() {
	FlagMergedBlocksStoreURL = flag.String("merged-blocks-store-url", "gs://example/blocks", "USE FOR REPROCESSING ONLY. Storage bucket with path prefix to write merged blocks logs to (in conjunction with --merge-and-upload-directly)")
	FlagMergeUploadDirectly = flag.Bool("merge-and-upload-directly", false, "USE FOR REPROCESSING ONLY. When enabled, do not write one-block files, sidestep the merger and write the merged 100-blocks logs directly to --merged-blocks-store-url")

	FlagOneBlockStoreURL = flag.String("oneblock-store-url", "gs://example/oneblocks", "Storage bucket with path prefix to write one-block file to")
	FlagGRPCAddr = flag.String("listen-grpc-addr", ":9000", "gRPC listening address for stream of blocks and transactions")

	FlagStartBlockNum = flag.Uint("start-block-num", 0, "Blocks that were produced with smaller block number then the given block num are skipped")
	FlagStopBlockNum = flag.Uint("stop-block-num", 0, "Shutdown mindreader when we the following 'stop-block-num' has been reached, inclusively.")
	FlagMindReadBlocksChanCapacity = flag.Int("mindread-blocks-chan-capacity", 10000, "Capacity of the channel holding blocks read by the mindreader. Process will shutdown nodeos/geth if the channel gets over 90% of that capacity to prevent horrible consequences. Raise this number when processing tiny blocks very quickly")
	FlagWorkingDir = flag.String("working-dir", "/tmp/manageos", "Storage bucket with path prefix to write archives to")
}

func ArchiveStoreURL() string {
	if *FlagMergeUploadDirectly {
		return *FlagMergedBlocksStoreURL
	}

	return *FlagOneBlockStoreURL
}
