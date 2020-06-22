# Change log

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [v0.0.1] - 2020-06-22

### Fixed:
* nodeos log levels are now properly decoded when going through zap logging engine instead of showing up as DEBUG
* mindreader does not get stuck anymore when trying to find an unexisting snapshot (it fails rapidly instead)

### Changed
* ContinuousChecker is not enabled by default now, use FailOnNonContinuousBlocks
* BREAKING: AutoRestoreLatest(bool) option becomes AutoRestoreSource (`backup`, `snapshot`)
* Nodeos unexpectedly shutting down now triggers a Shutdown of the whole app
* ShutdownDelay is now actually implemented for any action that will make nodeos unavailable (snapshot, volume_snapshot, backup, maintenance, shutdown..). It will report the app as "not ready" for that period before actually affecting the service.

### Added
* Options.DiscardAfterStopBlock, if not true, one-block-files will now be produced with the remaining blocks when MergeUploadDirectly is set. This way, they are not lost if you intend to restart without mergeUploadDirectly run a merger on these blocks later.
* App `nodeos_mindreader_stdin`, with a small subset of the features, a kind of "dumb mode" that only does the "mindreader" job (producing block files, relaying blocks through GRPC) on none of the "manager" job.
* Options.AutoSnapshotHostnameMatch(string) will only apply auto-snapshot parameters if os.Hostname() returns this string
* Options.AutoBackupHostnameMatch(string) will only apply auto-backup parameters if os.Hostname() returns this string
* Add FailOnNonContinuousBlocks Option to use continuousChecker or not
* Possibility to auto-restore from latest snapshot (useful for BP), deleting appropriate files to make it work and continue
* NumberOfSnapshotsToKeep flag to maintain a small list of snapshots -> If non-zero, it deletes older snapshot.

## 2020-03-21

### Changed

* License changed to Apache 2.0
