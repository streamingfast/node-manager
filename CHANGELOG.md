# Change log

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## Unreleased

### Changed
* ContinuousChecker is not enabled by default now, use FailOnNonContinuousBlocks
* AutoRestoreLatest(bool) option becomes AutoRestoreSource (`backup`, `snapshot`)
* Nodeos unexpectedly shutting down now triggers a Shutdown of the app

### Added
* Options.AutoSnapshotHostnameMatch(string) will only apply auto-snapshot parameters if os.Hostname() returns this string
* Options.AutoBackupHostnameMatch(string) will only apply auto-backup parameters if os.Hostname() returns this string
* Add FailOnNonContinuousBlocks Option to use continuousChecker or not
* Possibility to auto-restore from latest snapshot (useful for BP), deleting correct files to make it work
* NumberOfSnapshotsToKeep flag to maintain a small list of snapshots

## 2020-03-21

### Changed

* License changed to Apache 2.0
