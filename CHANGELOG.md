# Change log

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## Unreleased

### Changed
* AutoRestoreLatest(bool) option becomes AutoRestoreSource (`backup`, `snapshot`)
* Nodeos unexpectedly shutting down now triggers a Shutdown of the app

### Added
* Possibility to auto-restore from latest snapshot (useful for BP), deleting correct files to make it work

## 2020-03-21

### Changed

* License changed to Apache 2.0
