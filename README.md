# dfuse node manager
[![reference](https://img.shields.io/badge/godoc-reference-5272B4.svg?style=flat-square)](https://pkg.go.dev/github.com/dfuse-io/manageos)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

This service is a wrapping process that operates blockchain nodes as part of **[dfuse](https://github.com/dfuse-io/dfuse)**.

## Installation

Build and run from here:

* [**dfuse for EOSIO**](https://github.com/dfuse-io/dfuse-eosio)
* [**dfuse for Etherieum**](https://github.com/dfuse-io/dfuse-ethereum)

## Overview

1) Operator (process commands, owns the superviser, owns the http handlers that sends the commands to him)

2) Superviser is the the one managing the actual blockchain software (nodeos, geth..). It is made of an embedded generic superviser struct, plus specific nodeos/geth embedding it. It owns the plugins.


## Contributing

**Issues and PR in this repo related strictly to the core manageos engine**

Report any protocol-specific issues in their
[respective repositories](https://github.com/dfuse-io/dfuse#protocols)

**Please first refer to the general
[dfuse contribution guide](https://github.com/dfuse-io/dfuse/blob/master/CONTRIBUTING.md)**,
if you wish to contribute to this code base.

This codebase uses unit tests extensively, please write and run tests.


## Shutdown pattern for Node-Manager only:
App creates:
  * Superviser
  * Operator (+superviser)

So, the ownership is `app -> operator -> superviser`
  * app.OnTerminating(operator.Shutdown())
  * operator.OnTerminating(sendCmd:"maintenance", superviser.Shutdown())
  * superviser.OnTerminating(superviser.Stop() (blocking))

## Shutdown pattern for Mindreader:

App creates:
  * Superviser
  * Operator (+superviser)
  * mindreaderPlugin (has call back to set maintenance on operator and stopBlockReached)

App sets:
  * superviser.RegisterLogPlugin(mindreaderPlugin)

So, the ownership is `app -> operator -> superviser -> mindreader`
  * app.OnTerminating(operator.Shutdown())
  * operator.OnTerminating(sendCmd:"maintenance", superviser.Shutdown())
  * superviser.OnTerminating(mindreader.Shutdown(), then endLogPlugins)
  * superviser.OnTerminated(endLogPlugins)
  * mindreader.OnTerminating(async operator.Shutdown(), wait consumeFlowDone)
    * mindreader::archiver closes consumeFlowDone when superviser.endLogPlugins(+upload completed)
    * mindreader shuts itself down when stopBlockNum reached
  * mindreader.OnTerminated -> app.Shutdown()

## License

[Apache 2.0](LICENSE)

