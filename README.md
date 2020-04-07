# Structure

1) Operator (process commands, owns the superviser, owns the http handlers that sends the commands to him)

2) Superviser is the the one managing the actual blockchain software (nodeos, geth..). It is made of an embedded generic superviser struct, plus specific nodeos/geth embedding it.
   It owns the plugins.


# BRANCH-BASED BUILD SYSTEM

1. This repo can build 4 app derived from manageos:
  * eos
  * bos
  * eos-mindreader
  * bos-mindreader

2. The specific configuration for each build is in folder deploy/ under various environment files (build.bos.env, build.eos-mindreader.env, etc.) that will be used by the steps in cloudbuild.yaml

3. Each of these builds can be triggered from pushing master branch to origin/build/{app}
  ex:
  * git push origin master:build/eos-mindreader  ## this will trigger the build using the build.eos-mindreader.env file
  * git push origin master:build/eos             ## this will trigger the build using the build.eos.env file

4. You can test local builds with *deploy/local.sh fakeBranchName*

5. You can also trigger remote builds with *deploy/submit.sh fakeBranchName*, but should probably push to the "build" branch instead...

## HOWTO SWITCH BP (example nodeos-bp-0 to nodeos-bp-1)

1) check which node is producing: (the one answering 'false' for 'paused' is producing, example: nodeos-bp-0...)

```
  for i in nodeos-bp-{0,1,2}; do
    echo -n "$i "
    kubectl exec -ti $i -c nodeos-bp -- curl -w "\n" localhost:8888/v1/producer/paused
  done
```

2) edit the statefulset to change the image to the new version (wait few seconds for the file to be sync on all pods)

3) delete the "target" pod (one that is not currently producing, of course...) so that
   it restarts with the new version, and wait for it to catch up

```
    kubectl delete pod nodeos-bp-1
    # wait a bit ...
    kubectl logs -f nodeos-bp-1 nodeos-bp
```

4) edit configmap `manageos-conf` to point to the new producer (important that you do this AFTER step 3)

```
  manager.yaml: |
    producer_hostname: nodeos-bp-1  #  set to the one that you deleted
```

5) pause production on current bp and resume production on the next one:

```
    SRC_NODE=nodeos-bp-0
    DST_NODE=nodeos-bp-1

    # WATCH THE LOGS AND WAIT FOR OUR PRODUCTION ROUND TO COMPLETE, + ONE ROUND, then run:
    kubectl exec -ti ${SRC_NODE} -c nodeos-bp -- curl --fail -XPOST localhost:8080/v1/nodeos/maintenance?sync=true && kubectl exec -ti ${DST_NODE} -c nodeos-bp -- curl -XPOST localhost:8080/v1/nodeos/safely_resume_production?sync=true
    # the command below is currently failing on mainnet bp because safely_pause_production times out (state machine is fcked up). When fixed, it is the right command to execute because it does not require watching the logs...
    # kubectl exec -ti ${SRC_NODE} -c nodeos-bp -- curl --fail -XPOST localhost:8080/v1/nodeos/safely_pause_production?sync=true && kubectl exec -ti ${DST_NODE} -c nodeos-bp -- curl -XPOST localhost:8080/v1/nodeos/safely_resume_production?sync=true
```

6) verify that the correct node is producing:

```
  for i in nodeos-bp-{0,1,2}; do
    echo -n "$i "
    kubectl exec -ti $i -c nodeos-bp -- curl -w "\n" localhost:8888/v1/producer/paused
  done
```

7) restart the previously-producing node

```
  kubectl delete pod nodeos-bp-0
  # wait for it to be running and caught up...
  kubectl logs -f nodeos-bp-0 nodeos-bp
```
8) restart the other (spare) node
```
  kubectl delete pod nodeos-bp-2
  # wait for it to be running and caught up...
  kubectl logs -f nodeos-bp-2 nodeos-bp
```

## AWS ISSUES

### POD NOT RESTARTING
Usually, AWS fails restarting the pod because AWS kinda sucks at k8s...
When the pod does not appear after a bit (kubectl events will show a timeout in attaching mount points...)
When this happens, don't panic, just wait 2-3 minutes, then delete the pod again, then wait a few more minutes, maybe delete it again if still not up, etc., until either it works OR you run back to GCP screaming, horrified by your AWS k8s experience.

### POD NOT SCHEDULING
Sometimes, when a mountpoint attach issue happens, a node will get a "taint" from AWS that says that there is an issue with its volumes. That taint prevents any pod from being scheduled on that node. You will need to drain, then delete that node with kubectl and finally delete the VM from AWS console (ASG will recreate it...)


# Manageos manages a nodeos instance

Goals:
* A few modes of operations:
  * `--bp-mode`, enables all block producer flags and checks.
* Would check the dirty flag in the storage, or react to the "dirty flag"
  error log and react to that, by triggering an automatic backup from Google Storage.
  * Add an endpoint like POST /v1/manageos/backup to trigger a backup in this phase:
    * Ensure we are not on the hook to produce, or that another BP is currently
      assigned to produce. (when in --bp-mode)
    * Interrupt production if it was running, don't do it if we're on the hook to
      produce.
    * Get the last block height.
    * Shut down the HTTP proxy to `nodeos`, so that no new API
      connections arrive there.
    * Send a TERM signal to the `nodeos` process
    * Wait for it to completely shutdown, send a few more TERM signals if they don't
      work right away.
    * The TERM Signal can be delayed if the node is currently
      processing blocks massively (100% CPU)
    * Once the node exits with a proper shutdown signal and log, we:
    * Run a Google Storage 'rsync' with the /data directory.
      Tag it with the chain_id included and the last block height.
    * Restart the process
    * Once properly backup and sync'd (by checking we are receiving blocks that are
      incrementing.. or that get_info receives new blocks each 500ms:
      * Reopen the HTTP proxy

* Should query `localhost:6666`'s `eos-blocksigner` for the public keys available,
  and write corresponding `signature-provider`  to local `config.ini`, then boot.

* Manages config.ini creation, before running node.
  * With `--bp-mode`, add pause-on-startup, decide later if we `/v1/producer/resume`.
  * /v1/manageos/
  * Can pick up the `configmap`-mounted one, copy it somewhere useful, then call
    `nodeos` with it.
  * Listen on Kubernetes API, for changes to this configmap
    `--configmap=nodeos`, then do a ROLLING update, don't update if
    I'm producing blocks.  Take over production if I just
    rebooted.. so last to boot is always the producer, provided that
    we received 5 blocks with approximatly 500ms between (we're at the
    head of production).
    * If someone claimed to be producer in the last 5 seconds, don't
      do a handoff.

* Always keep an internal state, sync'd on etcd ? Of where we are in the schedule:
  ProductionRoundStatus: "producing", "producing next", "just produced", "not producing"
  InSchedule: true / false

On boot:
* When the `blocks/` do not exist:
  * Check Google Storage to see if the passed-on `--chain-id` exists over there.
    * If it does, download the Google Storage snapshot into `/data`
    * Start `nodeos`
  * Take the `--genesis-json` passed as a param to `nodeos` and start it.
  * Wait until sync before opening `http` endpoint.
* Before booting, inspect for the "dirty" flag.. and restore from backup if
  its dirty.

* Listens on 9999 and proxies to 8888,

* Whenever we execute `nodeos`, we are in "wrapping" mode... meaning we're receiving
  the logs, printing them ourselves (or simply processing them and keeping the stdout)
  * We instrument the log output with `prometheus`.

* Handle the SIGTERM reception, and properly defer it to the subprocess.
  Then wait for a second or so, and quit ourselves.
  (copy `trap` script from François and Bohdan)
