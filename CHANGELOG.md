## v0.5.7 (February 21, 2020)

### Notes

- Swarm Bootnodes have changed, please update your version to connect to the new bootnodes
- A memory leak related to the retrieval protocol has been fixed
- A deadlock related to incentivised retrievals has been fixed
- Possible data races in SWAP have been fixed

### Commits
* [#2113](https://github.com/ethersphere/swarm/pull/2113) stream: enforce server get ranges
* [#2111](https://github.com/ethersphere/swarm/pull/2111) bootnodes: change old ef bootnodes to new ones
* [#2107](https://github.com/ethersphere/swarm/pull/2107) stream: eliminate overlapping requests on the same stream
* [#2112](https://github.com/ethersphere/swarm/pull/2112) p2p/protocols: remove peer accounting lock
* [#2106](https://github.com/ethersphere/swarm/pull/2106) swap: fix various data races
* [#2103](https://github.com/ethersphere/swarm/pull/2103) retrieval: fix memory leak
* [#2104](https://github.com/ethersphere/swarm/pull/2104) stream: make empty wanted hashes more explicit
* [#2105](https://github.com/ethersphere/swarm/pull/2105) http: remove spamming logline
* [#2101](https://github.com/ethersphere/swarm/pull/2101) readme: add godoc shield
* [#2092](https://github.com/ethersphere/swarm/pull/2092) contract, swap: refactor test backend
* [#2100](https://github.com/ethersphere/swarm/pull/2100) bmt: fix data races in Hasher
* [#2097](https://github.com/ethersphere/swarm/pull/2097) bmt, file: Extract AsyncHasher to separate package


## v0.5.6 (February 10, 2020)

### Notes

- A hotfix to ENS resolver contract address has been applied due to a security vulnerability. More information here: https://medium.com/the-ethereum-name-service/ens-registry-migration-bug-fix-new-features-64379193a5a
- A bug related to syncing has been fixed

### Commits
* [#2021](https://github.com/ethersphere/swarm/pull/2021) bmt:  Introduce SectionWriter, implement hash.Hash in bmt
* [#2096](https://github.com/ethersphere/swarm/pull/2096) Revert "stream: limit the number of peers we sync with on bin 0 (#1972)"
* [#2095](https://github.com/ethersphere/swarm/pull/2095) stream: fix stream not wanted bug
* [#2091](https://github.com/ethersphere/swarm/pull/2091) contract/ens: use new ens contract address 0x00000000000C2E074eC69A0dFb2997BA6C7d2e1e
* [#1983](https://github.com/ethersphere/swarm/pull/1983) swap: prevent debt cheques
* [#2090](https://github.com/ethersphere/swarm/pull/2090) api/http/langos: fix TestHTTPMultipleRangeResponse test
* [#2088](https://github.com/ethersphere/swarm/pull/2088) state, swap: add batch support to state store


## v0.5.5 (January 29, 2020)

### Notes

- RNS resolution is now available in addition to the existing ENS resolution. See more about RNS here: https://developers.rsk.co/rif/rns
- Retrieve requests will now load balance across different peers in the address book
- Retrieve request which were forwarded will now be answered without going through the local store, making retrievals faster
- New peers will be suggested to connect to by address space gaps
- Protocol handlers are now async by design
- Fixes a bug where requested plain hashes would be redundantly sent to ENS resolver

### Commits

* [#2081](https://github.com/ethersphere/swarm/pull/2081) api: do not try to resolve swarm hashes over ens
* [#2073](https://github.com/ethersphere/swarm/pull/2073) kademlia: use IsSameAs instead of Match for capability index comparison
* [#2063](https://github.com/ethersphere/swarm/pull/2063) swap, uint256: unify variable types, pt. 1
* [#2065](https://github.com/ethersphere/swarm/pull/2065) network: Suggest peer by address space gap
* [#2070](https://github.com/ethersphere/swarm/pull/2070) swarm: simplify network test
* [#2062](https://github.com/ethersphere/swarm/pull/2062) Protocol async errors refactor
* [#2074](https://github.com/ethersphere/swarm/pull/2074) swap: move shared test helpers to separate file
* [#2066](https://github.com/ethersphere/swarm/pull/2066) swap: refactor cashout logic to cashout processor
* [#2072](https://github.com/ethersphere/swarm/pull/2072) network, storage: fix usage of base logger
* [#2018](https://github.com/ethersphere/swarm/pull/2018) p2p, network, bzzeth: p2p protocol handlers to async
* [#2051](https://github.com/ethersphere/swarm/pull/2051) P2p validate accounting
* [#1981](https://github.com/ethersphere/swarm/pull/1981) network/retrieval: integrate kademlia load balancing
* [#2064](https://github.com/ethersphere/swarm/pull/2064) tracing: fix Setup function
* [#2056](https://github.com/ethersphere/swarm/pull/2056) api, cmd/swarm: remove unused cli context from act functions
* [#2057](https://github.com/ethersphere/swarm/pull/2057) cmd, metrics: decouple metrics and cli library
* [#2061](https://github.com/ethersphere/swarm/pull/2061) swap: pass depositAmount to testDeploy
* [#2060](https://github.com/ethersphere/swarm/pull/2060) cmd/swarm, internal/debug: decouple cli library from internal/debug
* [#2059](https://github.com/ethersphere/swarm/pull/2059) cmd, tracing: decouple cli from tracing package
* [#2053](https://github.com/ethersphere/swarm/pull/2053) swap: pass the context to waitForTx directly
* [#2023](https://github.com/ethersphere/swarm/pull/2023) swap: added cheques metrics
* [#2016](https://github.com/ethersphere/swarm/pull/2016) storage: Preserve counter across Encrypt() calls
* [#2015](https://github.com/ethersphere/swarm/pull/2015) log, network, storage, testutil: Log base address only in tests
* [#1999](https://github.com/ethersphere/swarm/pull/1999) storage/localstore: decode tags in push index only if it is complete
* [#2000](https://github.com/ethersphere/swarm/pull/2000) all:use bzzaddr instead of overlay address for logging
* [#1982](https://github.com/ethersphere/swarm/pull/1982) netstore: optimize fetchers for retrieve forwarding
* [#1964](https://github.com/ethersphere/swarm/pull/1964) swap, contracts, vendor: move to ERC20
* [#2001](https://github.com/ethersphere/swarm/pull/2001) api/http: use swarm.eth instead theswarm.eth in html templates
* [#2002](https://github.com/ethersphere/swarm/pull/2002) metrics: remove metrics.influxdb.accounting flag
* [#1939](https://github.com/ethersphere/swarm/pull/1939) swarm, api, main: RNS resolution

## v0.5.4 (November 29, 2019)

### Notes

- This release fixes a possible error with the data migration that was introduced in 0.5.3 and an optimization that mitigates I/O bottlenecks when syncing
- Users can directly use this version from previous versions, there's no explicit need to run 0.5.3 before running 0.5.4

### Commits

* [#1993](https://github.com/ethersphere/swarm/pull/1993) stream: avoid repeated SetSyncPull for chunk
* [#1980](https://github.com/ethersphere/swarm/pull/1980) localstore: skip tag not found on migration
* [#1980](https://github.com/ethersphere/swarm/pull/1980) shed: RenameIndex return true if index is already renamed


## v0.5.3 (November 27, 2019)

### Notes

- This release brings a change in the Swarm retrieval protocol that blocks the delivery of unsolicited chunks
- A critical bug that caused the size of the Swarm node to overflow its designated storage size was fixed
- A limit on the number of peers being synced to on bin zero has been introduced to the stream protocol
- A lookahead buffered reader was introduced in order to speed up downloads by prefetching chunks
- A data migration was introduced in order to rename an index in the local store
- Tags are now persisted across node sessions, so you can keep track over your uploads when you restart your node
- Numerous data races have been fixed
- Push sync is now enabled by default
- Garbage collection and fetcher timeouts have changed, so you should be getting an overall better user experience with this version

### Features

- When uploading a file from the landing page at localhost:8500, progress bars would now be shown to indicate the progress of the upload

### Commits

* [#1980](https://github.com/ethersphere/swarm/pull/1980) storage/localstore: add subscriptions wait group before closing leveldb
* [#1979](https://github.com/ethersphere/swarm/pull/1979) network: increase fetcher timeout
* [#1978](https://github.com/ethersphere/swarm/pull/1978) localstore: change gc batch size
* [#1977](https://github.com/ethersphere/swarm/pull/1977) cmd/swarm: disable USB devices
* [#1973](https://github.com/ethersphere/swarm/pull/1973) localstore: fix subscribe push iterator tag value
* [#1972](https://github.com/ethersphere/swarm/pull/1972) stream: limit the number of peers we sync with on bin 0
* [#1969](https://github.com/ethersphere/swarm/pull/1969) smoke: remove redundant check for bail flag
* [#1970](https://github.com/ethersphere/swarm/pull/1970) localstore: fix data races on subscriptions
* [#1968](https://github.com/ethersphere/swarm/pull/1968) smoke: retry failed upload
* [#1959](https://github.com/ethersphere/swarm/pull/1959) api: Add bzz-feed-raw scheme to retrieve arbitary feed update
* [#1962](https://github.com/ethersphere/swarm/pull/1962) pss, network: remove spamming loglines
* [#1963](https://github.com/ethersphere/swarm/pull/1963) retrieval: change loglevel of chunk retrieval errors
* [#1934](https://github.com/ethersphere/swarm/pull/1934) swap: use p2ptest for protocol tests
* [#1960](https://github.com/ethersphere/swarm/pull/1960) cmd/swarm: add pprof custom profiles through flags
* [#1952](https://github.com/ethersphere/swarm/pull/1952) network/stream: fix flaky tests and first delivered batch of chunks for an unwanted stream
* [#1957](https://github.com/ethersphere/swarm/pull/1957) api/config: enable pushsync by default
* [#1919](https://github.com/ethersphere/swarm/pull/1919) bzzeth: Phase2 of bzz eth protocol
* [#1945](https://github.com/ethersphere/swarm/pull/1945) cmd/swarm: swap --sync-mode flag with --no-sync flag
* [#1947](https://github.com/ethersphere/swarm/pull/1947) api/http/langos: ensure only one second peek
* [#1915](https://github.com/ethersphere/swarm/pull/1915) pushsync, localstore: decouple push/pull indexes for tag increment
* [#1940](https://github.com/ethersphere/swarm/pull/1940) localstore: fix index leak when NN chunks enter localstore
* [#1922](https://github.com/ethersphere/swarm/pull/1922) swap: always allow debt-reducing messages
* [#1883](https://github.com/ethersphere/swarm/pull/1883) swap: segregate api code and tests
* [#1907](https://github.com/ethersphere/swarm/pull/1907) cmd/swarm-smoke-pss: initial pss smoke tests
* [#1926](https://github.com/ethersphere/swarm/pull/1926) integration-tests: config for set of integration tests for stable swarm
* [#1944](https://github.com/ethersphere/swarm/pull/1944) swap: fix a rare panic in TestMultiChequeSimulation
* [#1913](https://github.com/ethersphere/swarm/pull/1913) swap: exchange chain id on handshake
* [#1942](https://github.com/ethersphere/swarm/pull/1942) swap: also wait for the debit counter to reach maxMsgsInt64
* [#1774](https://github.com/ethersphere/swarm/pull/1774) network: Kademlia Load Balancing
* [#1876](https://github.com/ethersphere/swarm/pull/1876) api/http: lookahead buffer
* [#1937](https://github.com/ethersphere/swarm/pull/1937) swap: fixed bug which needs cheques to be confirmed
* [#1930](https://github.com/ethersphere/swarm/pull/1930) swap: fix instabilities in simulation tests
* [#1873](https://github.com/ethersphere/swarm/pull/1873) swap: improve balance tests
* [#1887](https://github.com/ethersphere/swarm/pull/1887) swap: introduce pending cheques, drop peers on io errors
* [#1927](https://github.com/ethersphere/swarm/pull/1927) network/stream: add mutex to avoid unsynchronized access to streamInfoRes
* [#1923](https://github.com/ethersphere/swarm/pull/1923) swarm: shutdown pushsync before pss
* [#1924](https://github.com/ethersphere/swarm/pull/1924) network/stream: fix slice append bug
* [#1914](https://github.com/ethersphere/swarm/pull/1914) chunk: fix tag Sent value on load, don't persist done tags
* [#1916](https://github.com/ethersphere/swarm/pull/1916) pss: Pushsync and PSS fixes
* [#1863](https://github.com/ethersphere/swarm/pull/1863) swap: expand RPC calls for cheque info querying
* [#1892](https://github.com/ethersphere/swarm/pull/1892) Swap available balance
* [#1833](https://github.com/ethersphere/swarm/pull/1833) network/kademlia: SugestPeer funcionality. Variable number of min peers per bin
* [#1801](https://github.com/ethersphere/swarm/pull/1801) network/hive: move message handlers from Peer to Hive
* [#1912](https://github.com/ethersphere/swarm/pull/1912) shed: allow to change the index name for easier migrations
* [#1909](https://github.com/ethersphere/swarm/pull/1909) storage/localstore: do not later batch in setSync if moveToGc is false
* [#1900](https://github.com/ethersphere/swarm/pull/1900) pss, pss/internal/ticker: fix data races
* [#1901](https://github.com/ethersphere/swarm/pull/1901) network: fix a data race on Hive.addPeer function in tests
* [#1894](https://github.com/ethersphere/swarm/pull/1894) swarm: enable tag persistence across sessions
* [#1908](https://github.com/ethersphere/swarm/pull/1908) api,storage: add index size rpc call for debugging
* [#1906](https://github.com/ethersphere/swarm/pull/1906) chunk: align tag struct for 32bit arch
* [#1905](https://github.com/ethersphere/swarm/pull/1905) pusher: db.Set before check for synced
* [#1904](https://github.com/ethersphere/swarm/pull/1904) pushsync: fix finish root span
* [#1881](https://github.com/ethersphere/swarm/pull/1881) pushsync: split sync to chunksWorker and receiptsWorker
* [#1902](https://github.com/ethersphere/swarm/pull/1902) network/retrieval: fix race condition where a retrieve request has been
* [#1880](https://github.com/ethersphere/swarm/pull/1880) chunk, pushsync: do not increment synced tags in pushsync
* [#1803](https://github.com/ethersphere/swarm/pull/1803) swap: use the SimpleSwapFactory for deployment and verification
* [#1898](https://github.com/ethersphere/swarm/pull/1898) api/http: fix bufferedReadSeeker Seek method
* [#1896](https://github.com/ethersphere/swarm/pull/1896) netkwork/hive: addPeer function assigned after launching goroutine connectInitialPeers
* [#1878](https://github.com/ethersphere/swarm/pull/1878) storage/pin: fix getAllChunksFromDB test function
* [#1891](https://github.com/ethersphere/swarm/pull/1891) state: avoid has call and remove error shadowing in Get
* [#1875](https://github.com/ethersphere/swarm/pull/1875) retrieve: block unsolicited chunk deliveries
* [#1715](https://github.com/ethersphere/swarm/pull/1715) swarm: add pull request template
* [#1890](https://github.com/ethersphere/swarm/pull/1890) build, .travis.yml, Makefile, README.md: remove GO111MODULE=on
* [#1889](https://github.com/ethersphere/swarm/pull/1889) build: configure golangci-lint timeout in .golangci.yml
* [#1888](https://github.com/ethersphere/swarm/pull/1888) testutil: remove pre Go 1.13 support
* [#1844](https://github.com/ethersphere/swarm/pull/1844) network: Reconnect to the same peers on startup
* [#1861](https://github.com/ethersphere/swarm/pull/1861) pss: Limit by time the retries of messages in the outbox
* [#1860](https://github.com/ethersphere/swarm/pull/1860) pss: Limit the number of workers forwarding messages in outbox
* [#1806](https://github.com/ethersphere/swarm/pull/1806) swarm, swap: add addresses to logs
* [#1870](https://github.com/ethersphere/swarm/pull/1870) pss: fix concurrent map read and map write on pubKeyRWPool
* [#1737](https://github.com/ethersphere/swarm/pull/1737) Incentives first sim
* [#1874](https://github.com/ethersphere/swarm/pull/1874) chunk: align Tag fields for atomic operations on 32bit architectures
* [#1871](https://github.com/ethersphere/swarm/pull/1871) simulation/examples: disable docker test from running locally
* [#1828](https://github.com/ethersphere/swarm/pull/1828) api, chunk, storage: anonymous uploads
* [#1859](https://github.com/ethersphere/swarm/pull/1859) api/http: add HTML Progress Bar


## v0.5.2 (October 07, 2019)

### Notes

- This release includes a small fix to avoid connections to the default bootnodes when not using the default testnet.

### Commits

* [#1865](https://github.com/ethersphere/swarm/pull/1865) cmd/swarm: nullify bootnodes before check for network id
* [#1862](https://github.com/ethersphere/swarm/pull/1862) cmd/swarm: only use the default bootnodes on the default network id

## v0.5.1 (October 03, 2019)

### Notes

- This release just includes some minor bugfixes.
- If you have a push sync enabled cluster, you now need to provide the `--progress` flag to `swarm up` if you want to see the progress bar.

### Commits

* [#1853](https://github.com/ethersphere/swarm/pull/1853) cmd/swarm: fix progress bars for encrypted upload
* [#1842](https://github.com/ethersphere/swarm/pull/1842) swap: query cheque information through RPC call
* [#1827](https://github.com/ethersphere/swarm/pull/1827) swap: message price
* [#1850](https://github.com/ethersphere/swarm/pull/1850) cmd/swarm: remove --no-track, add --progress flag
* [#1851](https://github.com/ethersphere/swarm/pull/1851) api/client: use a separate http client with cookie jar for sticky session
* [#1852](https://github.com/ethersphere/swarm/pull/1852) chunk, cmd/swarm: return last tag
* [#1846](https://github.com/ethersphere/swarm/pull/1846) network: fix hive autoconnect param that was added and caused regression in test
* [#1841](https://github.com/ethersphere/swarm/pull/1841) network/stream: move from v2


## v0.5.0 (September 30, 2019)

### Notes

 - This release is not backward compatible with the previous versions of Swarm. Please update your nodes.
 - Ubuntu PPA support has been removed. We will add support for multiple software distribution platforms at a later stage. In the meanwhile you can still find the Swarm binaries at the [official download page](https://swarm-gateways.net/bzz:/swarm.eth/downloads/).

### Features

 - **Content Pinning:** You can now pin content to your local swarm node. This way you can guarantee that certain content stays on your node. You can enable pinning by setting the `--enable-pinning` flag. For more information, read the [documentation](https://swarm-guide.readthedocs.io/en/latest/dapp_developer/index.html#pinning-content).

The following features are **experimental** and are **not enabled by default on our public testnet**. If you want to test these features, make sure to use a separate network (e.g. create your own swarm cluster) and set the same feature flags on all nodes.

 - **Push sync:** Additionally to our previous syncing algorithm, pull sync, we now also have [push sync](https://swarm-guide.readthedocs.io/en/latest/go_client_contributor.html#structural-components-and-key-processes) as an option available to synchronise the content across nodes. If you want to try it out, you have to start you swarm node with `--sync-mode=all`.
 - **Progress bar via Tags:** You can now track your upload progress when you use `swarm up`. This is possible due to [tags](https://swarm-guide.readthedocs.io/en/latest/dapp_developer/index.html#tags). This feature requires push sync to be enabled.
 - **SWAP:** A first version of of the SWarm Accounting Protocol (SWAP) is implemented. SWAP is a tit-for-tat incentive system where nodes account how much data they request and serve to their peers. If a node consumes significantly more than it provides, it pays for the consumed data using a L2 payment system: the chequebook. You can try SWAP on a SWAP enabled network with the flags `--swap --swap-backend-url=<evm_endpoint>` or learn more in the [SW3 paper](https://www.overleaf.com/read/yszmsdqyqbvc) , [Swap, Swear, and Swindle Games video](https://www.youtube.com/watch?v=9Cgyhsjsfbg) or [this article](https://www.rifos.org/blog/rif-storage-the-first-chunks/). The economics of this system is still in a highly experimental state.
- **BzzEth:** This is a new protocol that enables Ethereum and Swarm nodes to communicate with each other and help Ethereum nodes distribute blockchain data and state data over Swarm, enabling various functionalities like state syncing via Swarm and light client support. This release includes the preliminary phase of supporting blockchain on Swarm. This is experimental work that can be trialled with a dedicated version of the Ethereum Trinity client. For more information check out [Ethereum header chain over Swarm](https://github.com/ethersphere/user-stories/issues/9).

### Commits
* [#1814](https://github.com/ethersphere/swarm/pull/1814) cmd/swarm: add cli UI for tag progress on pushsync
* [#1793](https://github.com/ethersphere/swarm/pull/1793) cmd/swarm-smoke: add flag to bail on error
* [#1835](https://github.com/ethersphere/swarm/pull/1835) p2p: fix tests due to Drop signature change
* [#1834](https://github.com/ethersphere/swarm/pull/1834) network: disable stream spec in bzz if pull sync is not enabled
* [#1832](https://github.com/ethersphere/swarm/pull/1832) network/stream: skip longrunning simulation due to travis flakes
* [#1831](https://github.com/ethersphere/swarm/pull/1831) all: add reason for dropping peer
* [#1830](https://github.com/ethersphere/swarm/pull/1830) swarm: make pullsync disabled not expose stream protocol
* [#1819](https://github.com/ethersphere/swarm/pull/1819) pushsync: optimize TestPushsyncSimulation
* [#1826](https://github.com/ethersphere/swarm/pull/1826) swap: return the balance in swap.Balance for an unconnected but previous peers
* [#1798](https://github.com/ethersphere/swarm/pull/1798) swap: chequebook persistence
* [#1818](https://github.com/ethersphere/swarm/pull/1818) bzzeth: fix TestBzzBzzHandshakeWithMessage
* [#1816](https://github.com/ethersphere/swarm/pull/1816) build: add -trimpath build/install option
* [#1789](https://github.com/ethersphere/swarm/pull/1789) swap: prompt user for initial deposit amount
* [#1782](https://github.com/ethersphere/swarm/pull/1782) pushsync: initial implementation
* [#1815](https://github.com/ethersphere/swarm/pull/1815) bzzeth: correct the order of cleanup steps in * newTestNetworkStore
* [#1812](https://github.com/ethersphere/swarm/pull/1812) build: remove debian package and ubuntu ppa support
* [#1813](https://github.com/ethersphere/swarm/pull/1813) build: remove nsis, maven and iOS pod configurations
* [#1685](https://github.com/ethersphere/swarm/pull/1685) bzzeth: Phase 1
* [#1783](https://github.com/ethersphere/swarm/pull/1783) network: Documented, simplified and cleaner eachBin * function in pot.go
* [#1795](https://github.com/ethersphere/swarm/pull/1795) api, cmd: add pinning feature flag
* [#1786](https://github.com/ethersphere/swarm/pull/1786) network/stream: refactor cursors tests
* [#1791](https://github.com/ethersphere/swarm/pull/1791) network: Add capabilities if peer from store does not * have it
* [#1754](https://github.com/ethersphere/swarm/pull/1754) Swap logger
* [#1787](https://github.com/ethersphere/swarm/pull/1787) network: Add capability filtered depth calculation
* [#1784](https://github.com/ethersphere/swarm/pull/1784) travis: remove go1.12 job
* [#1761](https://github.com/ethersphere/swarm/pull/1761) cmd/swarm: correct bzznetworkid flag description
* [#1764](https://github.com/ethersphere/swarm/pull/1764) network, pss: Capability in pss
* [#1779](https://github.com/ethersphere/swarm/pull/1779) network/stream: handle nil peer in * TestNodesExchangeCorrectBinIndexes
* [#1771](https://github.com/ethersphere/swarm/pull/1771) protocols, retrieval: swap-enabled messages implement * Price
* [#1781](https://github.com/ethersphere/swarm/pull/1781) cmd/swarm-smoke: fix waitToPushSynced connection * closing
* [#1777](https://github.com/ethersphere/swarm/pull/1777) cmd/swarm: simplify testCluster.StartNewNodes
* [#1778](https://github.com/ethersphere/swarm/pull/1778) build: increase golangci-lint deadline
* [#1780](https://github.com/ethersphere/swarm/pull/1780) docker: ignore build/bin when copying files
* [#1769](https://github.com/ethersphere/swarm/pull/1769) swap: fix and rename Peer.getLastSentCumulativePayout
* [#1776](https://github.com/ethersphere/swarm/pull/1776) network/stream: more resilient * TestNodesCorrectBinsDynamic
* [#1713](https://github.com/ethersphere/swarm/pull/1713) network: Add Capabilities to Kademlia database
* [#1775](https://github.com/ethersphere/swarm/pull/1775) network: add own address to KademliaInfo
* [#1742](https://github.com/ethersphere/swarm/pull/1742) pss: Refactor. Step 2. Refactor forward cache
* [#1729](https://github.com/ethersphere/swarm/pull/1729) all: configurable payment/disconnect thresholds
* [#1760](https://github.com/ethersphere/swarm/pull/1760) network/stream/v2: more resilient * TestNodesExchangeCorrectBinIndexes
* [#1770](https://github.com/ethersphere/swarm/pull/1770) stream: dereference StreamInfoReq structure in * updateSyncSubscriptions function
* [#1765](https://github.com/ethersphere/swarm/pull/1765) network/retrieval: fix Price method reflect call
* [#1734](https://github.com/ethersphere/swarm/pull/1734) pss: Refactor. Step 1. Refactor PssMsg
* [#1758](https://github.com/ethersphere/swarm/pull/1758) network/retrieval: add balances to the retrieval * protocol
* [#1762](https://github.com/ethersphere/swarm/pull/1762) network: fix data race in simulation.NewBzzInProc
* [#1748](https://github.com/ethersphere/swarm/pull/1748) contracts/swap, swap: refactor contractAddress, * remove backend as inputParam, function order in contracts/swap
* [#1745](https://github.com/ethersphere/swarm/pull/1745) bzzeth: protocol runloop now quits on peer disconnzethect
* [#1752](https://github.com/ethersphere/swarm/pull/1752) api/client: fix data race in TarUpload
* [#1749](https://github.com/ethersphere/swarm/pull/1749) api, metrics, network: check caps when deciding on * next peer for a chunk
* [#1731](https://github.com/ethersphere/swarm/pull/1731) pss: Distill whisper elements in pss to a custom * fallback crypto
* [#1538](https://github.com/ethersphere/swarm/pull/1538) network: new stream! protocol and pull syncer * implementation
* [#1743](https://github.com/ethersphere/swarm/pull/1743) swap: fix rpc test to use peer balance
* [#1710](https://github.com/ethersphere/swarm/pull/1710) all: support go 1.13 change to testing/flag packages
* [#1725](https://github.com/ethersphere/swarm/pull/1725) swap: refactor lastReceivedCheque, lastSentCheque, * balances to peer
* [#1740](https://github.com/ethersphere/swarm/pull/1740) network: terminate Hive connect goroutine on Stop
* [#1733](https://github.com/ethersphere/swarm/pull/1733) Incentives rpc test
* [#1718](https://github.com/ethersphere/swarm/pull/1718) swarm, swap: pass chequebook address at start-up
* [#1721](https://github.com/ethersphere/swarm/pull/1721) swap: fix TestHandshake and TestEmitCheque
* [#1717](https://github.com/ethersphere/swarm/pull/1717) cmd/swarm-smoke: prevent smoke test from executing * trackChunks twice when we debug
* [#1683](https://github.com/ethersphere/swarm/pull/1683) swap, contracts, vendor: move to waiver-free * simplestswap
* [#1698](https://github.com/ethersphere/swarm/pull/1698) pss: Modularize crypto and remove Whisper. Step 1 - * isolate whisper code
* [#1695](https://github.com/ethersphere/swarm/pull/1695) pss: Improve pressure backstop queue handling - no * mutex
* [#1709](https://github.com/ethersphere/swarm/pull/1709) cmd/swarm-snapshot: if 2 nodes to create snapshot use * connectChain
* [#1675](https://github.com/ethersphere/swarm/pull/1675) network: Add API for Capabilities
* [#1702](https://github.com/ethersphere/swarm/pull/1702) pss: fixed flaky test that was using a global * variable instead of a local one
* [#1682](https://github.com/ethersphere/swarm/pull/1682) pss: Port tests to `network/simulation`
* [#1700](https://github.com/ethersphere/swarm/pull/1700) storage: fix hasherstore seen check to happen when * error is nil
* [#1689](https://github.com/ethersphere/swarm/pull/1689) vendor: upgrade go-ethereum to 1.9.2
* [#1571](https://github.com/ethersphere/swarm/pull/1571) bzzeth: initial support for bzz-eth protocol
* [#1696](https://github.com/ethersphere/swarm/pull/1696) network/stream: terminate runUpdateSyncing on peer * quit
* [#1554](https://github.com/ethersphere/swarm/pull/1554) all: first working SWAP version
* [#1684](https://github.com/ethersphere/swarm/pull/1684) chunk, storage: storage with multi chunk Set method
* [#1686](https://github.com/ethersphere/swarm/pull/1686) chunk, storage: add HasMulti to chunk.Store
* [#1691](https://github.com/ethersphere/swarm/pull/1691) chunk, shed, storage: chunk.Store GetMulti method
* [#1649](https://github.com/ethersphere/swarm/pull/1649) api, chunk: progress bar support
* [#1681](https://github.com/ethersphere/swarm/pull/1681) chunk, storage: chunk.Store multiple chunk Put
* [#1679](https://github.com/ethersphere/swarm/pull/1679) storage: fix pyramid chunker and hasherstore possible * deadlocks
* [#1672](https://github.com/ethersphere/swarm/pull/1672) pss: Use distance to determine single guaranteed * recipient
* [#1674](https://github.com/ethersphere/swarm/pull/1674) storage: fix possible hasherstore deadlock on waitC * channel
* [#1619](https://github.com/ethersphere/swarm/pull/1619) network: Add adaptive capabilities message
* [#1648](https://github.com/ethersphere/swarm/pull/1648) p2p/protocols, p2p/testing; conditional propagation * of context
* [#1673](https://github.com/ethersphere/swarm/pull/1673) api/http: remove unnecessary conversion
* [#1670](https://github.com/ethersphere/swarm/pull/1670) storage: fix LazyChunkReader.join potential deadlock
* [#1658](https://github.com/ethersphere/swarm/pull/1658) HTTP API support for pinning contents
* [#1621](https://github.com/ethersphere/swarm/pull/1621) pot: Add Distance methods with tests
* [#1667](https://github.com/ethersphere/swarm/pull/1667) README: Update Vendored Dependencies section
* [#1647](https://github.com/ethersphere/swarm/pull/1647) network, p2p, vendor: move vendored p2p/testing under * swarm
* [#1532](https://github.com/ethersphere/swarm/pull/1532) build, vendor: use go modules for vendoring
* [#1509](https://github.com/ethersphere/swarm/pull/1509) api, chunk, cmd, shed, storage: add support for * pinning content
* [#1620](https://github.com/ethersphere/swarm/pull/1620) docs/swarm-guide: cleanup
* [#1615](https://github.com/ethersphere/swarm/pull/1615) travis: split jobs into different stages
* [#1616](https://github.com/ethersphere/swarm/pull/1616) simulation: retry if we hit a collision on tcp/udp * ports
* [#1614](https://github.com/ethersphere/swarm/pull/1614) api, chunk: rename Tag.New to Tag.Create
* [#1580](https://github.com/ethersphere/swarm/pull/1580) pss: instrumentation and refactor
* [#1576](https://github.com/ethersphere/swarm/pull/1576) api, cmd, network: add --disable-auto-connect flag


## v0.4.3 (July 25, 2019)

### Notes

- **Docker users:** The `$PASSWORD` and `$DATADIR` environment variables are not supported anymore since this release. From now on you should mount the password or data directories as volumes. For example:
  ```bash
  $ docker run -it -v $PWD/hostdata:/data \
                   -v $PWD/password:/password \
                   ethersphere/swarm:0.4.3 \
                     --datadir /data \
                     --password /password
  ```

### Bug fixes and improvements

* [#1586](https://github.com/ethersphere/swarm/pull/1586): network: structured output for kademlia table
* [#1582](https://github.com/ethersphere/swarm/pull/1582): client: add bzz client, update smoke tests
* [#1578](https://github.com/ethersphere/swarm/pull/1578): swarm-smoke: fix check max prox hosts for pull/push sync modes
* [#1557](https://github.com/ethersphere/swarm/pull/1557): cmd/swarm: allow using a network interface by name for nat purposes
* [#1534](https://github.com/ethersphere/swarm/pull/1534): api, network: count chunk deliveries per peer
* [#1537](https://github.com/ethersphere/swarm/pull/1537): swarm: fix bzz_info.port when using dynamic port allocation
* [#1531](https://github.com/ethersphere/swarm/pull/1531): cmd/swarm: make bzzaccount flag optional and add bzzkeyhex flag
* [#1536](https://github.com/ethersphere/swarm/pull/1536): cmd/swarm: use only one function to parse flags
* [#1530](https://github.com/ethersphere/swarm/pull/1530): network/bitvector: Multibit set/unset + string rep
* [#1555](https://github.com/ethersphere/swarm/pull/1555): PoC: Network simulation framework

## v0.4.2 (June 28, 2019)

### Notes

This release is not backward compatible with the previous versions of Swarm due to changes to the wire protocol of the Retrieve Request messages. Please update your nodes.

### Bug fixes and improvements

* [#1503](https://github.com/ethersphere/swarm/pull/1503): network/simulation: add ExecAdapter capability to swarm simulations
* [#1495](https://github.com/ethersphere/swarm/pull/1495): build: enable ubuntu ppa disco (19.04) builds
* [#1395](https://github.com/ethersphere/swarm/pull/1395): swarm/storage: support for uploading 100gb files
* [#1344](https://github.com/ethersphere/swarm/pull/1344): swarm/network, swarm/storage: simplification of fetchers
* [#1488](https://github.com/ethersphere/swarm/pull/1488): docker: include git commit hash in swarm version

## v0.4.1 (June 13, 2019)

### Improvements

* [#1465](https://github.com/ethersphere/swarm/pull/1465): network: bump proto versions due to change in OfferedHashesMsg
* [#1428](https://github.com/ethersphere/swarm/pull/1428): swarm-smoke: add debug flag
* [#1422](https://github.com/ethersphere/swarm/pull/1422): swarm/network/stream: remove dead code
* [#1463](https://github.com/ethersphere/swarm/pull/1463): docker: create new dockerfiles that are context aware
* [#1466](https://github.com/ethersphere/swarm/pull/1466): changelog for releases

### Bug fixes

* [#1460](https://github.com/ethersphere/swarm/pull/1460): storage: fix alignement panics on 32 bit arch
* [#1422](https://github.com/ethersphere/swarm/pull/1422), [#19650](https://github.com/ethereum/go-ethereum/pull/19650): swarm/network/stream: remove dead code
* [#1420](https://github.com/ethersphere/swarm/pull/1420): swarm, cmd: fix migration link, change loglevel severity
* [#19594](https://github.com/ethereum/go-ethereum/pull/19594): swarm/api/http: fix bzz-hash to return ens resolved hash directly
* [#19599](https://github.com/ethereum/go-ethereum/pull/19599): swarm/storage: fix SubscribePull to not skip chunks

### Notes

* Swarm has split the codebase ([go-ethereum#19661](https://github.com/ethereum/go-ethereum/pull/19661), [#1405](https://github.com/ethersphere/swarm/pull/1405)) from [ethereum/go-ethereum](https://github.com/ethereum/go-ethereum). The code is now under [ethersphere/swarm](https://github.com/ethersphere/swarm)
* New docker images (>=0.4.0) can now be found under https://hub.docker.com/r/ethersphere/swarm

## v0.4.0 (May 17, 2019)

### Changes

* Implemented parallel feed lookups within Swarm Feeds
* Updated syncing protocol subscription algorithm
* Implemented EIP-1577 - Multiaddr support for ENS
* Improved LocalStore implementation
* Added support for syncing tags which provide the ability to measure how long it will take for an uploaded file to sync to the network
* Fixed data race bugs within PSS
* Improved end-to-end integration tests
* Various performance improvements and bug fixes
* Improved instrumentation - metrics and OpenTracing traces

### Notes
This release is not backward compatible with the previous versions of Swarm due to the new LocalStore implementation. If you wish to keep your data, you should run a data migration prior to running this version.

BZZ network ID has been updated to 4.

Swarm v0.4.0 introduces major changes to the existing codebase. Among other things, the storage layer has been rewritten to be more modular and flexible in a manner that will accommodate for our future needs. Since Swarm at this point does not provide any storage guarantees, we have made the decision to not impose any migrations on the nodes that we maintain as part of the public test network, nor on our users. We have provided a [manual](https://github.com/ethersphere/swarm/blob/master/docs/Migration-v0.3-to-v0.4.md) for those of you who are running private deployments and would like to migrate your data to the new local storage schema.
