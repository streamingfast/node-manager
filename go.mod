module github.com/dfuse-io/node-manager

go 1.15

require (
	cloud.google.com/go/storage v1.4.0
	github.com/ShinyTrinkets/overseer v0.3.0
	github.com/abourget/llerrgroup v0.0.0-20161118145731-75f536392d17
	github.com/dfuse-io/bstream v0.0.2-0.20200714123252-e9115283f55f
	github.com/dfuse-io/dbin v0.0.0-20200406215642-ec7f22e794eb
	github.com/dfuse-io/derr v0.0.0-20200406214256-c690655246a1
	github.com/dfuse-io/dgrpc v0.0.0-20200406214416-6271093e544c
	github.com/dfuse-io/dmetrics v0.0.0-20200508152325-93e7e9d576bb
	github.com/dfuse-io/dstore v0.1.1-0.20210203172334-dec78c6098a6
	github.com/dfuse-io/logging v0.0.0-20210109005628-b97a57253f70
	github.com/dfuse-io/shutter v1.4.1
	github.com/eoscanada/pitreos v1.1.0
	github.com/google/renameio v0.1.0
	github.com/gorilla/mux v1.7.0
	github.com/klauspost/compress v1.10.2
	github.com/matishsiao/goInfo v0.0.0-20200404012835-b5f882ee2288
	github.com/stretchr/testify v1.4.0
	go.uber.org/atomic v1.6.0
	go.uber.org/zap v1.14.0
	google.golang.org/grpc v1.26.0
)

replace github.com/ShinyTrinkets/overseer => github.com/maoueh/overseer v0.2.1-0.20191024193921-39856397cf3f
