module github.com/streamingfast/node-manager

go 1.15

require (
	cloud.google.com/go/storage v1.5.0
	github.com/ShinyTrinkets/overseer v0.3.0
	github.com/abourget/llerrgroup v0.0.0-20161118145731-75f536392d17
	github.com/dfuse-io/bstream v0.0.2-0.20210810200727-06a4c4aaeb99
	github.com/dfuse-io/logging v0.0.0-20210109005628-b97a57253f70
	github.com/eoscanada/pitreos v1.1.1-0.20210810174831-3e4668dcda56
	github.com/google/renameio v0.1.0
	github.com/gorilla/mux v1.7.3
	github.com/klauspost/compress v1.10.2
	github.com/matishsiao/goInfo v0.0.0-20200404012835-b5f882ee2288
	github.com/streamingfast/dbin v0.0.0-20210809205249-73d5eca35dc5
	github.com/streamingfast/derr v0.0.0-20210810022442-32249850a4fb
	github.com/streamingfast/dgrpc v0.0.0-20210810185305-905172f728e8
	github.com/streamingfast/dmetrics v0.0.0-20210810172555-564d623b116f
	github.com/streamingfast/dstore v0.1.1-0.20210810110932-928f221474e4
	github.com/streamingfast/shutter v1.5.0
	github.com/stretchr/testify v1.7.0
	go.uber.org/atomic v1.6.0
	go.uber.org/zap v1.15.0
	google.golang.org/grpc v1.29.1
)

replace github.com/ShinyTrinkets/overseer => github.com/maoueh/overseer v0.2.1-0.20191024193921-39856397cf3f
