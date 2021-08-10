module github.com/streamingfast/node-manager

go 1.15

require (
	github.com/ShinyTrinkets/overseer v0.3.0
	github.com/abourget/llerrgroup v0.0.0-20161118145731-75f536392d17
	github.com/dfuse-io/bstream v0.0.2-0.20210810125844-72912424b968
	github.com/dfuse-io/dbin v0.0.0-20200406215642-ec7f22e794eb // indirect
	github.com/dfuse-io/dgrpc v0.0.0-20210810125735-867cc255530c
	github.com/dfuse-io/dmetrics v0.0.0-20200508152325-93e7e9d576bb
	github.com/dfuse-io/dstore v0.1.1-0.20210507180120-88a95674809f // indirect
	github.com/dfuse-io/jsonpb v0.0.0-20200406211248-c5cf83f0e0c0 // indirect
	github.com/dfuse-io/logging v0.0.0-20210109005628-b97a57253f70
	github.com/dfuse-io/opaque v0.0.0-20210108174126-bc02ec905d48 // indirect
	github.com/google/renameio v0.1.0
	github.com/google/uuid v1.1.1 // indirect
	github.com/gorilla/mux v1.7.0
	github.com/klauspost/compress v1.10.2
	github.com/rs/xid v1.2.1 // indirect
	github.com/streamingfast/dbin v0.0.0-20210809205249-73d5eca35dc5
	github.com/streamingfast/derr v0.0.0-20210810022442-32249850a4fb
	github.com/streamingfast/dstore v0.1.1-0.20210810110932-928f221474e4 // indirect
	github.com/streamingfast/shutter v1.5.0 // indirect
	github.com/stretchr/testify v1.4.0
	github.com/tidwall/gjson v1.5.0 // indirect
	go.uber.org/atomic v1.6.0
	go.uber.org/zap v1.15.0
	google.golang.org/grpc v1.29.1
)

replace github.com/ShinyTrinkets/overseer => github.com/dfuse-io/overseer v0.2.1-0.20210326144022-ee491780e3ef
