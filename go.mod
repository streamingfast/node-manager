module github.com/streamingfast/node-manager

go 1.15

require (
	github.com/ShinyTrinkets/overseer v0.3.0
	github.com/abourget/llerrgroup v0.0.0-20161118145731-75f536392d17
	github.com/dfuse-io/bstream v0.0.2-0.20210218160250-ce6144227e87
	github.com/dfuse-io/dbin v0.0.0-20200406215642-ec7f22e794eb
	github.com/dfuse-io/derr v0.0.0-20200406214256-c690655246a1
	github.com/dfuse-io/dgrpc v0.0.0-20210128133958-db1ca95920e4
	github.com/dfuse-io/dmetrics v0.0.0-20200508152325-93e7e9d576bb
	github.com/dfuse-io/dstore v0.1.1-0.20210203172334-dec78c6098a6
	github.com/dfuse-io/logging v0.0.0-20210109005628-b97a57253f70
	github.com/dfuse-io/shutter v1.4.1
	github.com/google/renameio v0.1.0
	github.com/gorilla/mux v1.7.0
	github.com/klauspost/compress v1.10.2
	github.com/stretchr/testify v1.4.0
	go.uber.org/atomic v1.6.0
	go.uber.org/zap v1.14.0
	google.golang.org/grpc v1.29.1
)

replace github.com/ShinyTrinkets/overseer => github.com/dfuse-io/overseer v0.2.1-0.20210326144022-ee491780e3ef
