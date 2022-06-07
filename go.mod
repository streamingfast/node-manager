module github.com/streamingfast/node-manager

go 1.16

require (
	github.com/ShinyTrinkets/overseer v0.3.0
	github.com/abourget/llerrgroup v0.0.0-20161118145731-75f536392d17
	github.com/golang/protobuf v1.5.2
	github.com/google/renameio v0.1.0
	github.com/gorilla/mux v1.8.0
	github.com/streamingfast/bstream v0.0.2-0.20220607202937-611660228ea2
	github.com/streamingfast/derr v0.0.0-20220301163149-de09cb18fc70
	github.com/streamingfast/dgrpc v0.0.0-20220301153539-536adf71b594
	github.com/streamingfast/dmetrics v0.0.0-20210811180524-8494aeb34447
	github.com/streamingfast/dstore v0.1.1-0.20220607202639-35118aeaf648
	github.com/streamingfast/logging v0.0.0-20220304214715-bc750a74b424
	github.com/streamingfast/merger v0.0.3-0.20220506192522-f8be80e054cb
	github.com/streamingfast/shutter v1.5.0
	github.com/stretchr/testify v1.7.0
	go.uber.org/atomic v1.9.0
	go.uber.org/zap v1.21.0
	google.golang.org/grpc v1.44.0
)

replace github.com/ShinyTrinkets/overseer => github.com/streamingfast/overseer v0.2.1-0.20210326144022-ee491780e3ef
