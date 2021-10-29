module github.com/streamingfast/node-manager

go 1.15

require (
	github.com/ShinyTrinkets/overseer v0.3.0
	github.com/abourget/llerrgroup v0.0.0-20161118145731-75f536392d17
	github.com/google/renameio v0.1.0
	github.com/gorilla/mux v1.7.3
	github.com/streamingfast/bstream v0.0.2-0.20211029201027-268abefde7b4
	github.com/streamingfast/dbin v0.0.0-20210809205249-73d5eca35dc5
	github.com/streamingfast/derr v0.0.0-20210811180100-9138d738bcec
	github.com/streamingfast/dgrpc v0.0.0-20210901144702-c57c3701768b
	github.com/streamingfast/dmetrics v0.0.0-20210811180524-8494aeb34447
	github.com/streamingfast/dstore v0.1.1-0.20211012134319-16e840827e38
	github.com/streamingfast/logging v0.0.0-20210908162127-bdc5856d5341
	github.com/streamingfast/shutter v1.5.0
	github.com/stretchr/testify v1.7.0
	go.uber.org/atomic v1.7.0
	go.uber.org/zap v1.19.1
	google.golang.org/grpc v1.39.1
)

replace github.com/ShinyTrinkets/overseer => github.com/dfuse-io/overseer v0.2.1-0.20210326144022-ee491780e3ef
