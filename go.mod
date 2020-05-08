module github.com/dfuse-io/manageos

go 1.12

require (
	cloud.google.com/go/storage v1.4.0
	github.com/ShinyTrinkets/overseer v0.3.0
	github.com/abourget/llerrgroup v0.0.0-20161118145731-75f536392d17
	github.com/dfuse-io/bstream v0.0.0-20200427161155-5bc64e86c005
	github.com/dfuse-io/dbin v0.0.0-20200406215642-ec7f22e794eb
	github.com/dfuse-io/dgrpc v0.0.0-20200406214416-6271093e544c
	github.com/dfuse-io/dmetrics v0.0.0-20200508152325-93e7e9d576bb
	github.com/dfuse-io/dstore v0.1.0
	github.com/dfuse-io/logging v0.0.0-20200407175011-14021b7a79af
	github.com/dfuse-io/shutter v1.4.1-0.20200319040708-c809eec458e6
	github.com/dustin/go-humanize v1.0.0 // indirect
	github.com/eoscanada/eos-go v0.9.1-0.20200506160036-5e090ae689ef
	github.com/eoscanada/pitreos v1.0.1-0.20190618150521-240402eb30e2
	github.com/frostschutz/go-fibmap v0.0.0-20160825162329-b32c231bfe6a // indirect
	github.com/ghodss/yaml v1.0.0 // indirect
	github.com/google/renameio v0.1.0
	github.com/gorilla/mux v1.7.0
	github.com/klauspost/compress v1.10.2
	github.com/matishsiao/goInfo v0.0.0-20170803142006-617e6440957e
	github.com/spf13/viper v1.3.2
	github.com/stretchr/testify v1.4.0
	go.uber.org/atomic v1.6.0
	go.uber.org/zap v1.14.0
	google.golang.org/grpc v1.26.0
)

replace github.com/ShinyTrinkets/overseer => github.com/maoueh/overseer v0.2.1-0.20191024193921-39856397cf3f
