module github.com/saveio/dsp-go-sdk

go 1.14

replace (
	github.com/saveio/themis => ../themis
	github.com/saveio/themis-go-sdk => ../themis-go-sdk
)

require (
	github.com/gogo/protobuf v1.3.2
	github.com/golang/protobuf v1.3.2
	github.com/google/uuid v1.2.0
	github.com/hashicorp/golang-lru v0.5.5-0.20210104140557-80c98217689d
	github.com/ontio/ontology-eventbus v0.9.1
	github.com/saveio/carrier v0.0.0-20210519082359-9fc4d908c385
	github.com/saveio/max v0.0.0-20210624072549-139def04befb
	github.com/saveio/pylons v0.0.0-20210519083005-78a1ef20d8a0
	github.com/saveio/themis v1.0.121
	github.com/saveio/themis-go-sdk v0.0.0-20210702081903-52a40e927ed8
	github.com/syndtr/goleveldb v1.0.1-0.20190923125748-758128399b1d
)
