module github.com/saveio/dsp-go-sdk

go 1.16

require (
	github.com/ethereum/go-ethereum v1.10.15
	github.com/gogo/protobuf v1.3.2
	github.com/golang/protobuf v1.5.2
	github.com/google/uuid v1.2.0
	github.com/hashicorp/golang-lru v0.5.5-0.20210104140557-80c98217689d
	github.com/itchyny/base58-go v0.1.0
	github.com/ontio/ontology-eventbus v0.9.1
	github.com/saveio/carrier v0.0.0-20230322093539-24eaadd546b5
	github.com/saveio/max v0.0.0-20230324091118-889c76b11561
	github.com/saveio/pylons v0.0.0-20230322094600-b5981ca8ed91
	github.com/saveio/themis v1.0.175
	github.com/saveio/themis-go-sdk v0.0.0-20230314033227-3033a22d3bcd
	github.com/syndtr/goleveldb v1.0.1-0.20210819022825-2ae1ddf74ef7
)

replace (
	github.com/saveio/themis-go-sdk => ../themis-go-sdk
	github.com/saveio/themis => ../themis
	github.com/saveio/max => ../max
)