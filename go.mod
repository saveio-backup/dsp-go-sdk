module github.com/saveio/dsp-go-sdk

go 1.16

replace github.com/saveio/themis => ../themis

require (
	github.com/gogo/protobuf v1.3.2
	github.com/golang/protobuf v1.5.2
	github.com/google/uuid v1.2.0
	github.com/hashicorp/golang-lru v0.5.5-0.20210104140557-80c98217689d
	github.com/ontio/ontology-eventbus v0.9.1
	github.com/saveio/carrier v0.0.0-20210802055929-7567cc29dfc9
	github.com/saveio/max v0.0.0-20211028065147-9634b553b277
	github.com/saveio/pylons v0.0.0-20211109032939-13c30c056ead
	github.com/saveio/themis v1.0.163
	github.com/saveio/themis-go-sdk v0.0.0-20211105080535-6ccfe8eedbe9
	github.com/syndtr/goleveldb v1.0.1-0.20190923125748-758128399b1d
)
