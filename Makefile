gen: gen-go

gen-go:
	protoc --proto_path=proto --go_out=plugins=grpc:paxos paxos.proto