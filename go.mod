module github.com/panmuyun/corekv

go 1.16

require github.com/go-redis/redis/v8 v8.11.5

require (
	github.com/cespare/xxhash/v2 v2.1.2
	github.com/golang/protobuf v1.5.2
	github.com/pkg/errors v0.9.1
	github.com/stretchr/testify v1.7.0
	golang.org/x/sys v0.0.0-20211216021012-1d35b9e2eb4e
	google.golang.org/protobuf v1.27.1 // indirect
)

replace github.com/panmuyun/corekv => .
