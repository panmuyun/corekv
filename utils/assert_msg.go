package utils

import (
	"log"

	"github.com/pkg/errors"
)

func AssertTrue(condition bool) {
	if !condition {
		log.Fatalf("%+v", errors.Errorf("Assert failed"))
	}
	// %+v: 格式化输出结构体类型的变量时包括字段名称
}

// AssertTruef is AssertTrue with extra info.
func AssertTruef(condition bool, format string, args ...interface{}) {
	if !condition {
		log.Fatalf("%+v", errors.Errorf(format, args...))
	}
}
