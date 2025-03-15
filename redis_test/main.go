package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/go-redis/redis/v8"
)

func main() {
	// 创建 Redis 客户端
	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379", // Redis 服务器地址
		Password: "",               // 密码
		DB:       0,                // 使用默认 DB
	})

	ctx := context.Background()

	// 测试参数
	numOperations := 1000
	var totalSetLatency, totalGetLatency time.Duration

	// 压力测试
	for i := 0; i < numOperations; i++ {
		key, val := fmt.Sprintf("key%d", i), fmt.Sprintf("val%d", i)

		// 测试 Set 操作
		startSet := time.Now()
		err := rdb.Set(ctx, key, val, 1000*time.Second).Err()
		if err != nil {
			log.Fatalf("Set operation failed: %v", err)
		}
		elapsedSet := time.Since(startSet)
		totalSetLatency += elapsedSet

		// 测试 Get 操作
		startGet := time.Now()
		retrievedVal, err := rdb.Get(ctx, key).Result()
		if err != nil {
			log.Fatalf("Get operation failed: %v", err)
		}
		elapsedGet := time.Since(startGet)
		totalGetLatency += elapsedGet

		// 验证获取的值是否正确
		if retrievedVal != val {
			log.Fatalf("Value mismatch: expected %s, got %s", val, retrievedVal)
		}
	}

	// 计算平均延迟
	avgSetLatency := totalSetLatency / time.Duration(numOperations)
	avgGetLatency := totalGetLatency / time.Duration(numOperations)

	// 输出结果
	fmt.Printf("Pressure Test Results:\n")
	fmt.Printf("Total Set operations: %d, Total Set latency: %v, Avg Set latency: %v\n", numOperations, totalSetLatency, avgSetLatency)
	fmt.Printf("Total Get operations: %d, Total Get latency: %v, Avg Get latency: %v\n", numOperations, totalGetLatency, avgGetLatency)
}
