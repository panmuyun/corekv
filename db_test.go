// Copyright 2021 panmuyun Project Authors
//
// Licensed under the Apache License, Version 2.0 (the "License")
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package corekv

import (
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/panmuyun/corekv/utils"
)

func TestAPI(t *testing.T) {
	clearDir()
	db := Open(opt)
	defer func() { _ = db.Close() }()
	// 写入
	for i := 0; i < 50; i++ {
		key, val := fmt.Sprintf("key%d", i), fmt.Sprintf("val%d", i)
		e := utils.NewEntry([]byte(key), []byte(val)).WithTTL(1000 * time.Second)
		if err := db.Set(e); err != nil {
			t.Fatal(err)
		}
		// 查询
		if entry, err := db.Get([]byte(key)); err != nil {
			t.Fatal(err)
		} else {
			t.Logf("db.Get key=%s, value=%s, expiresAt=%d", entry.Key, entry.Value, entry.ExpiresAt)
		}
	}

	for i := 0; i < 40; i++ {
		key, _ := fmt.Sprintf("key%d", i), fmt.Sprintf("val%d", i)
		if err := db.Del([]byte(key)); err != nil {
			t.Fatal(err)
		}
	}

	// 迭代器
	iter := db.NewIterator(&utils.Options{
		Prefix: []byte("hello"),
		IsAsc:  false,
	})
	defer func() { _ = iter.Close() }()
	defer func() { _ = iter.Close() }()
	for iter.Rewind(); iter.Valid(); iter.Next() {
		it := iter.Item()
		t.Logf("db.NewIterator key=%s, value=%s, expiresAt=%d", it.Entry().Key, it.Entry().Value, it.Entry().ExpiresAt)
	}
	t.Logf("db.Stats.EntryNum=%+v", db.Info().EntryNum)
	// 删除
	if err := db.Del([]byte("hello")); err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 10; i++ {
		key, val := fmt.Sprintf("key%d", i), fmt.Sprintf("val%d", i)
		e := utils.NewEntry([]byte(key), []byte(val)).WithTTL(1000 * time.Second)
		if err := db.Set(e); err != nil {
			t.Fatal(err)
		}
		// 查询
		if entry, err := db.Get([]byte(key)); err != nil {
			t.Fatal(err)
		} else {
			t.Logf("db.Get key=%s, value=%s, expiresAt=%d", entry.Key, entry.Value, entry.ExpiresAt)
		}
	}

}

const (
	numOperations = 10000 // 总操作数
	numGoroutines = 2     // 并发 Goroutine 数
	keyLength     = 16    // 键的长度
	valueLength   = 64    // 值的长度
)

var (
	keys   = make([]string, numOperations)
	values = make([]string, numOperations)
)

func init() {
	// 初始化随机键值对
	rand.Seed(time.Now().UnixNano())
	for i := 0; i < numOperations; i++ {
		keys[i] = randString(keyLength)
		values[i] = randString(valueLength)
	}
}
func randString(length int) string {
	const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	b := make([]byte, length)
	for i := range b {
		b[i] = charset[rand.Intn(len(charset))]
	}
	return string(b)
}

// 单协程测试
func TestCoreKVSinglePressure(t *testing.T) {
	clearDir()
	db := Open(opt)
	defer func() { _ = db.Close() }()

	// 测试参数
	numOperations := 1000 // 设置操作次数
	var totalSetLatency time.Duration
	var totalGetLatency time.Duration

	// 压力测试
	for i := 0; i < numOperations; i++ {
		key, val := fmt.Sprintf("key%d", i), fmt.Sprintf("val%d", i)

		// 测试 Set 操作
		startSet := time.Now()
		e := utils.NewEntry([]byte(key), []byte(val)).WithTTL(1000 * time.Second)
		if err := db.Set(e); err != nil {
			t.Fatal(err)
		}
		elapsedSet := time.Since(startSet)
		totalSetLatency += elapsedSet

		// 测试 Get 操作
		startGet := time.Now()
		if entry, err := db.Get([]byte(key)); err != nil {
			t.Fatal(err)
		} else {
			t.Logf("db.Get key=%s, value=%s, expiresAt=%d", entry.Key, entry.Value, entry.ExpiresAt)
		}
		elapsedGet := time.Since(startGet)
		totalGetLatency += elapsedGet
	}

	// 计算平均延迟
	avgSetLatency := totalSetLatency / time.Duration(numOperations)
	avgGetLatency := totalGetLatency / time.Duration(numOperations)

	// 输出结果
	fmt.Printf("Pressure Test Results:\n")
	fmt.Printf("Total Set operations: %d, Total Set latency: %v, Avg Set latency: %v\n", numOperations, totalSetLatency, avgSetLatency)
	fmt.Printf("Total Get operations: %d, Total Get latency: %v, Avg Get latency: %v\n", numOperations, totalGetLatency, avgGetLatency)
}

func TestCoreKVMulti(t *testing.T) {
	clearDir()
	db := Open(opt)
	defer db.Close()
	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	start := time.Now()

	// 并发执行 Set 操作
	for i := 0; i < numGoroutines; i++ {
		go func(workerID int) {
			defer wg.Done()
			for j := workerID; j < numOperations; j += numGoroutines {
				e := utils.NewEntry([]byte(keys[j]), []byte(values[j]))
				if err := db.Set(e); err != nil {
					t.Errorf("Set failed: %v", err)
					return
				}
			}
		}(i)
	}
	wg.Wait()

	setElapsed := time.Since(start)
	fmt.Printf("corekv Set: %d operations completed in %v\n", numOperations, setElapsed)
	fmt.Printf("corekv Set throughput: %.2f ops/sec\n", float64(numOperations)/setElapsed.Seconds())

	// 并发执行 Get 操作
	wg.Add(numGoroutines)
	start = time.Now()

	for i := 0; i < numGoroutines; i++ {
		go func(workerID int) {
			defer wg.Done()
			for j := workerID; j < numOperations; j += numGoroutines {
				if _, err := db.Get([]byte(keys[j])); err != nil {
					t.Errorf("Get failed: %v", err)
					return
				}
			}
		}(i)
	}
	wg.Wait()

	getElapsed := time.Since(start)
	fmt.Printf("corekv Get: %d operations completed in %v\n", numOperations, getElapsed)
	fmt.Printf("corekv Get throughput: %.2f ops/sec\n", float64(numOperations)/getElapsed.Seconds())
}
