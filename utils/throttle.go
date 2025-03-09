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
package utils

import "sync"

// Throttle allows a limited number of workers to run at a time. It also
// provides a mechanism to check for errors encountered by workers and wait for
// them to finish.
// Throttle 允许在任何给定时间最多运行一定数量的工作者。它还提供了检查工作者遇到的错误并等待他们完成的机制。
type Throttle struct {
	once      sync.Once
	wg        sync.WaitGroup
	ch        chan struct{}
	errCh     chan error
	finishErr error
}

// NewThrottle creates a new throttle with a max number of workers.
// NewThrottle 创建一个新的 Throttle 实例，设置最多可以运行的工作者数量。
func NewThrottle(max int) *Throttle {
	return &Throttle{
		ch:    make(chan struct{}, max),
		errCh: make(chan error, max),
	}
}

// Do should be called by workers before they start working. It blocks if there
// are already maximum number of workers working. If it detects an error from
// previously Done workers, it would return it.
// Do 应该由工作者在开始工作前调用。如果已经有最大数量的工作者在运行，它会阻塞。
// 如果它检测到之前调用 Do 的工作者中的错误，它会返回该错误。
func (t *Throttle) Do() error {
	for {
		select {
		case t.ch <- struct{}{}:
			t.wg.Add(1)
			return nil
		case err := <-t.errCh:
			if err != nil {
				return err
			}
		}
	}
}

// Done should be called by workers when they finish working. They can also
// pass the error status of work done.
// Done 应该由工作者在完成工作时调用。他们也可以传递工作的错误状态。
func (t *Throttle) Done(err error) {
	if err != nil {
		t.errCh <- err
	}
	select {
	case <-t.ch:
	default:
		panic("Throttle Do Done mismatch")
	}
	t.wg.Done()
}

// Finish waits until all workers have finished working. It would return any error passed by Done.
// If Finish is called multiple time, it will wait for workers to finish only once(first time).
// From next calls, it will return same error as found on first call.
// Finish 等待所有工作者完成工作。它会返回 Do 方法中传递的任何错误。
// 如果 Finish 被多次调用，它只会等待工作者完成一次（第一次调用）。
// 从第二次及以后的调用，它会返回与第一次调用时相同的错误。
func (t *Throttle) Finish() error {
	t.once.Do(func() {
		t.wg.Wait()
		close(t.ch)
		close(t.errCh)
		for err := range t.errCh {
			if err != nil {
				t.finishErr = err
				return
			}
		}
	})

	return t.finishErr
}
