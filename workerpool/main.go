package main

import (
	"context"
	"errors"
	"log/slog"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"time"
)

var ErrWorkerPoolClosed = errors.New("worker pool closed")

type Task[R any] struct {
	ID      int
	Fn      func(context.Context) (R, error)
	Timeout time.Duration
}

type Result[R any] struct {
	TaskID int
	Value  R
	Err    error
}

type WorkerPool[R any] struct {
	workerNum int

	taskCh   chan Task[R]
	resultCh chan Result[R]

	wg     sync.WaitGroup
	closed atomic.Bool

	ctx context.Context
}

func NewWorkerPool[R any](ctx context.Context, workerNum int, queueSize int) *WorkerPool[R] {
	return &WorkerPool[R]{
		workerNum: workerNum,

		taskCh:   make(chan Task[R], queueSize),
		resultCh: make(chan Result[R], queueSize),

		ctx: ctx,
	}
}

func (p *WorkerPool[R]) Start() {
	p.wg.Add(p.workerNum)
	for i := 0; i < p.workerNum; i++ {
		go p.worker(i)
	}
	slog.Info("WorkerPool Started")
}

func (p *WorkerPool[R]) worker(workerID int) {
	defer p.wg.Done()

	for {
		task, ok := <-p.taskCh
		if !ok {
			return
		}
		func() {
			defer func() {
				if r := recover(); r != nil {
					slog.Error("worker panic recovered", "workerID", workerID, "taskID", task.ID, "r", r)
					p.resultCh <- Result[R]{TaskID: task.ID, Err: errors.New("panic")}
				}
			}()
			ctx := p.ctx
			var cancel context.CancelFunc
			if task.Timeout > 0 {
				ctx, cancel = context.WithTimeout(ctx, task.Timeout)
				defer cancel()
			}

			v, err := task.Fn(ctx)
			p.resultCh <- Result[R]{TaskID: task.ID, Value: v, Err: err}
		}()
	}
}

func (p *WorkerPool[R]) Submit(task Task[R]) error {
	if p.closed.Load() {
		return ErrWorkerPoolClosed
	}
	select {
	case <-p.ctx.Done():
		return p.ctx.Err()
	default:
		p.taskCh <- task
	}
	return nil
}

func (p *WorkerPool[R]) Shutdown() {
	if !p.closed.CompareAndSwap(false, true) {
		return
	}
	close(p.taskCh)
	p.wg.Wait()
	close(p.resultCh)
	slog.Info("WorkerPool Shutdown")
}

func (p *WorkerPool[R]) Results() <-chan Result[R] {
	return p.resultCh
}

func testFn(ctx context.Context) (int, error) {
	randSecs := rand.IntN(5) + 1
	for i := 0; i < randSecs; i++ {
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		default:
			time.Sleep(time.Second)
		}
	}
	return randSecs, nil
}

func main() {
	ctx := context.Background()
	workerPool := NewWorkerPool[int](ctx, 3, 5)
	workerPool.Start()

	go func() {
		taskCnt := 10
		for i := 0; i < taskCnt; i++ {
			err := workerPool.Submit(Task[int]{
				ID:      i,
				Fn:      testFn,
				Timeout: 3 * time.Second,
			})
			if err != nil {
				slog.Error("failed to submit task", "error", err)
			}
		}
		workerPool.Shutdown()

	}()

	for ret := range workerPool.Results() {
		if ret.Err == nil {
			slog.Info("task done", "taskID", ret.TaskID, "result", ret.Value)
		} else {
			slog.Error("task error", "taskID", ret.TaskID, "error", ret.Err)
		}
	}

}
