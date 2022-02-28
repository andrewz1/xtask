package xtask

import (
	"runtime"
	"sync"
	"sync/atomic"
)

const defaultQueueLen = 100000

type Task interface {
	Run()
}

type taskStruct struct {
	Task
	wg sync.WaitGroup
}

type Queue struct {
	closed int32
	ch     chan *taskStruct
	tp     sync.Pool
	wg     sync.WaitGroup
}

func (ts *taskStruct) runTS() {
	ts.Run()
	ts.wg.Done()
}

func NewQueue(wk, ql int) (q *Queue) {
	if wk <= 0 {
		wk = runtime.NumCPU()
	}
	if ql <= 0 {
		ql = defaultQueueLen
	}
	q = &Queue{
		ch: make(chan *taskStruct, ql),
		tp: sync.Pool{
			New: func() interface{} {
				return &taskStruct{}
			},
		},
	}
	for i := 0; i < wk; i++ {
		q.wg.Add(1)
		go q.worker()
	}
	return q
}

func (q *Queue) worker() {
	for ts := range q.ch {
		ts.runTS()
	}
	q.wg.Done()
}

func (q *Queue) getTS(t Task) (ts *taskStruct) {
	ts = q.tp.Get().(*taskStruct)
	ts.Task = t
	ts.wg.Add(1)
	return
}

func (q *Queue) putTS(ts *taskStruct) {
	ts.wg.Wait()
	ts.Task = nil
	q.tp.Put(ts)
}

func (q *Queue) AddTask(t Task) {
	ts := q.getTS(t)
	defer func() {
		q.putTS(ts)
		runtime.Gosched()
	}()
	if !q.Stopped() {
		q.ch <- ts
	}
}

func (q *Queue) Stop() {
	if atomic.CompareAndSwapInt32(&q.closed, 0, 1) {
		close(q.ch)
		q.wg.Wait()
	}
}

func (q *Queue) Stopped() bool {
	return atomic.LoadInt32(&q.closed) != 0
}
