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
	t  Task
	wg sync.WaitGroup
}

type taskFunc func(*taskStruct)

type Queue struct {
	closed int32
	ch     chan *taskStruct
	fn     taskFunc
	tp     sync.Pool
	wg     sync.WaitGroup
}

func runTask(ts *taskStruct) {
	ts.t.Run()
	ts.wg.Done()
}

func failTask(ts *taskStruct) {
	if f, ok := ts.t.(interface{ Failed() }); ok {
		f.Failed()
	}
	ts.wg.Done()
}

func stopTask(ts *taskStruct) {
	if f, ok := ts.t.(interface{ Stopped() }); ok {
		f.Stopped()
	}
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
		fn: runTask,
	}
	for i := 0; i < wk; i++ {
		q.wg.Add(1)
		go q.worker()
	}
	return q
}

func (q *Queue) worker() {
	for ts := range q.ch {
		q.fn(ts)
	}
	q.wg.Done()
}

func (q *Queue) getTS(t Task) (ts *taskStruct) {
	if v := q.tp.Get(); v != nil {
		ts = v.(*taskStruct)
	} else {
		ts = &taskStruct{}
	}
	ts.t = t
	ts.wg.Add(1)
	return
}

func (q *Queue) putTS(ts *taskStruct) {
	ts.wg.Wait()
	ts.t = nil
	q.tp.Put(ts)
}

func (q *Queue) AddTask(t Task) {
	ts := q.getTS(t)
	if atomic.LoadInt32(&q.closed) == 0 {
		select {
		case q.ch <- ts:
		default:
			go failTask(ts)
		}
	} else {
		go stopTask(ts)
	}
	q.putTS(ts)
}

func (q *Queue) Stop() {
	if atomic.SwapInt32(&q.closed, 1) != 0 {
		return
	}
	close(q.ch)
	q.wg.Wait()
}
