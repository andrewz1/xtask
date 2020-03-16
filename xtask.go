package xtask

import (
	"runtime"
	"sync"
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
	ch chan *taskStruct
	fn taskFunc
	tp sync.Pool
}

func runTask(ts *taskStruct) {
	ts.t.Run()
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
		go q.worker()
	}
	return q
}

func (q *Queue) worker() {
	for ts := range q.ch {
		q.fn(ts)
	}
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
	select {
	case q.ch <- ts:
	default:
		go func() {
			if f, ok := t.(interface{ Fail() }); ok {
				f.Fail()
			}
			ts.wg.Done()
		}()
	}
	q.putTS(ts)
}
