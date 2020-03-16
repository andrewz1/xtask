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

type Pool struct {
	ch chan *taskStruct
	fn taskFunc
	tp sync.Pool
}

func runTask(ts *taskStruct) {
	ts.t.Run()
}

func NewQueue(wk, ql int) (p *Pool) {
	if wk <= 0 {
		wk = runtime.NumCPU()
	}
	if ql <= 0 {
		ql = defaultQueueLen
	}
	p = &Pool{
		ch: make(chan *taskStruct, ql),
		fn: runTask,
	}
	for i := 0; i < wk; i++ {
		go p.worker()
	}
	return p
}

func (p *Pool) worker() {
	for ts := range p.ch {
		p.fn(ts)
	}
}

func (p *Pool) getTS(t Task) (ts *taskStruct) {
	if v := p.tp.Get(); v != nil {
		ts = v.(*taskStruct)
	} else {
		ts = &taskStruct{}
	}
	ts.t = t
	ts.wg.Add(1)
	return
}

func (p *Pool) putTS(ts *taskStruct) {
	ts.wg.Done()
	ts.t = nil
	p.tp.Put(ts)
}

func (p *Pool) Queue(t Task) {
	ts := p.getTS(t)
	p.ch <- ts
	p.putTS(ts)
}
