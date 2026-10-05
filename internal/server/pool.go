package server

import "sync"

const (
	defaultPoolSize  = 256
	defaultPoolQueue = 1024
)

// workerPool 是一个固定大小、带缓冲队列的协程池。
// submit 在池已停止时返回 false；stop 会优雅退出所有 worker。
type workerPool struct {
	jobs chan func()
	quit chan struct{}
	wg   sync.WaitGroup
	once sync.Once
}

func newWorkerPool(size, queue int) *workerPool {
	if size < 1 {
		size = 1
	}
	if queue < 0 {
		queue = 0
	}

	p := &workerPool{
		jobs: make(chan func(), queue),
		quit: make(chan struct{}),
	}

	for i := 0; i < size; i++ {
		p.wg.Add(1)
		go p.worker()
	}
	return p
}

func (p *workerPool) worker() {
	defer p.wg.Done()

	for {
		select {
		case job := <-p.jobs:
			job()
		case <-p.quit:
			// 退出前把已入队的任务处理完
			for {
				select {
				case job := <-p.jobs:
					job()
				default:
					return
				}
			}
		}
	}
}

// submit 提交任务；池已停止时返回 false
func (p *workerPool) submit(job func()) bool {
	select {
	case <-p.quit:
		return false
	default:
	}

	select {
	case p.jobs <- job:
		return true
	case <-p.quit:
		return false
	}
}

func (p *workerPool) stop() {
	p.once.Do(func() {
		close(p.quit)
		p.wg.Wait()
	})
}
