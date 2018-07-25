package refined

import (
	"errors"
	"io"
	"sync"
)

var (
	PoolClosed    = errors.New("conn pool is closed")
	InvalidConfig = errors.New("invalid config")
)

type Poolable interface {
	io.Closer
	Done() <-chan struct{}
}

type builder func() (Poolable, error)

type Conn struct {
	pool    chan Poolable // 可关闭对象池
	max     int           // 池容量
	active  int           // 可用的对象数
	closed  bool          // 池是否已关闭
	builder builder       // 构造对象
	mutex   *sync.Mutex
	event   chan struct{} // 关闭对象之后通知排队者有对象可用
}

// 获取对象
func (conn *Conn) Acquire() (Poolable, error) {
	if conn.closed {
		return nil, PoolClosed
	}
	for {
		closer, err := conn.acquire()
		if err != nil {
			return nil, err
		}
		select {
		case <-closer.Done():
			conn.Close(closer)
			continue
		default:
			return closer, nil
		}
	}
}

func (conn *Conn) acquire() (Poolable, error) {
acquire:
	select {
	case closer := <-conn.pool:
		return closer, nil
	default:
		conn.mutex.Lock()
		if conn.active >= conn.max {
			conn.mutex.Unlock()
			select {
			case closer := <-conn.pool:
				return closer, nil
			case <-conn.event:
				goto acquire
			}
		}
		closer, err := conn.builder()
		if err != nil {
			conn.mutex.Unlock()
			return nil, err
		}
		conn.active++
		conn.mutex.Unlock()
		conn.pool <- closer
		return <-conn.pool, nil
	}
}

// 回收对象
func (conn *Conn) Regain(closer Poolable) error {
	if conn.closed {
		return PoolClosed
	}
	conn.pool <- closer
	return nil
}

// 关闭对象
func (conn *Conn) Close(closer Poolable) error {
	conn.mutex.Lock()
	err := closer.Close()
	if err != nil {
		return err
	}
	conn.active--
	conn.mutex.Unlock()
	conn.event <- struct{}{}
	return nil
}

// 关闭对象池
func (conn *Conn) Release() error {
	if conn.closed {
		return PoolClosed
	}
	conn.mutex.Lock()
	close(conn.pool)
	for closer := range conn.pool {
		conn.active--
		closer.Close()
	}
	conn.closed = true
	conn.mutex.Unlock()
	return nil
}

// 创建对象管理器
func NewConnManager(max int, builder builder) (*Conn, error) {
	if max <= 0 {
		return nil, InvalidConfig
	}
	conn := &Conn{
		max:     max,
		pool:    make(chan Poolable, max),
		closed:  false,
		builder: builder,
		mutex:   new(sync.Mutex),
		event:   make(chan struct{}),
	}
	for i := 0; i < max; i++ {
		closer, err := builder()
		if err != nil {
			return nil, err
		}
		conn.active++
		conn.pool <- closer
	}
	return conn, nil
}
