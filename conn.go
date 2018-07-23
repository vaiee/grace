package refined

import (
	"errors"
	"io"
	"sync"
	"time"
)

var (
	PoolClosed    = errors.New("conn pool is closed")
	InvalidConfig = errors.New("invalid config")
)

type Poolable interface {
	io.Closer
	GetActiveTime() time.Time
}

type builder func() (Poolable, error)

type Conn struct {
	sync.Mutex
	idle    time.Duration // 每个连接的空闲时间
	pool    chan Poolable // 连接池
	max     int           // 最大连接数
	active  int           // 可用的连接数
	closed  bool          // 连接池是否已关闭
	builder builder       // 构造连接
}

// 获取连接
func (conn *Conn) Acquire() (Poolable, error) {
	if conn.closed {
		return nil, PoolClosed
	}
	for {
		closer, err := conn.acquire()
		if err != nil {
			return nil, err
		}
		if closer.GetActiveTime().Add(time.Duration(conn.idle)).Before(time.Now()) {
			conn.Close(closer)
			continue
		}
		return closer, nil
	}
}

func (conn *Conn) acquire() (Poolable, error) {
	select {
	case closer := <-conn.pool:
		return closer, nil
	default:
		conn.Lock()
		if conn.active >= conn.max {
			closer := <-conn.pool
			conn.Unlock()
			return closer, nil
		}
		closer, err := conn.builder()
		if err != nil {
			conn.Unlock()
			return nil, err
		}
		conn.active++
		conn.pool <- closer
		conn.Unlock()
		return closer, nil
	}
}

// 回收连接
func (conn *Conn) Regain(closer Poolable) error {
	if conn.closed {
		return PoolClosed
	}
	conn.Lock()
	conn.pool <- closer
	conn.Unlock()
	return nil
}

// 关闭连接
func (conn *Conn) Close(closer Poolable) error {
	conn.Lock()
	err := closer.Close()
	if err != nil {
		return err
	}
	conn.active--
	conn.Unlock()
	return nil
}

// 关闭连接池
func (conn *Conn) Release() error {
	if conn.closed {
		return PoolClosed
	}
	conn.Lock()
	close(conn.pool)
	for closer := range conn.pool {
		conn.active--
		closer.Close()
	}
	conn.closed = true
	conn.Unlock()
	return nil
}

func NewConnManager(builder builder, max, idle time.Duration) (*Conn, error) {
	if max <= 0 || idle < 0 {
		return nil, InvalidConfig
	}
	conn := &Conn{
		idle:    idle,
		max:     max,
		pool:    make(chan Poolable, max),
		closed:  false,
		builder: builder,
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
