package gohbase

import (
	"errors"
	"sync"
	"sync/atomic"
	"time"
)

var ErrPoolClosed = errors.New("pool closed")

type Config struct {
	Addr string
	// 最小连接数
	MinConn int
	// 最大连接数
	MaxConn int
	// 连接最长空闲时间
	IdleTimeout time.Duration
}

type Conn struct {
	*conn
	ownerPool   *Pool
	idleTimeout time.Duration
	activeTime  time.Time
}

func (c *Conn) Close() error {
	timer := time.NewTimer(c.idleTimeout)
	defer timer.Stop()

	select {
	case <-c.ownerPool.exitC:
		atomic.AddInt32(&c.ownerPool.connNum, -1)
		return c.conn.Close()
	case <-timer.C:
		atomic.AddInt32(&c.ownerPool.connNum, -1)
		return c.conn.Close()
	case c.ownerPool.idleConnC <- c:

	}
	return nil
}

type Pool struct {
	cfg         Config
	mu          sync.Mutex
	idleConnC   chan *Conn
	factory     func() (*conn, error)
	idleTimeout time.Duration
	exitC       chan struct{}

	connNum int32
}

func setDefaultConfig(config *Config) error {
	if config.Addr == "" {
		return errors.New("addr can not be empty")
	}

	if config.MinConn <= 0 { // default 16
		config.MinConn = 16
	}

	if config.MaxConn <= 0 { // default 256
		config.MaxConn = 256
	}

	if config.IdleTimeout <= 0 {
		config.IdleTimeout = time.Second * 10
	}

	if config.MaxConn < config.MinConn {
		return errors.New("invalid 'maxConn' and 'minConn'")
	}

	return nil
}

func NewPool(config Config) (*Pool, error) {
	if err := setDefaultConfig(&config); err != nil {
		return nil, err
	}

	p := &Pool{
		cfg:       config,
		idleConnC: make(chan *Conn, config.MinConn),
		factory: func() (service *conn, err error) {
			return newConn(config.Addr)
		},
		idleTimeout: config.IdleTimeout,
		exitC:       make(chan struct{}),
	}

	for i := 0; i < config.MinConn; i++ {
		conn, err := p.factory()
		if err != nil {
			_ = p.Close()
			return nil, err
		}

		p.idleConnC <- &Conn{
			conn:        conn,
			ownerPool:   p,
			idleTimeout: p.idleTimeout,
		}
	}

	return p, nil
}

func (p *Pool) Do(f func(conn *Conn) error) error {
	conn, err := p.Get()
	if err != nil {
		return err
	}
	defer conn.Close()

	return f(conn)
}

// 获取连接
func (p *Pool) Get() (*Conn, error) {
	select {
	case <-p.exitC:
		return nil, ErrPoolClosed
	case conn := <-p.idleConnC:
		return conn, nil
	default:
		connNum := atomic.AddInt32(&p.connNum, 1)
		if p.cfg.MinConn+int(connNum) <= p.cfg.MaxConn {
			p.mu.Lock()
			defer p.mu.Unlock()
			conn, err := p.factory()
			if err != nil {
				return nil, err
			}

			return &Conn{
				conn:      conn,
				ownerPool: p,
			}, err
		} else {
			connNum = atomic.AddInt32(&p.connNum, -1)
			if conn := <-p.idleConnC; conn != nil {
				return conn, nil
			}
			return nil, ErrPoolClosed
		}
	}
}

func (p *Pool) Close() error {
	close(p.exitC)
	close(p.idleConnC)

	for conn := range p.idleConnC {
		conn.conn.Close()
	}

	return nil
}
