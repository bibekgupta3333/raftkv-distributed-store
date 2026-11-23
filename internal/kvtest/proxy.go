package kvtest

import (
	"io"
	"net"
	"sync"
)

// proxy forwards TCP from its own listener to target and can be cut: while
// blocked it drops live connections and refuses new ones. One proxy exists per
// ordered pair of nodes, so a partition can cut exactly the links it names.
type proxy struct {
	lis    net.Listener
	target string

	mu      sync.Mutex
	blocked bool
	conns   map[net.Conn]struct{}
	closed  bool
}

func newProxy(target string) (*proxy, error) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	p := &proxy{lis: lis, target: target, conns: map[net.Conn]struct{}{}}
	go p.serve()
	return p, nil
}

func (p *proxy) addr() string { return p.lis.Addr().String() }

func (p *proxy) serve() {
	for {
		c, err := p.lis.Accept()
		if err != nil {
			return
		}
		go p.handle(c)
	}
}

func (p *proxy) track(c net.Conn) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.blocked || p.closed {
		return false
	}
	p.conns[c] = struct{}{}
	return true
}

func (p *proxy) handle(in net.Conn) {
	if !p.track(in) {
		_ = in.Close()
		return
	}
	out, err := net.Dial("tcp", p.target)
	if err != nil {
		_ = in.Close()
		return
	}
	if !p.track(out) {
		_ = in.Close()
		_ = out.Close()
		return
	}
	done := make(chan struct{}, 2)
	pipe := func(dst, src net.Conn) {
		_, _ = io.Copy(dst, src)
		_ = dst.Close()
		_ = src.Close()
		done <- struct{}{}
	}
	go pipe(out, in)
	go pipe(in, out)
	<-done
	<-done
	p.mu.Lock()
	delete(p.conns, in)
	delete(p.conns, out)
	p.mu.Unlock()
}

// setBlocked cuts (true) or restores (false) the link.
func (p *proxy) setBlocked(b bool) {
	p.mu.Lock()
	p.blocked = b
	var drop []net.Conn
	if b {
		for c := range p.conns {
			drop = append(drop, c)
		}
	}
	p.mu.Unlock()
	for _, c := range drop {
		_ = c.Close()
	}
}

func (p *proxy) close() {
	p.mu.Lock()
	p.closed = true
	p.mu.Unlock()
	_ = p.lis.Close()
	p.setBlocked(true)
}
