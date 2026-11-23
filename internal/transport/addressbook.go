package transport

import (
	"context"
	"fmt"
	"net"
	"slices"
)

// LookupFunc resolves a host name to IP addresses. net.DefaultResolver's
// LookupHost has this shape.
type LookupFunc func(ctx context.Context, host string) ([]string, error)

// AddressBook is the static peer address book built from config (WBS 2.2.6):
// node ID -> "host:port". It never changes after construction, so it is safe
// for concurrent use. Host names are re-resolved on every (re)connect via
// [AddressBook.Resolve], so a pod that comes back with a new IP is found again
// (headless-Service DNS, D-16).
type AddressBook struct {
	addrs  map[uint64]string
	lookup LookupFunc
}

// NewAddressBook copies peers (node ID -> host:port) and validates every
// address.
func NewAddressBook(peers map[uint64]string) (*AddressBook, error) {
	b := &AddressBook{addrs: make(map[uint64]string, len(peers)), lookup: net.DefaultResolver.LookupHost}
	for id, addr := range peers {
		if id == 0 {
			return nil, fmt.Errorf("transport: address book: node ID 0 is invalid")
		}
		if _, _, err := net.SplitHostPort(addr); err != nil {
			return nil, fmt.Errorf("transport: address book: node %d address %q: %w", id, addr, err)
		}
		b.addrs[id] = addr
	}
	return b, nil
}

// WithLookup returns a copy that resolves host names with fn. Tests use it to
// simulate DNS changes.
func (b *AddressBook) WithLookup(fn LookupFunc) *AddressBook {
	return &AddressBook{addrs: b.addrs, lookup: fn}
}

// Addr returns the configured address of node id.
func (b *AddressBook) Addr(id uint64) (string, bool) {
	a, ok := b.addrs[id]
	return a, ok
}

// Has reports whether id is a known member (D-31: unknown senders are ignored).
func (b *AddressBook) Has(id uint64) bool { _, ok := b.addrs[id]; return ok }

// IDs returns the known node IDs in ascending order.
func (b *AddressBook) IDs() []uint64 {
	ids := make([]uint64, 0, len(b.addrs))
	for id := range b.addrs {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	return ids
}

// Resolve returns a dialable ip:port for node id, doing a fresh DNS lookup when
// the configured host is a name. IPv4 is preferred for determinism.
func (b *AddressBook) Resolve(ctx context.Context, id uint64) (string, error) {
	addr, ok := b.addrs[id]
	if !ok {
		return "", fmt.Errorf("transport: unknown node %d", id)
	}
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return "", err
	}
	if net.ParseIP(host) != nil {
		return addr, nil
	}
	ips, err := b.lookup(ctx, host)
	if err != nil {
		return "", fmt.Errorf("transport: resolve node %d (%s): %w", id, host, err)
	}
	if len(ips) == 0 {
		return "", fmt.Errorf("transport: resolve node %d (%s): no addresses", id, host)
	}
	pick := ips[0]
	for _, ip := range ips {
		if p := net.ParseIP(ip); p != nil && p.To4() != nil {
			pick = ip
			break
		}
	}
	return net.JoinHostPort(pick, port), nil
}
