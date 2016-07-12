package consul

import (
	"crypto/tls"
	"errors"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/hashicorp/consul/api"
	"github.com/king-jam/libreg"
	"github.com/king-jam/libreg/registry"
)

const (
	// DefaultWatchWaitTime is how long we block for at a
	// time to check if the watched key has changed. This
	// affects the minimum time it takes to cancel a watch.
	DefaultWatchWaitTime = 15 * time.Second

	// RenewSessionRetryMax is the number of time we should try
	// to renew the session before giving up and throwing an error
	RenewSessionRetryMax = 5

	// MaxSessionDestroyAttempts is the maximum times we will try
	// to explicitely destroy the session attached to a lock after
	// the connectivity to the store has been lost
	MaxSessionDestroyAttempts = 5

	// defaultLockTTL is the default ttl for the consul lock
	defaultLockTTL = 20 * time.Second
)

var (
	// ErrMultipleEndpointsUnsupported is thrown when there are
	// multiple endpoints specified for Consul
	ErrMultipleEndpointsUnsupported = errors.New("consul does not support multiple endpoints")

	// ErrSessionRenew is thrown when the session can't be
	// renewed because the Consul version does not support sessions
	ErrSessionRenew = errors.New("cannot set or renew session for ttl, unable to operate on sessions")
)

// Consul is the receiver type for the
// Store interface
type Consul struct {
	sync.Mutex
	config *api.Config
	client *api.Client
}

// Register registers consul to libkv
func Register() {
	libreg.AddRegistry(registry.CONSUL, New)
}

// New creates a new Consul client given a list
// of endpoints and optional tls config
func New(endpoints []string, options *store.Config) (store.Store, error) {
	if len(endpoints) > 1 {
		return nil, ErrMultipleEndpointsUnsupported
	}

	s := &Consul{}

	// Create Consul client
	config := api.DefaultConfig()
	s.config = config
	config.HttpClient = http.DefaultClient
	config.Address = endpoints[0]
	config.Scheme = "http"

	// Set options
	if options != nil {
		if options.TLS != nil {
			s.setTLS(options.TLS)
		}
		if options.ConnectionTimeout != 0 {
			s.setTimeout(options.ConnectionTimeout)
		}
	}

	// Creates a new client
	client, err := api.NewClient(config)
	if err != nil {
		return nil, err
	}
	s.client = client

	return s, nil
}

// SetTLS sets Consul TLS options
func (s *Consul) setTLS(tls *tls.Config) {
	s.config.HttpClient.Transport = &http.Transport{
		TLSClientConfig: tls,
	}
	s.config.Scheme = "https"
}

// SetTimeout sets the timeout for connecting to Consul
func (s *Consul) setTimeout(time time.Duration) {
	s.config.WaitTime = time
}

func (s *Consul) renewSession(pair *api.KVPair, ttl time.Duration) error {
	// Check if there is any previous session with an active TTL
	session, err := s.getActiveSession(pair.Key)
	if err != nil {
		return err
	}

	if session == "" {
		entry := &api.SessionEntry{
			Behavior:  api.SessionBehaviorDelete, // Delete the key when the session expires
			TTL:       (ttl / 2).String(),        // Consul multiplies the TTL by 2x
			LockDelay: 1 * time.Millisecond,      // Virtually disable lock delay
		}

		// Create the key session
		session, _, err = s.client.Session().Create(entry, nil)
		if err != nil {
			return err
		}

		lockOpts := &api.LockOptions{
			Key:     pair.Key,
			Session: session,
		}

		// Lock and ignore if lock is held
		// It's just a placeholder for the
		// ephemeral behavior
		lock, _ := s.client.LockOpts(lockOpts)
		if lock != nil {
			lock.Lock(nil)
		}
	}

	_, _, err = s.client.Session().Renew(session, nil)
	return err
}

// getActiveSession checks if the key already has
// a session attached
func (s *Consul) getActiveSession(key string) (string, error) {
	pair, _, err := s.client.KV().Get(key, nil)
	if err != nil {
		return "", err
	}
	if pair != nil && pair.Session != "" {
		return pair.Session, nil
	}
	return "", nil
}

// Get the value at "key", returns the last modified index
// to use in conjunction to CAS calls
func (s *Consul) Get(key string) (*store.KVPair, error) {
	options := &api.QueryOptions{
		AllowStale:        false,
		RequireConsistent: true,
	}

	pair, meta, err := s.client.KV().Get(s.normalize(key), options)
	if err != nil {
		return nil, err
	}

	// If pair is nil then the key does not exist
	if pair == nil {
		return nil, store.ErrKeyNotFound
	}

	return &store.KVPair{Key: pair.Key, Value: pair.Value, LastIndex: meta.LastIndex}, nil
}

// Put a value at "key"
func (s *Consul) Put(key string, value []byte, opts *store.WriteOptions) error {
	key = s.normalize(key)

	p := &api.KVPair{
		Key:   key,
		Value: value,
		Flags: api.LockFlagValue,
	}

	if opts != nil && opts.TTL > 0 {
		// Create or renew a session holding a TTL. Operations on sessions
		// are not deterministic: creating or renewing a session can fail
		for retry := 1; retry <= RenewSessionRetryMax; retry++ {
			err := s.renewSession(p, opts.TTL)
			if err == nil {
				break
			}
			if retry == RenewSessionRetryMax {
				return ErrSessionRenew
			}
		}
	}

	_, err := s.client.KV().Put(p, nil)
	return err
}

// Delete a value at "key"
func (s *Consul) Delete(key string) error {
	if _, err := s.Get(key); err != nil {
		return err
	}
	_, err := s.client.KV().Delete(s.normalize(key), nil)
	return err
}

// Exists checks that the key exists inside the store
func (s *Consul) Exists(key string) (bool, error) {
	_, err := s.Get(key)
	if err != nil {
		if err == store.ErrKeyNotFound {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// List child nodes of a given directory
func (s *Consul) List(directory string) ([]*store.KVPair, error) {
	pairs, _, err := s.client.KV().List(s.normalize(directory), nil)
	if err != nil {
		return nil, err
	}
	if len(pairs) == 0 {
		return nil, store.ErrKeyNotFound
	}

	kv := []*store.KVPair{}

	for _, pair := range pairs {
		if pair.Key == directory {
			continue
		}
		kv = append(kv, &store.KVPair{
			Key:       pair.Key,
			Value:     pair.Value,
			LastIndex: pair.ModifyIndex,
		})
	}

	return kv, nil
}

// DeleteTree deletes a range of keys under a given directory
func (s *Consul) DeleteTree(directory string) error {
	if _, err := s.List(directory); err != nil {
		return err
	}
	_, err := s.client.KV().DeleteTree(s.normalize(directory), nil)
	return err
}
