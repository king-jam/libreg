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

func (s *Consul) Register(reg *CatalogRegistration, options *WriteOptions) error {
	catalog := s.api.Catalog()
	_, err := catalog.Register(reg, options)
	return err
}

// Deregister removes a node, service or check
func (s *Consul) Deregister(dereg *CatalogDeregistration, options *WriteOptions) error {
	catalog := s.api.Catalog()
	_, err := catalog.Deregister(dereg, options)
	return err
}

// Datacenters lists known datacenters
func (s *Consul) Datacenters() ([]string, error) {
	catalog := s.api.Catalog()
	dc, err := catalog.Datacenters()
	return dc, err
}

// Nodes lists all nodes in a given DC
func (s *Consul) Nodes(options *QueryOptions) ([]*Node, error) {
	catalog := s.api.Catalog()
	nodes, _, err := catalog.Nodes(options)
	return nodes, err
}

// Services lists all services in a given DC
func (s *Consul) Services(options *QueryOptions) (map[string][]string, error) {
	catalog := s.api.Catalog()
	services, _, err := catalog.Services(options)
	return services, err
}

// Service lists the nodes in a given service
func (s *Consul) Service(service, tag string, options *QueryOptions) ([]*CatalogService, error) {
	catalog := s.api.Catalog()
	services, _, err := catalog.Services(service, tag, options)
	return services, err
}

// Node lists the services provided by a given node
func (s *Consul) Node(node string, options *QueryOptions) (*CatalogNode, error) {
	catalog := s.api.Catalog()
	node, _, err := catalog.Node(node, options)
	return node, err
}
