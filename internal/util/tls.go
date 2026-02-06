package util

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net/http"
	"os"

	"github.com/leonunix/oqbridge/internal/config"
)

// NewHTTPClient builds an *http.Client with TLS settings from the given config.
// If neither SkipVerify nor CACert is set, it returns a default client.
func NewHTTPClient(tc config.TLSConfig) (*http.Client, error) {
	if !tc.SkipVerify && tc.CACert == "" {
		return &http.Client{}, nil
	}

	tlsConfig := &tls.Config{}

	if tc.SkipVerify {
		tlsConfig.InsecureSkipVerify = true
	}

	if tc.CACert != "" {
		caCert, err := os.ReadFile(tc.CACert)
		if err != nil {
			return nil, fmt.Errorf("reading CA certificate %s: %w", tc.CACert, err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(caCert) {
			return nil, fmt.Errorf("failed to parse CA certificate %s", tc.CACert)
		}
		tlsConfig.RootCAs = pool
	}

	return &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: tlsConfig,
		},
	}, nil
}

// NewTLSTransport builds an *http.Transport with TLS settings from the given config.
// Used by the reverse proxy which needs a Transport rather than an http.Client.
func NewTLSTransport(tc config.TLSConfig) (*http.Transport, error) {
	if !tc.SkipVerify && tc.CACert == "" {
		return nil, nil
	}

	tlsConfig := &tls.Config{}

	if tc.SkipVerify {
		tlsConfig.InsecureSkipVerify = true
	}

	if tc.CACert != "" {
		caCert, err := os.ReadFile(tc.CACert)
		if err != nil {
			return nil, fmt.Errorf("reading CA certificate %s: %w", tc.CACert, err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(caCert) {
			return nil, fmt.Errorf("failed to parse CA certificate %s", tc.CACert)
		}
		tlsConfig.RootCAs = pool
	}

	return &http.Transport{
		TLSClientConfig: tlsConfig,
	}, nil
}
