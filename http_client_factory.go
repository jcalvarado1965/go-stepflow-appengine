package stepflow_ae

import (
	"context"
	"net/http"

	"google.golang.org/appengine/urlfetch"

	stepflow "github.com/jcalvarado1965/go-stepflow"
)

type httpClientFactory struct{}

// NewHTTPClientFactory creates an http client factory
func NewHTTPClientFactory() stepflow.HTTPClientFactory {
	return &httpClientFactory{}
}

func (h *httpClientFactory) GetHTTPClient(ctx context.Context, disableSSLValidation bool) *http.Client {

	return &http.Client{
		Transport: &urlfetch.Transport{
			Context: ctx,
			AllowInvalidServerCertificate: disableSSLValidation,
		},
	}
}
