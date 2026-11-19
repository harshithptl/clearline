// Command healthcheck GETs a URL and exits 0 on a 2xx response, 1 otherwise.
// The service images are distroless (no shell, no curl), so compose
// healthchecks run this instead:
//
//	healthcheck http://127.0.0.1:8080/healthz
package main

import (
	"fmt"
	"net/http"
	"os"
	"time"
)

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintln(os.Stderr, "usage: healthcheck <url>")
		os.Exit(2)
	}
	cl := &http.Client{Timeout: 2 * time.Second}
	resp, err := cl.Get(os.Args[1]) //nolint:gosec // G107: the URL is the operator-supplied healthcheck target
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	_ = resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		fmt.Fprintln(os.Stderr, resp.Status)
		os.Exit(1)
	}
}
