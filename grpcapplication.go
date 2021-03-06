// Package grpcapplication offers an Application implementation of github.com/goinsane/application for GRPC applications.
package grpcapplication

import (
	"context"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"net/http/pprof"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/goinsane/application"
	"github.com/goinsane/xlog"
	grpc_prometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
	"google.golang.org/grpc"
)

// GRPCApplication is an implementation of Application optimized by GRPC applications.
type GRPCApplication struct {
	// App is an Application instance for wrapping notification methods if needed.
	App application.Application

	// Logger logs error and warning logs if needed.
	Logger *xlog.Logger

	// HTTPServer for using custom HTTP server if needed.
	HTTPServer *http.Server

	// GRPCServerOptions for additional server options if needed.
	GRPCServerOptions []grpc.ServerOption

	// If HandleMetrics is true, the GRPCApplication serves /metrics end-point for prometheus metrics .
	HandleMetrics bool

	// If HandlePprof is true, the GRPCApplication serves /debug/pprof/ end-point for pprof.
	HandlePprof bool

	// RegisterFunc for registering GRPC services. If it is nil, the GRPCApplication doesn't handle any request.
	RegisterFunc RegisterFunc

	// Listeners for serving requests. If it is nil, the GRPCApplication doesn't serve any connections.
	Listeners []net.Listener

	grpcServer    *grpc.Server
	httpServeMux  *http.ServeMux
	connCount     int64
	httpConnCount int64
	grpcConnCount int64
}

// RegisterFunc is a type of function for using in GRPCApplication.
type RegisterFunc func(ctx application.Context, grpcServer *grpc.Server, httpServeMux *http.ServeMux)

func (a *GRPCApplication) httpHandler(w http.ResponseWriter, r *http.Request) {
	defer func() {
		if e := recover(); e != nil {
			xlog.Errorf("panic on http handler: %v", e)
		}
	}()

	atomic.AddInt64(&a.connCount, 1)
	defer atomic.AddInt64(&a.connCount, -1)

	if r.ProtoMajor == 2 && strings.HasPrefix(r.Header.Get("Content-Type"), "application/grpc") {
		func() {
			atomic.AddInt64(&a.grpcConnCount, 1)
			defer atomic.AddInt64(&a.grpcConnCount, -1)
			a.grpcServer.ServeHTTP(w, r)
		}()
	} else {
		func() {
			atomic.AddInt64(&a.httpConnCount, 1)
			defer atomic.AddInt64(&a.httpConnCount, -1)
			a.httpServeMux.ServeHTTP(w, r)
		}()
	}
}

// Start implements Application.Start(). It initializes HTTP and GRPC servers, calls RegisterFunc.
// And after calls App.Start() if App isn't nil.
func (a *GRPCApplication) Start(ctx application.Context) {
	if a.HTTPServer == nil {
		a.HTTPServer = &http.Server{
			ErrorLog: log.New(ioutil.Discard, "", log.LstdFlags),
		}
	}
	http2Server := &http2.Server{}
	a.HTTPServer.Handler = h2c.NewHandler(http.HandlerFunc(a.httpHandler), http2Server)

	grpcServerOptions := make([]grpc.ServerOption, 0, 128)
	if a.HandleMetrics {
		grpcServerOptions = append(grpcServerOptions, []grpc.ServerOption{
			grpc.StreamInterceptor(grpc_prometheus.StreamServerInterceptor),
			grpc.UnaryInterceptor(grpc_prometheus.UnaryServerInterceptor),
		}...)
	}
	grpcServerOptions = append(grpcServerOptions, a.GRPCServerOptions...)

	a.grpcServer = grpc.NewServer(grpcServerOptions...)
	a.httpServeMux = http.NewServeMux()

	if a.HandleMetrics {
		a.httpServeMux.Handle("/metrics", promhttp.Handler())
	}
	if a.HandlePprof {
		a.httpServeMux.HandleFunc("/debug/pprof/", pprof.Index)
		a.httpServeMux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
		a.httpServeMux.HandleFunc("/debug/pprof/profile", pprof.Profile)
		a.httpServeMux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
		a.httpServeMux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	}

	if a.App != nil {
		a.App.Start(ctx)
	}

	if a.RegisterFunc != nil {
		a.RegisterFunc(ctx, a.grpcServer, a.httpServeMux)
	}
}

// Run implements Application.Run(). It calls Serve methods for given listeners.
// And also it calls App.Run() asynchronously if App isn't nil.
func (a *GRPCApplication) Run(ctx application.Context) {
	var wg sync.WaitGroup

	if a.App != nil {
		wg.Add(1)
		go func() {
			defer wg.Done()
			a.App.Run(ctx)
		}()
	}

	for _, lis := range a.Listeners {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var err error
			if a.HTTPServer.TLSConfig != nil &&
				(len(a.HTTPServer.TLSConfig.Certificates) > 0 || a.HTTPServer.TLSConfig.GetCertificate != nil) {
				err = a.HTTPServer.ServeTLS(lis, "", "")
			} else {
				err = a.HTTPServer.Serve(lis)
			}
			if err != http.ErrServerClosed {
				a.Logger.Errorf("http serve error: %q: %v", lis.Addr().String(), err)
			}
			ctx.Terminate()
		}()
	}

	wg.Wait()
}

// Terminate implements Application.Terminate(). It terminates servers.
// And also it calls App.Terminate() asynchronously if App isn't nil.
func (a *GRPCApplication) Terminate(ctx context.Context) {
	var wg sync.WaitGroup

	if a.App != nil {
		wg.Add(1)
		go func() {
			defer wg.Done()
			a.App.Terminate(ctx)
		}()
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		var err error

		err = a.HTTPServer.Shutdown(ctx)
		if err != nil {
			_ = a.HTTPServer.Close()
			a.Logger.Warningf("closed active http connections: %v", err)
		}

		for {
			if a.grpcConnCount <= 0 {
				err = nil
				break
			}
			err = ctx.Err()
			if err != nil {
				break
			}
			<-time.After(250 * time.Millisecond)
		}
		a.grpcServer.Stop()
		if err != nil {
			a.Logger.Warningf("closed active grpc connections: %v", err)
		}
	}()

	wg.Wait()
}

// Stop implements Application.Stop().
// And also it calls App.Stop() if App isn't nil.
func (a *GRPCApplication) Stop() {
	// you should implement GRPCApplication codes firstly
	if a.App != nil {
		a.App.Stop()
	}
}
