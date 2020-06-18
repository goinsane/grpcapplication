package grpcapplication

import (
	"context"
	"errors"
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
	"github.com/grpc-ecosystem/go-grpc-prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
	"google.golang.org/grpc"
)

type GrpcApplication struct {
	App           application.Application
	Logger        *xlog.Logger
	HTTPServer    *http.Server
	RegisterFunc  RegisterFunc
	Listeners     []net.Listener
	HandleMetrics bool
	HandleDebug   bool

	started      int32
	grpcServer   *grpc.Server
	httpServeMux *http.ServeMux
	connCount    int64
}

type RegisterFunc func(grpcServer *grpc.Server, httpServeMux *http.ServeMux)

func (a *GrpcApplication) httpHandler(w http.ResponseWriter, r *http.Request) {
	atomic.AddInt64(&a.connCount, 1)
	defer atomic.AddInt64(&a.connCount, -1)
	if r.ProtoMajor == 2 && strings.Contains(r.Header.Get("Content-Type"), "application/grpc") {
		a.grpcServer.ServeHTTP(w, r)
	} else {
		a.httpServeMux.ServeHTTP(w, r)
	}
}

func (a *GrpcApplication) Start() {
	if !atomic.CompareAndSwapInt32(&a.started, 0, 1) {
		panic("already started")
	}

	if a.HTTPServer == nil {
		a.HTTPServer = &http.Server{
			ErrorLog: log.New(ioutil.Discard, "", log.LstdFlags),
		}
	}
	http2Server := &http2.Server{}
	a.HTTPServer.Handler = h2c.NewHandler(http.HandlerFunc(a.httpHandler), http2Server)

	a.grpcServer = grpc.NewServer(
		grpc.StreamInterceptor(grpc_prometheus.StreamServerInterceptor),
		grpc.UnaryInterceptor(grpc_prometheus.UnaryServerInterceptor))
	a.httpServeMux = http.NewServeMux()

	if a.RegisterFunc != nil {
		a.RegisterFunc(a.grpcServer, a.httpServeMux)
	}

	if a.HandleMetrics {
		a.httpServeMux.Handle("/metrics", promhttp.Handler())
	}
	if a.HandleDebug {
		a.httpServeMux.HandleFunc("/debug/pprof/", pprof.Index)
		a.httpServeMux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
		a.httpServeMux.HandleFunc("/debug/pprof/profile", pprof.Profile)
		a.httpServeMux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
		a.httpServeMux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	}

	if a.App != nil {
		a.App.Start()
	}
}

func (a *GrpcApplication) Run(ctx application.Context) {
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
			if err := a.HTTPServer.Serve(lis); err != nil {
				if !errors.Is(err, http.ErrServerClosed) {
					a.Logger.Errorf("http serve error: %q: %v", lis.Addr().String(), err)
				}
				ctx.Terminate()
			}
		}()
	}

	wg.Wait()
}

func (a *GrpcApplication) Terminate(ctx context.Context) {
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
			a.HTTPServer.Close()
			a.Logger.Warningf("closed active http connections: %v", err)
		}

		for {
			if a.connCount <= 0 {
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

func (a *GrpcApplication) Stop() {
	// first implement GrpcApplication codes
	if a.App != nil {
		a.App.Stop()
	}
}
