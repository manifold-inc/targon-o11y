package main

import (
	"context"
	"errors"
	"log"
	"math/rand/v2"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	o11y "github.com/manifold-inc/targon-o11y"
)

var (
	version   = "dev"
	gitHash   = ""
	buildDate = ""
)

func main() {
	srv, err := o11y.Init(o11y.Config{
		Service:       "example",
		Addr:          ":9090",
		CheckInterval: o11y.DefaultCheckInterval,
		CheckTimeout:  o11y.DefaultCheckTimeout,
		AuthToken:     os.Getenv("ADMIN_TOKEN"),
		Version: func() o11y.Version {
			return o11y.Version{Version: version, Commit: gitHash, BuildTime: buildDate}
		},
		Dependencies: []o11y.Dependency{
			{
				Name:        "database",
				Kind:        o11y.KindDatastore,
				Criticality: o11y.Critical,
				Check:       func(ctx context.Context) error { return nil },
			},
			{
				Name:        "inventory-api",
				Kind:        o11y.KindService,
				Criticality: o11y.Soft,
				Check:       func(ctx context.Context) error { return errors.New("connection refused") },
			},
			{
				Name:        "email-provider",
				Kind:        o11y.KindExternal,
				Criticality: o11y.Soft,
			},
		},
	})
	if err != nil {
		log.Fatal(err)
	}

	orders := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "example_orders",
		Help: "Orders by state.",
	}, []string{"state"})
	ordersProcessed := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "example_orders_processed_total",
		Help: "Processed orders by result.",
	}, []string{"result"})
	orderDuration := prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "example_order_processing_duration_seconds",
		Help:    "Time taken to process an order.",
		Buckets: prometheus.ExponentialBuckets(0.05, 2, 8),
	})
	srv.Registry().MustRegister(orders, ordersProcessed, orderDuration)

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()
	if err := srv.Start(ctx); err != nil {
		log.Fatal(err)
	}
	log.Printf("admin listening on %s", srv.Addr())

	go func() {
		t := time.NewTicker(time.Second)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				orders.WithLabelValues("pending").Set(float64(rand.IntN(20)))
				orders.WithLabelValues("shipped").Set(float64(rand.IntN(100)))
				result := "ok"
				if rand.IntN(10) == 0 {
					result = "error"
				}
				ordersProcessed.WithLabelValues(result).Inc()
				orderDuration.Observe(0.05 + rand.Float64()*2)
			}
		}
	}()

	<-ctx.Done()

	srv.SetDraining()
	time.Sleep(2 * time.Second)
	// the service's public server.Shutdown(ctx) goes here

	shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelShutdown()
	_ = srv.Shutdown(shutdownCtx)
}
