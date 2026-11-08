// Command topics creates Clearline's Kafka topics if they do not exist.
//
//	go run ./cmd/topics
//
// Brokers come from CLEARLINE_KAFKA_BROKERS (comma separated, default
// localhost:9092). Replication factor comes from CLEARLINE_KAFKA_REPLICATION
// (default 1, the single local broker).
package main

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/harshithptl/clearline/internal/topics"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "topics:", err)
		os.Exit(1)
	}
}

func run() error {
	brokers := []string{"localhost:9092"}
	if b := os.Getenv("CLEARLINE_KAFKA_BROKERS"); b != "" {
		brokers = strings.Split(b, ",")
	}
	rf := int16(1)
	if s := os.Getenv("CLEARLINE_KAFKA_REPLICATION"); s != "" {
		v, err := strconv.ParseInt(s, 10, 16)
		if err != nil {
			return fmt.Errorf("CLEARLINE_KAFKA_REPLICATION: %w", err)
		}
		rf = int16(v)
	}

	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		return err
	}
	defer cl.Close()
	adm := kadm.NewClient(cl)

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	if err := topics.Ensure(ctx, adm, rf, topics.All...); err != nil {
		return err
	}

	details, err := adm.ListTopics(ctx)
	if err != nil {
		return err
	}
	for _, s := range topics.All {
		d, ok := details[s.Name]
		if !ok {
			return fmt.Errorf("topic %s missing after create", s.Name)
		}
		fmt.Printf("%-14s partitions=%d\n", s.Name, len(d.Partitions))
	}
	return nil
}
