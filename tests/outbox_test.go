package tests

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/n0f4ph4mst3r/outbox-relay/internal/outbox"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
)

func mockUUID(id int) string {
	return fmt.Sprintf("00000000-0000-0000-0000-%012d", id)
}

func setupDB(t *testing.T, ctx context.Context) *pgxpool.Pool {
	dbPool, err := pgxpool.New(ctx, "postgres://postgres:qwerty@localhost:5432/test?sslmode=disable")
	require.NoError(t, err, "Failed to connect to database - check credentials or port mapping")

	err = dbPool.Ping(ctx)
	require.NoError(t, err, "Database ping failed - check credentials or port mapping")

	_, err = dbPool.Exec(ctx, "TRUNCATE TABLE outbox_events")
	require.NoError(t, err, "Failed to truncate outbox table")

	return dbPool
}

func setupProducer(t *testing.T) *kgo.Client {
	cl, err := kgo.NewClient(
		kgo.SeedBrokers("localhost:9093"),
		kgo.AllowAutoTopicCreation(),
		kgo.DialTimeout(5*time.Second),
	)
	require.NoError(t, err, "Failed to create Kafka producer")
	return cl
}

func setupConsumer(t *testing.T, topic string) *kgo.Client {
	cl, err := kgo.NewClient(
		kgo.SeedBrokers("localhost:9093"),
		kgo.DialTimeout(5*time.Second),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	require.NoError(t, err, "Failed to create Kafka consumer")
	return cl
}

func TestBaseFlow(t *testing.T) {
	ctx := context.Background()

	dbPool := setupDB(t, ctx)
	producer := setupProducer(t)
	defer producer.Close()

	if err := producer.Ping(ctx); err != nil {
		t.Fatalf("Kafka producer cannot ping broker: %v", err)
	}

	const topic = "test-topic"
	consumer := setupConsumer(t, topic)
	defer consumer.Close()

	eventID := mockUUID(1)

	_, err := dbPool.Exec(ctx,
		"INSERT INTO outbox_events (id, event_type, payload, created_at) VALUES ($1, $2, $3, $4)",
		eventID, topic, []byte("{}"), time.Now(),
	)
	require.NoError(t, err)

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	w := outbox.NewWorker(logger, dbPool, producer)
	w.ProcessBatch(ctx, 10)

	require.Eventually(t, func() bool {
		fetches := consumer.PollFetches(ctx)
		if fetches.Errors() != nil {
			return false
		}

		iter := fetches.RecordIter()
		for !iter.Done() {
			record := iter.Next()
			return string(record.Value) == "{}"
		}
		return false
	}, 5*time.Second, 100*time.Millisecond, "Event was not received by consumer from Kafka")

	var processedAt *time.Time
	require.Eventually(t, func() bool {
		err := dbPool.QueryRow(ctx, "SELECT processed_at FROM outbox_events WHERE id = $1", eventID).Scan(&processedAt)
		return err == nil && processedAt != nil
	}, 5*time.Second, 100*time.Millisecond, "Event was not marked as processed in DB")

	require.NotNil(t, processedAt)
}

func TestCleanup(t *testing.T) {
	ctx := context.Background()

	dbPool := setupDB(t, ctx)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	w := outbox.NewWorker(logger, dbPool, nil)

	oldTime := time.Now().Add(-30 * time.Hour)
	eventID := mockUUID(2)

	_, err := dbPool.Exec(ctx,
		"INSERT INTO outbox_events (id, event_type, payload, created_at, processed_at) VALUES ($1, $2, $3, $4, $5)",
		eventID, "Old", []byte("{}"), oldTime.Add(-1*time.Hour), oldTime,
	)
	require.NoError(t, err)

	err = w.Cleanup(ctx, 24*time.Hour)
	require.NoError(t, err)

	var count int
	dbPool.QueryRow(ctx, "SELECT count(*) FROM outbox_events").Scan(&count)
	require.Equal(t, 0, count, "Old event should have been deleted")
}

func TestConcurrentProcessing(t *testing.T) {
	ctx := context.Background()

	dbPool := setupDB(t, ctx)
	producer := setupProducer(t)
	defer producer.Close()

	const topic = "concurrent-topic"
	const totalEvents = 50
	const batchSize = 10
	const concurrentRoutines = 5

	baseTime := time.Now()
	for i := 0; i < totalEvents; i++ {
		id := mockUUID(1000 + i)
		_, err := dbPool.Exec(ctx,
			"INSERT INTO outbox_events (id, event_type, payload, created_at) VALUES ($1, $2, $3, $4)",
			id, topic, []byte("{}"), baseTime.Add(time.Duration(i)*time.Millisecond),
		)
		require.NoError(t, err)
	}

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	w := outbox.NewWorker(logger, dbPool, producer)

	var wg sync.WaitGroup
	for i := 0; i < concurrentRoutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			for j := 0; j < (totalEvents / batchSize); j++ {
				w.ProcessBatch(ctx, int64(batchSize))
			}
		}()
	}

	wg.Wait()

	var processedCount int
	err := dbPool.QueryRow(ctx, "SELECT count(*) FROM outbox_events WHERE processed_at IS NOT NULL").Scan(&processedCount)
	require.NoError(t, err)

	require.Equal(t, totalEvents, processedCount, "All events must be marked as processed exactly once without race conditions")
}

func TestKafkaFailureRollback(t *testing.T) {
	ctx := context.Background()

	dbPool := setupDB(t, ctx)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	brokenClient, err := kgo.NewClient(
		kgo.SeedBrokers("localhost:12345"),
		kgo.DialTimeout(100*time.Millisecond),
	)
	require.NoError(t, err, "Failed to create Kafka client")
	defer brokenClient.Close()

	w := outbox.NewWorker(logger, dbPool, brokenClient)

	eventID := mockUUID(3)

	_, err = dbPool.Exec(ctx,
		"INSERT INTO outbox_events (id, event_type, payload, created_at) VALUES ($1, $2, $3, $4)",
		eventID, "Test", []byte("{}"), time.Now(),
	)
	require.NoError(t, err)

	w.ProcessBatch(ctx, 10)

	var processedAt *time.Time
	err = dbPool.QueryRow(ctx, "SELECT processed_at FROM outbox_events WHERE id = $1", eventID).Scan(&processedAt)
	require.NoError(t, err)
	require.Nil(t, processedAt, "Event should NOT be processed because Kafka delivery failed")
}

func TestBatchSizeLimiting(t *testing.T) {
	ctx := context.Background()

	dbPool := setupDB(t, ctx)
	producer := setupProducer(t)
	defer producer.Close()

	const topic = "batch-limit-topic"
	const totalEvents = 10
	const batchSize = 4

	baseTime := time.Now()
	for i := 0; i < totalEvents; i++ {
		id := mockUUID(200 + i)
		_, err := dbPool.Exec(ctx,
			"INSERT INTO outbox_events (id, event_type, payload, created_at) VALUES ($1, $2, $3, $4)",
			id, topic, []byte("{}"), baseTime.Add(time.Duration(i)*time.Second),
		)
		require.NoError(t, err)
	}

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	w := outbox.NewWorker(logger, dbPool, producer)

	w.ProcessBatch(ctx, int64(batchSize))

	var processedCount int
	err := dbPool.QueryRow(ctx, "SELECT count(*) FROM outbox_events WHERE processed_at IS NOT NULL").Scan(&processedCount)
	require.NoError(t, err)
	require.Equal(t, batchSize, processedCount, "Worker must process exactly batchSize records")
}

func TestFIFOOrdering(t *testing.T) {
	ctx := context.Background()

	dbPool := setupDB(t, ctx)
	producer := setupProducer(t)
	defer producer.Close()

	const topic = "fifo-topic"
	const count = 5

	baseTime := time.Now()
	for i := 0; i < count; i++ {
		payload, _ := json.Marshal(map[string]int{"seq": i})
		id := mockUUID(300 + i)

		_, err := dbPool.Exec(ctx,
			"INSERT INTO outbox_events (id, event_type, payload, created_at) VALUES ($1, $2, $3, $4)",
			id, topic, payload, baseTime.Add(time.Duration(i)*time.Second),
		)
		require.NoError(t, err)
	}

	consumer := setupConsumer(t, topic)
	defer consumer.Close()

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	w := outbox.NewWorker(logger, dbPool, producer)
	w.ProcessBatch(ctx, int64(count))

	fetchCtx, fetchCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer fetchCancel()

	expectedSeq := 0
	for expectedSeq < count {
		fetches := consumer.PollFetches(fetchCtx)
		if fetches.IsClientClosed() || fetchCtx.Err() != nil {
			break
		}

		fetches.EachRecord(func(record *kgo.Record) {
			var msg map[string]int
			err := json.Unmarshal(record.Value, &msg)
			require.NoError(t, err)

			require.Equal(t, expectedSeq, msg["seq"], "FIFO order broken at sequence index")
			expectedSeq++
		})
	}
	require.Equal(t, count, expectedSeq, "Not all messages were fetched in correct order")
}

func TestEmptyTable(t *testing.T) {
	ctx := context.Background()

	dbPool := setupDB(t, ctx)
	producer := setupProducer(t)
	defer producer.Close()

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	w := outbox.NewWorker(logger, dbPool, producer)

	require.NotPanics(t, func() {
		w.ProcessBatch(ctx, 10)
	})

	var count int
	err := dbPool.QueryRow(ctx, "SELECT count(*) FROM outbox_events").Scan(&count)
	require.NoError(t, err)
	require.Equal(t, 0, count)
}

func TestContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	dbPool := setupDB(t, ctx)
	producer := setupProducer(t)
	defer producer.Close()

	eventID := mockUUID(4)

	_, err := dbPool.Exec(ctx,
		"INSERT INTO outbox_events (id, event_type, payload, created_at) VALUES ($1, $2, $3, $4)",
		eventID, "cancel-topic", []byte("{}"), time.Now(),
	)
	require.NoError(t, err)

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	w := outbox.NewWorker(logger, dbPool, producer)

	cancel()

	w.ProcessBatch(ctx, 10)

	var processedAt *time.Time
	err = dbPool.QueryRow(context.Background(), "SELECT processed_at FROM outbox_events WHERE id = $1", eventID).Scan(&processedAt)
	require.NoError(t, err)
	require.Nil(t, processedAt, "Event should not be marked as processed when context is canceled")
}
