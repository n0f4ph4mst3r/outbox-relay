package outbox

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/n0f4ph4mst3r/outbox-relay/internal/sl"
	"github.com/twmb/franz-go/pkg/kgo"
)

type Worker struct {
	log       *slog.Logger
	db        *pgxpool.Pool
	publisher *kgo.Client
}

type event struct {
	id      string
	topic   string
	payload []byte
}

func NewWorker(log *slog.Logger, db *pgxpool.Pool, publisher *kgo.Client) *Worker {
	return &Worker{db: db, log: log, publisher: publisher}
}

func (w *Worker) Start(ctx context.Context, eventTTL time.Duration, limit int64) {
	processTicker := time.NewTicker(2 * time.Second)
	cleanupTicker := time.NewTicker(1 * time.Hour)
	defer processTicker.Stop()
	defer cleanupTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			w.log.Info("outbox worker stopped")
			return
		case <-processTicker.C:
			w.ProcessBatch(ctx, limit)
		case <-cleanupTicker.C:
			w.Cleanup(ctx, eventTTL)
		}
	}
}

func (w *Worker) ProcessBatch(ctx context.Context, limit int64) {
	const op = "outbox.worker.process"

	log := w.log.With(
		slog.String("op", op),
	)

	tx, err := w.db.Begin(ctx)
	if err != nil {
		log.Error("Failed to begin tx for outbox", sl.Err(err))
		return
	}
	defer tx.Rollback(ctx)

	const fetchQuery = `
		SELECT id, event_type, payload 
		FROM outbox_events 
		WHERE processed_at IS NULL 
		ORDER BY created_at ASC 
		LIMIT $1
		FOR UPDATE SKIP LOCKED
	`

	rows, err := tx.Query(ctx, fetchQuery, limit)
	if err != nil {
		if !errors.Is(err, pgx.ErrNoRows) {
			log.Error("Failed to fetch outbox event", sl.Err(err))
		}

		return
	}
	defer rows.Close()

	var events []event
	var ids []string
	for rows.Next() {
		var e event
		if err := rows.Scan(&e.id, &e.topic, &e.payload); err != nil {
			if !errors.Is(err, pgx.ErrNoRows) {
				log.Error("Failed to scan outbox event", sl.Err(err))
			}
			return
		}

		events = append(events, e)
		ids = append(ids, e.id)
	}

	if len(events) == 0 {
		return
	}

	records := make([]*kgo.Record, len(events))
	for i, e := range events {
		records[i] = &kgo.Record{Topic: e.topic, Value: e.payload}
		log.Debug("Preparing record for Kafka", "topic", e.topic)
	}

	produceCtx, produceCancel := context.WithTimeout(ctx, 2*time.Second)
	defer produceCancel()
	if err := w.publisher.ProduceSync(produceCtx, records...).FirstErr(); err != nil {
		log.Error("Kafka batch produce error - aborting transaction", "err", err)
		return
	}

	const updateQuery = `
        UPDATE outbox_events 
        SET processed_at = now() 
        WHERE id = ANY($1)`

	_, err = tx.Exec(ctx, updateQuery, ids)
	if err != nil {
		log.Error("Failed to mark batch as processed", "err", err)
		return
	}

	if err := tx.Commit(ctx); err != nil {
		log.Error("Failed to commit transaction", sl.Err(err))
		return
	}

	log.Info("Batch processed", "count", len(events))
}

func (w *Worker) Cleanup(ctx context.Context, ttl time.Duration) error {
	const op = "outbox.worker.cleanup"

	const query = `
        DELETE FROM outbox_events 
		WHERE processed_at IS NOT NULL 
		AND processed_at < now() - $1::interval
    `

	res, err := w.db.Exec(ctx, query, ttl)
	if err != nil {
		w.log.Error("Failed to cleanup outbox", "err", err)
		return err
	}

	w.log.Info("Outbox cleanup complete", "deleted_rows", res.RowsAffected())
	return nil
}
