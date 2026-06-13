package jobs

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/onix-fun/media-service/internal/config"
	amqp "github.com/rabbitmq/amqp091-go"
)

type Job struct {
	ID        string `json:"id"`
	Type      string `json:"type"`
	SessionID string `json:"session_id,omitempty"`
	BlobID    string `json:"blob_id,omitempty"`
	Profile   string `json:"profile,omitempty"`
}
type Handler interface {
	HandleJob(context.Context, Job) error
}
type Rabbit struct {
	cfg config.RabbitMQ
	log *slog.Logger
}

func New(cfg config.RabbitMQ, log *slog.Logger) *Rabbit { return &Rabbit{cfg: cfg, log: log} }
func (r *Rabbit) PublishHash(ctx context.Context, sessionID uuid.UUID) error {
	return r.publish(ctx, Job{ID: uuid.Must(uuid.NewV7()).String(), Type: "hash", SessionID: sessionID.String()})
}
func (r *Rabbit) PublishProcess(ctx context.Context, blobID uuid.UUID, profile string) error {
	return r.publish(ctx, Job{ID: uuid.Must(uuid.NewV7()).String(), Type: "process", BlobID: blobID.String(), Profile: profile})
}
func (r *Rabbit) publish(ctx context.Context, job Job) error {
	conn, err := amqp.Dial(r.cfg.URL)
	if err != nil {
		return err
	}
	defer conn.Close()
	ch, err := conn.Channel()
	if err != nil {
		return err
	}
	defer ch.Close()
	if err = r.declare(ch); err != nil {
		return err
	}
	body, _ := json.Marshal(job)
	return ch.PublishWithContext(ctx, r.cfg.JobsExchange, r.cfg.JobsRoutingKey, false, false, amqp.Publishing{ContentType: "application/json", DeliveryMode: amqp.Persistent, MessageId: job.ID, Timestamp: time.Now(), Body: body})
}
func (r *Rabbit) Run(ctx context.Context, handler Handler) error {
	for ctx.Err() == nil {
		if err := r.consume(ctx, handler); err != nil {
			r.log.Error("job consumer stopped", "error", err)
			select {
			case <-ctx.Done():
				return nil
			case <-time.After(5 * time.Second):
			}
		}
	}
	return nil
}
func (r *Rabbit) consume(ctx context.Context, handler Handler) error {
	conn, err := amqp.Dial(r.cfg.URL)
	if err != nil {
		return err
	}
	defer conn.Close()
	ch, err := conn.Channel()
	if err != nil {
		return err
	}
	defer ch.Close()
	if err = r.declare(ch); err != nil {
		return err
	}
	if err = ch.Qos(r.cfg.Prefetch, 0, false); err != nil {
		return err
	}
	ds, err := ch.Consume(r.cfg.JobsQueue, "", false, false, false, false, nil)
	if err != nil {
		return err
	}
	for {
		select {
		case <-ctx.Done():
			return nil
		case d, ok := <-ds:
			if !ok {
				return fmt.Errorf("job channel closed")
			}
			var job Job
			if json.Unmarshal(d.Body, &job) != nil || job.ID == "" {
				_ = d.Nack(false, false)
				continue
			}
			if err := handler.HandleJob(ctx, job); err != nil {
				r.log.Error("job failed", "id", job.ID, "error", err)
				attempts := retryCount(d) + 1
				if attempts >= r.cfg.MaxRetries {
					_ = d.Nack(false, false)
					continue
				}
				_ = ch.PublishWithContext(ctx, r.cfg.JobsExchange, r.cfg.JobsRoutingKey, false, false, amqp.Publishing{ContentType: "application/json", DeliveryMode: amqp.Persistent, Headers: amqp.Table{"x-retry-count": attempts}, Body: d.Body})
				_ = d.Ack(false)
				continue
			}
			event, _ := json.Marshal(map[string]any{"event_id": uuid.Must(uuid.NewV7()).String(), "type": "media.job.completed", "job": job, "occurred_at": time.Now().UTC().Format(time.RFC3339Nano)})
			_ = ch.PublishWithContext(ctx, r.cfg.EventsExchange, "", false, false, amqp.Publishing{ContentType: "application/json", DeliveryMode: amqp.Persistent, Body: event})
			_ = d.Ack(false)
		}
	}
}

func retryCount(delivery amqp.Delivery) int64 {
	switch value := delivery.Headers["x-retry-count"].(type) {
	case int64:
		return value
	case int32:
		return int64(value)
	}
	return 0
}
func (r *Rabbit) declare(ch *amqp.Channel) error {
	if err := ch.ExchangeDeclare(r.cfg.JobsExchange, "direct", true, false, false, false, nil); err != nil {
		return err
	}
	if err := ch.ExchangeDeclare(r.cfg.EventsExchange, "fanout", true, false, false, false, nil); err != nil {
		return err
	}
	if err := ch.ExchangeDeclare(r.cfg.DLQExchange, "direct", true, false, false, false, nil); err != nil {
		return err
	}
	if _, err := ch.QueueDeclare(r.cfg.DLQQueue, true, false, false, false, nil); err != nil {
		return err
	}
	if err := ch.QueueBind(r.cfg.DLQQueue, r.cfg.DLQQueue, r.cfg.DLQExchange, false, nil); err != nil {
		return err
	}
	args := amqp.Table{"x-dead-letter-exchange": r.cfg.DLQExchange, "x-dead-letter-routing-key": r.cfg.DLQQueue}
	if _, err := ch.QueueDeclare(r.cfg.JobsQueue, true, false, false, false, args); err != nil {
		return err
	}
	return ch.QueueBind(r.cfg.JobsQueue, r.cfg.JobsRoutingKey, r.cfg.JobsExchange, false, nil)
}
