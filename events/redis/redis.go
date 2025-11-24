package redis

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/bitcoin-sv/arcade/models"
	"github.com/redis/go-redis/v9"
)

const (
	statusUpdateChannel = "arcade:status_updates"
)

// RedisPublisher implements event publishing via Redis Pub/Sub
type RedisPublisher struct {
	client *redis.Client
	pubsub *redis.PubSub
}

// NewRedisPublisher creates a new Redis-based event publisher
func NewRedisPublisher(redisURL string) (*RedisPublisher, error) {
	opts, err := redis.ParseURL(redisURL)
	if err != nil {
		return nil, fmt.Errorf("failed to parse Redis URL: %w", err)
	}

	client := redis.NewClient(opts)

	if err := client.Ping(context.Background()).Err(); err != nil {
		return nil, fmt.Errorf("failed to connect to Redis: %w", err)
	}

	return &RedisPublisher{
		client: client,
	}, nil
}

// Publish publishes a status update event to Redis
func (r *RedisPublisher) Publish(ctx context.Context, update models.StatusUpdate) error {
	data, err := json.Marshal(update)
	if err != nil {
		return fmt.Errorf("failed to marshal status update: %w", err)
	}

	if err := r.client.Publish(ctx, statusUpdateChannel, data).Err(); err != nil {
		return fmt.Errorf("failed to publish to Redis: %w", err)
	}

	return nil
}

// Subscribe subscribes to status update events from Redis
func (r *RedisPublisher) Subscribe(ctx context.Context) (<-chan models.StatusUpdate, error) {
	r.pubsub = r.client.Subscribe(ctx, statusUpdateChannel)

	ch := make(chan models.StatusUpdate, 100)

	go func() {
		defer close(ch)

		for {
			select {
			case <-ctx.Done():
				return
			case msg, ok := <-r.pubsub.Channel():
				if !ok {
					return
				}

				var update models.StatusUpdate
				if err := json.Unmarshal([]byte(msg.Payload), &update); err != nil {
					continue
				}

				select {
				case ch <- update:
				case <-ctx.Done():
					return
				}
			}
		}
	}()

	return ch, nil
}

// Close closes the Redis connection
func (r *RedisPublisher) Close() error {
	if r.pubsub != nil {
		r.pubsub.Close()
	}
	return r.client.Close()
}
