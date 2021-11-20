package sentinel

import (
	"context"
	"fmt"

	"github.com/go-redis/redis/v8"
	"github.com/keva-dev/go-sentinel/tools"
)

func newRedisClient(addr string) *redisClient {
	cl := redis.NewClient(&redis.Options{
		Addr: addr,
	})
	return &redisClient{
		cl: cl,
	}
}
func (c *redisClient) Info() (string, error) {
	str := c.cl.Info(context.Background(), "replication")
	err := str.Err()
	if err != nil {
		return "", err
	}
	return str.Result()
}
func (c *redisClient) Ping() (string, error) {
	sttcmd := c.cl.Ping(context.Background())
	if sttcmd.Err() != nil {
		return "", sttcmd.Err()
	}
	return sttcmd.Result()
}
func (c *redisClient) SubscribeHelloChan() HelloChan {
	ps := c.cl.Subscribe(context.Background(), helloChan)
	return pubsubHelloChan{
		internal: ps,
		cl:       c.cl,
	}
}
func (c *redisClient) SlaveOfNoOne() error {
	return c.SlaveOf("NO", "ONE")
}

func (c *redisClient) SlaveOf(host, port string) error {
	sttcmd := c.cl.SlaveOf(context.Background(), host, port)
	if sttcmd.Err() != nil {
		return sttcmd.Err()
	}
	_, err := sttcmd.Result()
	return err
}

// SlaveOfNoOne() error
// SlaveOf(host, port string) error

type pubsubHelloChan struct {
	internal *redis.PubSub
	cl       *redis.Client
}

func (c pubsubHelloChan) Close() error {
	return c.internal.Close()
}

func (c pubsubHelloChan) Publish(str string) error {
	intcmd := c.cl.Publish(context.Background(), helloChan, str)
	if intcmd.Err() != nil {
		return intcmd.Err()
	}
	return nil
}

func (c pubsubHelloChan) Receive() (string, error) {
	msg, ok := <-c.internal.Channel()
	if !ok {
		return "", fmt.Errorf("pubsub closed")
	}
	return msg.Payload, nil
}

type redisClient struct {
	cl *redis.Client
}

// Close() error
// Publish(string) error
// Receive() (string, error)

var (
	helloChan = "__sentinel__:hello"
)

type InternalClient interface {
	Info() (string, error)
	Ping() (string, error)
	SubscribeHelloChan() HelloChan
	SlaveOfNoOne() error
	SlaveOf(host, port string) error
}

type HelloChan interface {
	Close() error
	Publish(string) error
	Receive() (string, error)
}
type NoOpClient struct{}

func (c *NoOpClient) Ping() (string, error) { return "", nil }

func (c *NoOpClient) Info() (string, error) {
	return "", nil
}

func (c *NoOpClient) SlaveOf(addr, port string) error {
	return nil
}
func (c *NoOpClient) SlaveOfNoOne() error {
	return nil
}

func (c *NoOpClient) SubscribeHelloChan() HelloChan {
	return nil
}

func GenInterface() {
	tools.MockInterfaces("sentinel", ".", map[string]interface{}{
		"internal_client": &NoOpClient{},
	})
}
