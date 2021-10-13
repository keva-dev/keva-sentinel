package sentinel

import (
	"github.com/keva-dev/go-sentinel/tools"
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
