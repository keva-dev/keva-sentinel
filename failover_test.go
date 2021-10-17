package sentinel

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func Test_reconfigSlaves(t *testing.T) {
	t.Run("timed out without update from client", func(t *testing.T) {
		slaves := map[string]*slaveInstance{}
		var promoted *slaveInstance
		for i := 0; i < 10; i++ {
			mockClient := &MockNoOpClient{}
			addr := fmt.Sprintf("localhost:%d", i)
			sla := &slaveInstance{
				runID:  addr,
				addr:   addr,
				host:   "localhost",
				port:   fmt.Sprintf("%d", i),
				client: mockClient,
			}

			if i == 0 {
				promoted = sla
				mockClient.On("SlaveOf", sla.host, sla.port).Panic("not expect promoted to be configured")
			} else {
				mockClient.On("SlaveOf", sla.host, sla.port).Once().Return(nil)
			}
			slaves[sla.addr] = sla
		}
		m := &masterInstance{
			slaves: slaves,
			sentinelConf: MasterMonitor{
				ReconfigSlaveTimeout: 2 * time.Second,
				ParallelSync:         1,
			},
			promotedSlave: promoted,
		}
		s := &Sentinel{}

		err := s.reconfigSlaves(m)
		assert.ErrorIs(t, err, context.DeadlineExceeded)
	})
	t.Run("randomly fail configuration can recover", func(t *testing.T) {
		slaves := map[string]*slaveInstance{}
		var promoted *slaveInstance
		for i := 0; i < 5; i++ {
			mockClient := &MockNoOpClient{}
			addr := fmt.Sprintf("localhost:%d", i)
			sla := &slaveInstance{
				runID:  addr,
				addr:   addr,
				host:   "localhost",
				port:   fmt.Sprintf("%d", i),
				client: mockClient,
			}

			if i == 0 {
				promoted = sla
				mockClient.On("SlaveOf", sla.host, sla.port).Panic("not expect promoted to be configured")
			} else {
				// make some slave fail to communicate at first
				if i%2 == 0 {
					mockClient.On("SlaveOf", sla.host, sla.port).Once().Return(errors.New("dummy error"))
				}
				mockClient.On("SlaveOf", sla.host, sla.port).Once().Run(func(mock.Arguments) {
					go func() {
						sla.mu.Lock()
						sla.reconfigFlag |= reconfigDone
						sla.mu.Unlock()
					}()
				}).Return(nil)
			}
			slaves[sla.addr] = sla
		}
		m := &masterInstance{
			slaves: slaves,
			sentinelConf: MasterMonitor{
				ReconfigSlaveTimeout: 10 * time.Second,
				ParallelSync:         1,
			},
			promotedSlave: promoted,
		}
		s := &Sentinel{}

		beginAt := time.Now()
		err := s.reconfigSlaves(m)
		assert.NoError(t, err)
		since := time.Since(beginAt)
		if since > m.sentinelConf.ReconfigSlaveTimeout {
			assert.Failf(t, "expect timeout not reached", "all slave correctly configured in time should not makes this function timeout")
		}
	})
	t.Run("permanently failing slaves do not block reconfiguration", func(t *testing.T) {
		slaves := map[string]*slaveInstance{}
		var promoted *slaveInstance
		for i := 0; i < 5; i++ {
			mockClient := &MockNoOpClient{}
			addr := fmt.Sprintf("localhost:%d", i)
			sla := &slaveInstance{
				runID:  addr,
				addr:   addr,
				host:   "localhost",
				port:   fmt.Sprintf("%d", i),
				client: mockClient,
			}

			if i == 0 {
				promoted = sla
				mockClient.On("SlaveOf", sla.host, sla.port).Panic("not expect promoted to be configured")
			} else {
				// some slaves are permanently failing
				if i%2 == 0 {
					mockClient.On("SlaveOf", sla.host, sla.port).Return(errors.New("dummy err"))
				} else {
					mockClient.On("SlaveOf", sla.host, sla.port).Once().Run(func(mock.Arguments) {
						go func() {
							sla.mu.Lock()
							sla.reconfigFlag |= reconfigDone
							sla.mu.Unlock()
						}()
					}).Return(nil)

				}
			}
			slaves[sla.addr] = sla
		}
		m := &masterInstance{
			slaves: slaves,
			sentinelConf: MasterMonitor{
				ReconfigSlaveTimeout: 10 * time.Second,
				ParallelSync:         1,
			},
			promotedSlave: promoted,
		}
		s := &Sentinel{}

		err := s.reconfigSlaves(m)
		assert.ErrorIs(t, err, context.DeadlineExceeded)
	})

	t.Run("run successfully with all slave reconfigured", func(t *testing.T) {
		slaves := map[string]*slaveInstance{}
		var promoted *slaveInstance
		for i := 0; i < 5; i++ {
			mockClient := &MockNoOpClient{}
			addr := fmt.Sprintf("localhost:%d", i)
			sla := &slaveInstance{
				runID:  addr,
				addr:   addr,
				host:   "localhost",
				port:   fmt.Sprintf("%d", i),
				client: mockClient,
			}

			if i == 0 {
				promoted = sla
				mockClient.On("SlaveOf", sla.host, sla.port).Panic("not expect promoted to be configured")
			} else {
				mockClient.On("SlaveOf", sla.host, sla.port).Once().Run(func(mock.Arguments) {
					go func() {
						sla.mu.Lock()
						sla.reconfigFlag |= reconfigDone
						sla.mu.Unlock()
					}()
				}).Return(nil)
			}
			slaves[sla.addr] = sla
		}
		m := &masterInstance{
			slaves: slaves,
			sentinelConf: MasterMonitor{
				ReconfigSlaveTimeout: 10 * time.Second,
				ParallelSync:         1,
			},
			promotedSlave: promoted,
		}
		s := &Sentinel{}

		beginAt := time.Now()
		err := s.reconfigSlaves(m)
		assert.NoError(t, err)
		since := time.Since(beginAt)
		if since > m.sentinelConf.ReconfigSlaveTimeout {
			assert.Failf(t, "expect timeout not reached", "all slave correctly configured in time should not makes this function timeout")
		}
	})

}
