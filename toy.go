package sentinel

import (
	"bytes"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/google/uuid"
)

type InstancesManager struct {
	currentTerm   int
	currentMaster *ToyKeva
	addrMap       map[string]*ToyKeva
	mu            *sync.Mutex
}

func (m *InstancesManager) killCurrentMaster() {
	m.mu.Lock()
	master := m.currentMaster
	m.mu.Unlock()
	master.kill()
}

func (m *InstancesManager) getInstanceByAddr(addr string) *ToyKeva {
	m.mu.Lock()
	defer m.mu.Unlock()
	instance, ok := m.addrMap[addr]
	if !ok {
		panic(fmt.Sprintf("instance %s does not exist", addr))
	}
	return instance
}

func (m *InstancesManager) getMaster() *ToyKeva {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.currentMaster

}

// ToyKeva simulator used for testing purpose
type ToyKeva struct {
	host string
	port string

	mu         *sync.Mutex
	alive      bool
	diedAt     *time.Time
	role       string
	id         string
	slaves     []*ToyKeva
	subs       map[string]chan string //key by fake sessionid
	*slaveInfo                        // only set if current keva is a slave
	*InstancesManager
}

func (keva *ToyKeva) slaveOf(host, port string) {
	addr := fmt.Sprintf("%s:%s", host, port)
	instance := keva.InstancesManager.getInstanceByAddr(addr)

	keva.mu.Lock()
	keva.role = "slave"
	keva.slaves = nil
	keva.master = instance
	keva.masterHost = host
	keva.masterPort = port
	keva.mu.Unlock()
}

func (keva *ToyKeva) info() string {
	keva.mu.Lock()
	var ret = bytes.Buffer{}
	ret.WriteString(fmt.Sprintf("role:%s\n", keva.role))
	ret.WriteString(fmt.Sprintf("run_id:%s\n", keva.id))
	slaves := keva.slaves
	isSlave := keva.role == "slave"
	keva.mu.Unlock()
	for idx := range slaves {
		sl := keva.slaves[idx]
		sl.mu.Lock()
		state := "online"
		if !sl.alive {
			state = "offline"
		}
		ret.WriteString(fmt.Sprintf("slave%d:ip=%s,port=%s,state=%s,offset=%d,lag=%d\n",
			idx, sl.host, sl.port, state, sl.offset, sl.lag,
		))
		sl.mu.Unlock()
	}
	var (
		masterDead bool
		diedSince  int
	)
	if isSlave {
		locked(keva.master.mu, func() {
			masterDead = !keva.master.alive
			if masterDead {
				diedSince = int(time.Since(*keva.master.diedAt).Seconds())
			}
		})

	}

	locked(keva.mu, func() {
		if keva.role == "slave" {
			ret.WriteString(fmt.Sprintf("master_host:%s\n", keva.masterHost))
			ret.WriteString(fmt.Sprintf("master_port:%s\n", keva.masterPort))
			if masterDead {
				ret.WriteString(fmt.Sprintf("master_link_down_since_seconds:%d\n", diedSince))
				ret.WriteString("master_link_status:down\n")
			} else {
				ret.WriteString("master_link_down_since_seconds:-1\n")
				ret.WriteString("master_link_status:up\n")
			}
			ret.WriteString(fmt.Sprintf("slave_repl_offset:%d\n", keva.offset))
			ret.WriteString(fmt.Sprintf("slave_priority:%d\n", keva.priority))
		}
	})
	return ret.String()
}

func (sl *slaveInfo) String() string {
	return fmt.Sprintf("offset: %d, priority: %d", sl.offset, sl.priority)
}

type slaveInfo struct {
	// runID  string

	masterHost string
	masterPort string
	offset     int
	lag        int
	priority   int
	master     *ToyKeva
}

func (keva *ToyKeva) diedSince() time.Duration {
	keva.mu.Lock()
	defer keva.mu.Unlock()
	return time.Since(*keva.diedAt)
}

func (keva *ToyKeva) kill() {
	keva.mu.Lock()
	defer keva.mu.Unlock()
	keva.alive = false
	now := time.Now()
	keva.diedAt = &now
}

func (keva *ToyKeva) isAlive() bool {
	keva.mu.Lock()
	defer keva.mu.Unlock()
	return keva.alive
}

type toyClient struct {
	link      *ToyKeva
	connected bool
	mu        *sync.Mutex
}

func (m *InstancesManager) NewSlaveKeva(host, port string) *ToyKeva {
	addr := host + ":" + port
	instance := &ToyKeva{
		role:             "slave",
		id:               uuid.NewString(),
		mu:               &sync.Mutex{},
		subs:             map[string]chan string{},
		alive:            true,
		host:             host,
		port:             port,
		InstancesManager: m,
	}
	m.addrMap[addr] = instance
	return instance
}

func (m *InstancesManager) NewMasterToyKeva(host, port string) *ToyKeva {
	addr := host + ":" + port

	instance := &ToyKeva{
		role:             "master",
		id:               uuid.NewString(),
		mu:               &sync.Mutex{},
		subs:             map[string]chan string{},
		alive:            true,
		host:             host,
		port:             port,
		InstancesManager: m,
	}
	m.addrMap[addr] = instance
	m.currentMaster = instance
	return instance
}

func (m *InstancesManager) SpawnSlaves(num int) map[string]*ToyKeva {
	m.mu.Lock()
	defer m.mu.Unlock()
	slaves := []*ToyKeva{}
	slaveMap := map[string]*ToyKeva{}
	master := m.currentMaster

	for i := 0; i < num; i++ {
		newSlave := m.NewSlaveKeva("localhost", strconv.Itoa(i)) // fake port, toy master does not call toy slave through network call
		newSlave.slaveInfo = &slaveInfo{
			masterHost: master.host,
			masterPort: master.port,
			priority:   1,
			offset:     0,
			lag:        0, // TODO: don't understand what it means
			master:     master,
		}
		addr := fmt.Sprintf("%s:%s", newSlave.host, newSlave.port)
		slaves = append(slaves, newSlave)
		slaveMap[addr] = newSlave
	}
	master.slaves = slaves
	return slaveMap
}

// func (keva *ToyKeva) turnToSlave() {
// 	keva.role = "slave"
// 	keva.alive = true
// }

func (keva *ToyKeva) turnToMaster() {
	keva.mu.Lock()
	keva.role = "master"
	keva.alive = true
	keva.slaveInfo = nil
	keva.mu.Unlock()
	locked(keva.InstancesManager.mu, func() {
		keva.InstancesManager.currentMaster = keva
	})
}

func NewToyKevaClient(keva *ToyKeva) *toyClient {
	return &toyClient{
		link:      keva,
		mu:        &sync.Mutex{},
		connected: true,
	}
}

func (cl *toyClient) SlaveOf(host, port string) error {
	if !cl.link.isAlive() {
		return fmt.Errorf("dead")
	}
	cl.link.slaveOf(host, port)
	return nil
}

func (cl *toyClient) SlaveOfNoOne() error {
	cl.link.turnToMaster()
	return nil
}

func (cl *toyClient) Info() (string, error) {
	if !cl.link.isAlive() {
		return "", fmt.Errorf("dead")
	}

	return cl.link.info(), nil
}

func (cl *toyClient) disconnect() {
	cl.mu.Lock()
	defer cl.mu.Unlock()
	cl.connected = false
}

// simulate network partition
func (cl *toyClient) isConnected() bool {
	cl.mu.Lock()
	defer cl.mu.Unlock()
	return cl.connected
}

func (cl *toyClient) Ping() (string, error) {
	if !cl.link.isAlive() || !cl.isConnected() {
		return "", fmt.Errorf("dead keva")
	}
	return "pong", nil
}

type toyHelloChan struct {
	root      *ToyKeva
	subChan   chan string
	sessionID string //
}

func (c *toyHelloChan) Publish(toBroadcast string) error {
	c.root.mu.Lock()
	for sessionID, sub := range c.root.subs {
		if sessionID == c.sessionID {
			continue
		}
		sub <- toBroadcast
	}
	c.root.mu.Unlock()
	return nil
}

func (c *toyHelloChan) Receive() (string, error) {
	newMsg := <-c.subChan
	return newMsg, nil
}

func (c *toyHelloChan) Close() error {
	c.root.mu.Lock()
	delete(c.root.subs, c.sessionID)
	c.root.mu.Unlock()
	close(c.subChan)
	return nil
}

func (cl *toyClient) SubscribeHelloChan() HelloChan {
	newChan := &toyHelloChan{
		root:      cl.link,
		subChan:   make(chan string, 1),
		sessionID: uuid.NewString(),
	}
	cl.link.mu.Lock()
	cl.link.subs[newChan.sessionID] = newChan.subChan
	cl.link.mu.Unlock()
	return newChan
}

// func (cl *toyClient) ExchangeSentinel(intro Intro) (ExchangeSentinelResponse, error) {
// 	cl.link.mu.Lock()
// 	var ret []SentinelIntroResponse
// 	for _, s := range cl.link.sentinels {
// 		ret = append(ret, SentinelIntroResponse{
// 			Addr:        s.addr,
// 			Port:        s.port,
// 			RunID:       s.runID,
// 			MasterName:  s.masterName,
// 			MasterPort:  s.masterPort,
// 			MasterAddr:  s.masterAddr,
// 			Epoch:       s.epoch,
// 			MasterEpoch: s.masterEpoch,
// 		})
// 	}
// 	cl.link.mu.Unlock()

// 	cl.link.addSentinel(intro)
// 	return ExchangeSentinelResponse{Sentinels: ret}, nil
// }
