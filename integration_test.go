package sentinel

import (
	"bytes"
	"fmt"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/garyburd/redigo/redis"
	"github.com/google/uuid"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap/zaptest/observer"
)

func TestIntegration_SimpleCheck(t *testing.T) {
	s := &failoverTestSuite{}
	defer func() {
		s.TearDownTest(t)
	}()
	s.SetUpSuite(t)
	s.SetUpTest(t)
	s.TestSimpleCheck(t)
}
func TestIntegration_Failover(t *testing.T) {
	s := &failoverTestSuite{}
	defer func() {
		s.TearDownTest(t)
	}()
	s.SetUpSuite(t)
	s.SetUpTest(t)
	s.CheckFailover(t)
}

type failoverTestSuite struct {
}

var testPort = []int{16379, 16380, 16381}

func (s *failoverTestSuite) SetUpSuite(t *testing.T) {
	_, err := exec.LookPath("redis-server")
	assert.NoError(t, err)
}

func (s *failoverTestSuite) TearDownSuite(testing.T) {

}

func (s *failoverTestSuite) SetUpTest(t *testing.T) {
	for _, port := range testPort {
		s.stopRedis(t, port)
		s.startRedis(t, port)
		s.doCommand(t, port, "SLAVEOF", "NO", "ONE")
		s.doCommand(t, port, "FLUSHALL")
	}
}

func (s *failoverTestSuite) TearDownTest(t *testing.T) {
	for _, port := range testPort {
		fmt.Println("stopping redis")
		s.stopRedis(t, port)
	}
}

type redisChecker struct {
	sync.Mutex
	ok  bool
	buf bytes.Buffer
}

func (r *redisChecker) Write(data []byte) (int, error) {
	r.Lock()
	defer r.Unlock()

	r.buf.Write(data)
	if strings.Contains(r.buf.String(), "to accept connections") {
		r.ok = true
	}
	fmt.Println(string(data))

	return len(data), nil
}

func (s *failoverTestSuite) startRedis(t *testing.T, port int) {
	checker := &redisChecker{ok: false}
	// start redis and use memory only
	cmd := exec.Command("redis-server", "--port", fmt.Sprintf("%d", port), "--save", "")
	cmd.Stdout = checker
	cmd.Stderr = checker

	err := cmd.Start()

	assert.NoError(t, err)

	for i := 0; i < 20; i++ {
		var ok bool
		checker.Lock()
		ok = checker.ok
		checker.Unlock()

		if ok {
			return
		}
		time.Sleep(500 * time.Millisecond)
	}
	assert.FailNow(t, "redis-server can not start ok after 10s")
}

func (s *failoverTestSuite) stopRedis(c *testing.T, port int) {
	cmd := exec.Command("redis-cli", "-p", fmt.Sprintf("%d", port), "shutdown", "nosave")
	cmd.Run()
}

func (s *failoverTestSuite) doCommand(t *testing.T, port int, cmd string, cmdArgs ...interface{}) interface{} {
	conn, err := redis.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", port))
	assert.NoError(t, err)

	v, err := conn.Do(cmd, cmdArgs...)
	assert.NoError(t, err)
	return v
}

func (s *failoverTestSuite) TestSimpleCheck(t *testing.T) {

	masterPort := testPort[0]

	setupIntegration(t, 3, func(c *Config) {
		c.Masters = []MasterMonitor{
			{
				Addr:      fmt.Sprintf("127.0.0.1:%d", masterPort),
				DownAfter: time.Second,
			},
		}
	})
}

// after this return, numInstances of sentinels have already recognized master
func setupIntegration(t *testing.T, numInstances int, customConf func(*Config)) *testSuite {
	file := filepath.Join("test", "config", "sentinel.yaml")
	viper.SetConfigType("yaml")
	viper.SetConfigFile(file)
	err := viper.ReadInConfig()
	assert.NoError(t, err)

	var conf Config
	err = viper.Unmarshal(&conf)
	assert.NoError(t, err)
	if customConf != nil {
		customConf(&conf)
	}
	masterAddr := conf.Masters[0].Addr
	sentinels := []*Sentinel{}
	logObservers := make([]*observer.ObservedLogs, numInstances)
	testLock := &sync.Mutex{}
	basePort := 2000
	mapRunIDToIdx := map[string]int{}
	mapIdxToRunID := map[int]string{}
	for i := 0; i < numInstances; i++ {
		localIdx := i
		// start new sentinel instance
		s, err := NewFromConfig(conf)
		assert.NoError(t, err)
		s.conf.Port = strconv.Itoa(basePort + localIdx)
		mapRunIDToIdx[s.runID] = localIdx
		mapIdxToRunID[localIdx] = s.runID

		// a function to create fake client from sentinel to other instance
		s.clientFactory = func(addr string) (InternalClient, error) {
			client, err := newInternalClient(addr)
			return client, err
		}

		// s.slaveFactory = toySlaveFactory
		customLogger, observer := customLogObserver()
		logObservers[localIdx] = observer
		s.logger = customLogger
		err = s.Start()
		assert.NoError(t, err)

		sentinels = append(sentinels, s)
		defer s.Shutdown()
	}
	// sleep for 2 second to ensure all sentinels have pubsub and recognized each other
	time.Sleep(3 * time.Second)
	for _, s := range sentinels {
		s.mu.Lock()
		masterI, ok := s.masterInstances[masterAddr]
		assert.True(t, ok)
		s.mu.Unlock()
		masterI.mu.Lock()
		assert.Equal(t, numInstances-1, len(masterI.sentinels))
		masterI.mu.Unlock()
	}
	suite := &testSuite{
		instances: sentinels,
		mu:        testLock,
		conf:      conf,
		history: history{
			termsVote:                   map[int][]termInfo{},
			termsVoters:                 map[int][]string{},
			termsLeader:                 map[int]string{},
			failOverStates:              map[int]failOverState{},
			termsSelectedSlave:          map[int]string{},
			termsPromotedSlave:          map[int]string{},
			termsMasterID:               map[int]string{},
			termsMasterInstanceCreation: map[int][]string{},
			instancesMasterSlaveMap:     map[string]instanceMasterSlaveMap{},
		},
		mapRunIDtoIdx: mapRunIDToIdx,
		logObservers:  logObservers,
		t:             t,
	}

	for idx := range suite.logObservers {
		go suite.consumeLogs(idx, suite.logObservers[idx])
	}
	return suite
}

func (s *failoverTestSuite) CheckFailover(t *testing.T) {
	var masterAddr string
	masterPort := testPort[0]

	s.buildReplTopo(t, testPort[0], testPort[1:])
	masterID := uuid.NewString()

	suite := setupIntegration(t, 3, func(c *Config) {
		masterAddr = fmt.Sprintf("127.0.0.1:%d", masterPort)
		c.Masters = []MasterMonitor{
			{
				Name:                 "test",
				RunID:                masterID,
				Addr:                 masterAddr,
				DownAfter:            time.Second,
				Quorum:               2,
				FailoverTimeout:      time.Second * 10,
				ReconfigSlaveTimeout: time.Second * 10,
				ParallelSync:         1,
			},
		}
	})
	//slave instances connected
	ok := eventually(t, func() bool {
		suite.mu.Lock()
		defer suite.mu.Unlock()
		for runID := range suite.mapRunIDtoIdx {
			masterSlavesMap, exist := suite.instancesMasterSlaveMap[runID]
			if !exist {
				return false
			}
			slaves, exist := masterSlavesMap[masterID]
			if !exist {
				return false
			}
			// may check more detail here
			if len(slaves) != len(testPort[1:]) {
				return false
			}
		}

		return true
	}, time.Second*3, "slave instances monitoring routines are not intitialized")
	if !ok {
		return
	}

	// setup slave for master

	// kill master
	s.stopRedis(t, masterPort)

	// check if all instance create new master instance
	instanceIDs := suite.checkTermMasterCreation(1, 3)
	suite.mu.Lock()
	totalInstances := []string{}
	for _, s := range suite.instances {
		totalInstances = append(totalInstances, s.runID)
	}
	assert.ElementsMatch(t, totalInstances, instanceIDs, "mismatch sentinels instances")
}

func (s *failoverTestSuite) buildReplTopo(t *testing.T, masterPort int, slavePorts []int) {
	// port := testPort[0]
	for _, p := range slavePorts {
		s.doCommand(t, p, "SLAVEOF", "127.0.0.1", masterPort)
	}

	s.doCommand(t, masterPort, "SET", "a", 10)
	s.doCommand(t, masterPort, "SET", "b", 20)

	// wait data sync
	for _, p := range slavePorts {
		s.waitReplConnected(t, p, 10)
	}

	s.waitSync(t, masterPort, 10)
	time.Sleep(2 * time.Second)

	// slaves are fully sync
	for _, p := range slavePorts {
		n, err := redis.Int(s.doCommand(t, p, "GET", "a"), nil)
		assert.NoError(t, err)
		if err != nil {
			fmt.Printf("here : %s\n", err.Error())
		}
		assert.Equal(t, 10, n)
	}
}

func (s *failoverTestSuite) waitReplConnected(t *testing.T, port int, timeout int) {
	for i := 0; i < timeout*2; i++ {
		v, _ := redis.Values(s.doCommand(t, port, "ROLE"), nil)
		tp, _ := redis.String(v[0], nil)
		if tp == "slave" {
			state, _ := redis.String(v[3], nil)
			if state == ConnectedState || state == SyncState {
				return
			}
		}

		time.Sleep(500 * time.Millisecond)
	}

	logger.Fatalf("wait %ds, but 127.0.0.1:%d can not connect to master", timeout, port)
}

func (s *failoverTestSuite) waitSync(t *testing.T, port int, timeout int) {
	g := newGroup(fmt.Sprintf("127.0.0.1:%d", port))

	for i := 0; i < timeout*2; i++ {
		err := g.doRole()
		assert.NoError(t, err)

		same := true
		offset := g.Master.Offset
		if offset > 0 {
			for _, slave := range g.Slaves {
				if slave.Offset != offset {
					same = false
				}
			}
		}
		if same {
			return
		}

		time.Sleep(500 * time.Millisecond)
	}

	logger.Fatalf("wait %ds, but all slaves can not sync the same with master %v", timeout, g)
}
