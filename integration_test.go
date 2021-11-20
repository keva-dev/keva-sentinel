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
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap/zaptest/observer"
)

func TestSimpleCheck(t *testing.T) {
	s := &failoverTestSuite{}
	defer func() {
		s.TearDownTest(t)
	}()
	s.SetUpSuite(t)
	s.SetUpTest(t)
	s.TestSimpleCheck(t)
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

	// app, err := NewApp(cfg)
	// c.Assert(err, IsNil)

	// defer app.Close()

	// go func() {
	// 	app.Run()
	// }()

	// s.doCommand(c, port, "SET", "a", 1)
	// n, err := redis.Int(s.doCommand(c, port, "GET", "a"), nil)
	// c.Assert(err, IsNil)
	// c.Assert(n, Equals, 1)

	// ch := s.addBeforeHandler(app)

	// ms := app.masters.GetMasters()
	// assert.Equal()
	// c.Assert(ms, DeepEquals, []string{fmt.Sprintf("127.0.0.1:%d", port)})

	s.stopRedis(t, masterPort)

	// select {
	// case <-ch:
	// case <-time.After(5 * time.Second):
	// 	c.Fatal("check is not ok after 5s, too slow")
	// }
}

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
	fmt.Printf("master addr: %s\n", masterAddr)
	// parts := strings.Split(masterAddr, ":")
	// host, port := parts[0], parts[1]

	// make master remember slaves
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

// func (s *failoverTestSuite) TestFailoverCheck(c *C) {
// 	cfg := new(Config)
// 	cfg.Addr = ":11000"

// 	port := testPort[0]
// 	masterAddr := fmt.Sprintf("127.0.0.1:%d", port)

// 	cfg.Masters = []string{masterAddr}
// 	cfg.CheckInterval = 500
// 	cfg.MaxDownTime = 1

// 	app, err := NewApp(cfg)
// 	c.Assert(err, IsNil)

// 	defer app.Close()

// 	ch := s.addAfterHandler(app)

// 	go func() {
// 		app.Run()
// 	}()

// 	s.buildReplTopo(c)

// 	s.stopRedis(c, port)

// 	select {
// 	case <-ch:
// 	case <-time.After(5 * time.Second):
// 		c.Fatal("failover is not ok after 5s, too slow")
// 	}
// }

// func (s *failoverTestSuite) TestOneFaftFailoverCheck(c *C) {
// 	s.testOneClusterFailoverCheck(c, "raft")
// }

// func (s *failoverTestSuite) checkLeader(c *C, apps []*App) *App {
// 	for i := 0; i < 20; i++ {
// 		for _, app := range apps {
// 			if app != nil && app.cluster.IsLeader() {
// 				return app
// 			}
// 		}
// 		time.Sleep(500 * time.Millisecond)
// 	}

// 	c.Assert(1, Equals, 0)

// 	return nil
// }

// func (s *failoverTestSuite) testOneClusterFailoverCheck(c *C, broker string) {
// 	apps := s.newClusterApp(c, 1, 0, broker)
// 	app := apps[0]

// 	defer app.Close()

// 	s.checkLeader(c, apps)

// 	port := testPort[0]
// 	masterAddr := fmt.Sprintf("127.0.0.1:%d", port)

// 	err := app.addMasters([]string{masterAddr})
// 	c.Assert(err, IsNil)

// 	ch := s.addBeforeHandler(app)

// 	ms := app.masters.GetMasters()
// 	c.Assert(ms, DeepEquals, []string{fmt.Sprintf("127.0.0.1:%d", port)})

// 	s.stopRedis(c, port)

// 	select {
// 	case <-ch:
// 	case <-time.After(5 * time.Second):
// 		c.Fatal("check is not ok after 5s, too slow")
// 	}
// }

func (s *failoverTestSuite) buildReplTopo(t *testing.T) {
	port := testPort[0]

	s.doCommand(t, testPort[1], "SLAVEOF", "127.0.0.1", port)
	s.doCommand(t, testPort[2], "SLAVEOF", "127.0.0.1", port)

	s.doCommand(t, port, "SET", "a", 10)
	s.doCommand(t, port, "SET", "b", 20)

	s.waitReplConnected(t, testPort[1], 10)
	s.waitReplConnected(t, testPort[2], 10)

	s.waitSync(t, port, 10)

	n, err := redis.Int(s.doCommand(t, testPort[1], "GET", "a"), nil)
	assert.NoError(t, err)
	assert.Equal(t, 10, n)

	n, err = redis.Int(s.doCommand(t, testPort[2], "GET", "a"), nil)
	assert.NoError(t, err)
	assert.Equal(t, 10, n)
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
