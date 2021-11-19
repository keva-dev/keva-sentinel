package sentinel

import (
	"fmt"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
	"golang.org/x/sync/errgroup"
)

var (
	stdLogger *zap.SugaredLogger
)

func init() {
	stdLogger = newLogger()
}

func customLogObserver() (*zap.SugaredLogger, *observer.ObservedLogs) {
	observedCore, recordedLogs := observer.New(zap.DebugLevel)
	normalCore := stdLogger.Desugar().Core()
	teeCore := zapcore.NewTee(observedCore, normalCore)
	built := zap.New(teeCore, zap.WithCaller(true)).Sugar()
	return built, recordedLogs
}

var (
	defaultMasterAddr = "localhost:6767"
)

type testSuite struct {
	mu            *sync.Mutex
	mapRunIDtoIdx map[string]int // code use runID as identifier, test suite use integer index, use this to remap
	mapIdxtoRunID map[int]string // code use runID as identifier, test suite use integer index, use this to remap
	instances     []*Sentinel
	// masterLinks   []*toyClient // each represent a connection from a sentinel to master
	conf    Config
	cluster *InstancesManager
	// master        *ToyKeva
	slavesMap map[string]*ToyKeva
	history
	logObservers []*observer.ObservedLogs
	t            *testing.T
}
type history struct {
	currentLeader               string
	currentTerm                 int
	termsVote                   map[int][]termInfo // key by term seq, val is array of each sentinels' term info
	termsVoters                 map[int][]string
	termsLeader                 map[int]string
	termsSelectedSlave          map[int]string
	termsPromotedSlave          map[int]string
	termsMasterID               map[int]string
	termsMasterInstanceCreation map[int][]string      // check which sentinel has initialize master instance
	failOverStates              map[int]failOverState // capture current failoverstate of each instance
}
type termInfo struct {
	selfVote      string
	neighborVotes map[string]string // info about what a sentinel sees other sentinel voted
}

func (t *testSuite) CleanUp() {
	for _, instance := range t.instances {
		instance.Shutdown()
	}
}

// this function also pre spawn some goroutines to continuously update testSuite state according
// to event logs recorded from sentinels instance, test functions only need to assert those states
func setupWithCustomConfig(t *testing.T, numInstances int, customConf func(*Config)) *testSuite {
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
	parts := strings.Split(masterAddr, ":")
	host, port := parts[0], parts[1]
	// fake cluster of instances
	toyCluster := &InstancesManager{
		addrMap:             map[string]*ToyKeva{},
		mu:                  &sync.Mutex{},
		simulateConnections: map[string]*toyClient{},
	}

	cluster := toyCluster.NewMasterToyKeva(host, port)
	slaveMap := cluster.SpawnSlaves(3)

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
			curMaster := cluster.getMaster()

			instance := cluster.getInstanceByAddr(addr)
			client := NewToyKevaClient(instance)

			if curMaster.getAddr() == addr {
				cluster.mu.Lock()
				defer cluster.mu.Unlock()
				cluster.simulateConnections[s.runID] = client
			}
			return client, nil
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
		// masterLinks: masterLinks,
		mu:      testLock,
		conf:    conf,
		cluster: toyCluster,
		// master:      master,
		slavesMap: slaveMap,
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

func setup(t *testing.T, numInstances int) *testSuite {
	return setupWithCustomConfig(t, numInstances, nil)
}

func TestInit(t *testing.T) {
	t.Run("3 instances", func(t *testing.T) {
		s := setup(t, 3)
		s.CleanUp()
	})
	t.Run("5 instances", func(t *testing.T) {
		s := setup(t, 5)
		s.CleanUp()
	})

}

func TestSDown(t *testing.T) {
	checkSdown := func(t *testing.T, numSdown int, numInstances int) {
		suite := setupWithCustomConfig(t, numInstances, func(c *Config) {
			//important: adjust quorum to the strictest level = numInstances
			c.Masters[0].Quorum = numInstances
		})
		defer suite.CleanUp()

		for _, s := range suite.instances {
			s.mu.Lock()
			masterI, ok := s.masterInstances[defaultMasterAddr]
			assert.True(t, ok)
			s.mu.Unlock()
			masterI.mu.Lock()
			assert.Equal(t, numInstances-1, len(masterI.sentinels))
			masterI.mu.Unlock()
		}

		suite.cluster.mu.Lock()
		counter := 0
		downInstances := map[string]struct{}{}
		for runID := range suite.cluster.simulateConnections {
			s := suite.cluster.simulateConnections[runID]
			downInstances[runID] = struct{}{}
			s.disconnect()
			counter++
			if counter == numSdown {
				break
			}
		}
		suite.cluster.mu.Unlock()

		// links[disconnectedIdx].disconnect()

		time.Sleep(suite.conf.Masters[0].DownAfter)
		// 1 more second for sure
		time.Sleep(1 * time.Second)

		// check if a given sentinel is in sdown state, and holds for a long time
		for _, sentinel := range suite.instances {
			_, isDown := downInstances[sentinel.selfID()]
			if isDown {
				// should be subjdown
				checkMasterState(t, defaultMasterAddr, sentinel, masterStateSubjDown)
			} else {
				// should be normal
				checkMasterState(t, defaultMasterAddr, sentinel, masterStateUp)
			}

		}
	}
	t.Run("1 out of 3 subjectively down", func(t *testing.T) {
		checkSdown(t, 1, 3)
	})
	t.Run("2 out of 4 subjectively down", func(t *testing.T) {
		checkSdown(t, 2, 3)
	})

}

func TestODown(t *testing.T) {
	testOdown := func(t *testing.T, numInstances int) {
		suite := setup(t, numInstances)

		for _, s := range suite.instances {
			s.mu.Lock()
			masterI, ok := s.masterInstances[defaultMasterAddr]
			assert.True(t, ok)
			s.mu.Unlock()
			masterI.mu.Lock()
			assert.Equal(t, numInstances-1, len(masterI.sentinels))
			masterI.mu.Unlock()
		}
		suite.cluster.killCurrentMaster()

		time.Sleep(suite.conf.Masters[0].DownAfter)

		// check if a given sentinel is in sdown state, and holds for a long time
		// others still see master is up
		gr := errgroup.Group{}
		for idx := range suite.instances {
			localSentinel := suite.instances[idx]
			gr.Go(func() error {
				met := eventually(t, func() bool {
					return masterStateIs(defaultMasterAddr, localSentinel, masterStateObjDown)
				}, 5*time.Second, "sentinel %s did not recognize master as o down", localSentinel.listener.Addr())
				if !met {
					return fmt.Errorf("sentinel %s did not recognize master as o down", localSentinel.listener.Addr())
				}
				return nil
			})
		}
		assert.NoError(t, gr.Wait())
	}
	t.Run("3 instances o down", func(t *testing.T) {
		testOdown(t, 3)
	})
	t.Run("5 instances o down", func(t *testing.T) {
		testOdown(t, 5)
	})
}
func (suite *testSuite) checkTermVoteOfSentinel(t *testing.T, sentinelIdx int, term int) {
	currentSentinel := suite.instances[sentinelIdx]
	reached := eventually(t, func() bool {
		suite.mu.Lock()
		defer suite.mu.Unlock()
		termInfo := suite.termsVote[term][sentinelIdx]
		return termInfo.selfVote != ""
	}, 10*time.Second, "sentinel %s never votes for any instance in term %d", currentSentinel.runID, term)
	if !reached {
		assert.FailNowf(suite.t, "sentinel did not vote for any instance", "")
	}

}

func (suite *testSuite) checkTermVoteOfSentinelNeighbor(t *testing.T, instanceIdx int, neighborID string, term int) {
	currentSentinel := suite.instances[instanceIdx]
	eventually(t, func() bool {
		suite.mu.Lock()
		defer suite.mu.Unlock()
		termInfo := suite.termsVote[term][instanceIdx]

		if termInfo.neighborVotes == nil {
			return false
		}
		vote := termInfo.neighborVotes[neighborID]
		return vote != ""
	}, 10*time.Second, "sentinel %s cannot get its neighbor's leader in term %d", currentSentinel.runID, term)
}

// - create a stream of logs, observe from stream and change status of test suite
// - assert function wait for the change of status only
func TestLeaderVoteNotConflict(t *testing.T) {
	assertion := func(t *testing.T, numInstances int) {
		suite := setupWithCustomConfig(t, numInstances, func(c *Config) {
			c.Masters[0].Quorum = numInstances/2 + 1 // force normal quorum
		})
		suite.cluster.killCurrentMaster()
		time.Sleep(suite.conf.Masters[0].DownAfter)

		// check if a given sentinel is in sdown state, and holds for a long time
		// others still see master is up
		gr := errgroup.Group{}
		suite.mu.Lock()
		suite.termsVote[1] = make([]termInfo, len(suite.instances))
		suite.mu.Unlock()
		for idx := range suite.instances {
			instanceIdx := idx
			gr.Go(func() error {
				suite.checkTermVoteOfSentinel(t, instanceIdx, 1)
				return nil
			})
		}
		gr.Wait()

		gr2 := errgroup.Group{}
		for idx := range suite.instances {
			localSentinel := suite.instances[idx]
			m := getSentinelMaster(defaultMasterAddr, localSentinel)
			m.mu.Lock()

			for sentinelIdx := range m.sentinels {
				si := m.sentinels[sentinelIdx]
				si.mu.Lock()
				neighborID := si.runID
				si.mu.Unlock()

				instanceIdx := idx

				gr2.Go(func() error {
					suite.checkTermVoteOfSentinelNeighbor(t, instanceIdx, neighborID, 1)
					return nil
				})
			}
			m.mu.Unlock()
		}
		gr2.Wait()
		suite.mu.Lock()
		defer suite.mu.Unlock()

		for idx := range suite.instances {
			thisInstanceHistory := suite.termsVote[1][idx]
			thisInstanceVote := thisInstanceHistory.selfVote

			thisInstanceID := suite.instances[idx].runID

			for idx2 := range suite.instances {
				if idx2 == idx {
					continue
				}
				neiborInstanceVote := suite.termsVote[1][idx2]
				if neiborInstanceVote.neighborVotes[thisInstanceID] != thisInstanceVote {
					assert.Failf(t, "conflict vote between instances",
						"instance %s records that instance %s voted for %s, but %s says it voted for %s",
						suite.instances[idx2].runID,
						thisInstanceID,
						neiborInstanceVote.neighborVotes[thisInstanceID],
						thisInstanceID,
						thisInstanceVote,
					)
				}
			}
		}
		// 1.for each instance, compare its vote with how other instances records its vote
		// 2.record each instance leader, find real leader of that term
		// 3.find that real leader and check if its failover state is something in selecting slave
	}
	t.Run("3 instances do not conflict", func(t *testing.T) {
		assertion(t, 3)
	})
}

func TestLeaderElection(t *testing.T) {
	assertion := func(t *testing.T, numInstances int) {
		suite := setupWithCustomConfig(t, numInstances, func(c *Config) {
			c.Masters[0].Quorum = numInstances/2 + 1 // force normal quorum
		})
		suite.cluster.killCurrentMaster()
		time.Sleep(suite.conf.Masters[0].DownAfter)
		//TODO: check more info of this recognized leader

		voters := suite.checkVotersOfTerm(1, numInstances)
		expect := []string{}
		for _, item := range suite.instances {
			expect = append(expect, item.runID)
		}
		assert.ElementsMatch(t, expect, voters)
		suite.checkClusterHasLeader()
	}
	t.Run("3 instances vote leader success", func(t *testing.T) {
		assertion(t, 3)
	})
}

func TestPromoteSlave(t *testing.T) {
	// slaveCustomizer let test customize initial slave config (offset, priority) to check if sentinel choose
	// correct instance for failover
	assertion := func(t *testing.T, numInstances int, slaveCustomizer func(map[string]*ToyKeva) *ToyKeva) {
		suite := setupWithCustomConfig(t, numInstances, func(c *Config) {
			c.Masters[0].Quorum = numInstances/2 + 1 // force normal quorum
		})
		expectedChosenSlave := slaveCustomizer(suite.slavesMap)

		suite.cluster.killCurrentMaster()
		time.Sleep(suite.conf.Masters[0].DownAfter)
		//TODO: check more info of this recognized leader
		suite.checkClusterHasLeader()
		promotedSlave := suite.checkTermPromotedSlave(1)

		if promotedSlave == "" {
			assert.FailNowf(t, "empty slave promoted", "term %d has no promoted slave", 1)
		} else {
			suite.mu.Lock()
			selectedSlave := suite.termsSelectedSlave[1]
			suite.mu.Unlock()
			assert.Equal(t, selectedSlave, promotedSlave,
				"different promoted and selected slave", "promoted (%s) slave is different from selected (%s) slave", promotedSlave, selectedSlave)

			assert.Equal(t, expectedChosenSlave.id, promotedSlave, "wrong slave promoted", "want %s, but have %s", expectedChosenSlave.id,
				promotedSlave,
			)
		}
	}
	t.Run("select slave by highest offset", func(t *testing.T) {
		assertion(t, 3, func(slaveMap map[string]*ToyKeva) *ToyKeva {
			for idx := range slaveMap {
				slave := slaveMap[idx]
				slave.mu.Lock()
				slave.offset = 10
				slave.mu.Unlock()
				return slave
			}
			return nil
		})
	})
	t.Run("select slave by highest priority", func(t *testing.T) {
		assertion(t, 3, func(slaveMap map[string]*ToyKeva) *ToyKeva {
			for idx := range slaveMap {
				slave := slaveMap[idx]
				slave.mu.Lock()
				slave.priority = 10
				slave.mu.Unlock()
				return slave
			}
			return nil
		})
	})
}
func TestResetMasterInstance(t *testing.T) {
	// slaveCustomizer let test customize initial slave config (offset, priority) to check if sentinel choose
	// correct instance for failover
	assertion := func(t *testing.T, numInstances int, slaveCustomizer func(map[string]*ToyKeva) *ToyKeva) {
		suite := setupWithCustomConfig(t, numInstances, func(c *Config) {
			c.Masters[0].Quorum = numInstances/2 + 1 // force normal quorum
		})
		expectedNewMaster := slaveCustomizer(suite.slavesMap)
		expectedNewMaster.mu.Lock()
		expectedID := expectedNewMaster.id
		expectedNewMaster.mu.Unlock()
		suite.cluster.killCurrentMaster()
		time.Sleep(suite.conf.Masters[0].DownAfter)
		//TODO: check more info of this recognized leader
		promotedSlave := suite.checkTermPromotedSlave(1)
		suite.checkClusterHasLeader()
		newRunID := suite.checkTermMasterRunID(1)
		assert.Equal(t, promotedSlave, newRunID)
		if newRunID != "" {
			assert.Equal(t, newRunID, expectedID, "failover to wrong master", "term %d wants master %s instead of %s", expectedID, newRunID)
		}

		// check if all instance create new master instance
		instanceIDs := suite.checkTermMasterCreation(1, 3)
		suite.mu.Lock()
		totalInstances := []string{}
		for _, s := range suite.instances {
			totalInstances = append(totalInstances, s.runID)
		}
		assert.ElementsMatch(t, totalInstances, instanceIDs, "mismatch sentinels instances")
		suite.mu.Unlock()
	}
	t.Run("select slave by highest offset", func(t *testing.T) {
		assertion(t, 3, func(slaveMap map[string]*ToyKeva) *ToyKeva {
			for idx := range slaveMap {
				slave := slaveMap[idx]
				slave.mu.Lock()
				slave.offset = 10
				slave.mu.Unlock()
				return slave
			}
			return nil
		})
	})
	t.Run("select slave by highest priority", func(t *testing.T) {
		assertion(t, 3, func(slaveMap map[string]*ToyKeva) *ToyKeva {
			for idx := range slaveMap {
				slave := slaveMap[idx]
				slave.mu.Lock()
				slave.priority = 10
				slave.mu.Unlock()
				return slave
			}
			return nil
		})
	})
}

func (s *testSuite) checkTermMasterCreation(term int, length int) []string {
	var ret []string
	eventually(s.t, func() bool {
		s.mu.Lock()
		defer s.mu.Unlock()
		sentinelIds, exist := s.termsMasterInstanceCreation[term]
		if exist && len(sentinelIds) == length {
			ret = sentinelIds
			return true
		}
		return false
	}, 5*time.Second, "term %d has not enough %d master creation event", term, length)
	return ret
}
func (s *testSuite) checkTermMasterRunID(term int) string {
	var ret string
	eventually(s.t, func() bool {
		s.mu.Lock()
		defer s.mu.Unlock()
		runID, exist := s.termsMasterID[term]
		if exist && runID != "" {
			ret = runID
			return true
		}
		return false
	}, 5*time.Second, "term %d has no master created", term)
	return ret
}

func (s *testSuite) checkTermPromotedSlave(term int) string {
	var ret string
	eventually(s.t, func() bool {
		s.mu.Lock()
		defer s.mu.Unlock()
		promoted, exist := s.termsPromotedSlave[term]
		if exist && promoted != "" {
			ret = promoted
			return true
		}
		return false
	}, 5*time.Second, "term %d has no slave promoted", term)
	return ret
}

func (s *testSuite) checkVotersOfTerm(term int, expectVoters int) []string {
	var ret []string
	eventually(s.t, func() bool {
		s.mu.Lock()
		defer s.mu.Unlock()
		voters := s.termsVoters[term]
		if len(voters) == expectVoters {
			ret = voters
			return true
		}
		return false
	}, 10*time.Second, "term %d does not have %d voters", term, expectVoters)
	return ret
}

func (s *testSuite) checkClusterHasLeader() {
	eventually(s.t, func() bool {
		s.mu.Lock()
		defer s.mu.Unlock()
		return s.currentLeader != ""
	}, 10*time.Second, "current leader is empty")
}

func getSentinelMaster(masterAddr string, s *Sentinel) *masterInstance {
	s.mu.Lock()
	m := s.masterInstances[masterAddr]
	s.mu.Unlock()
	return m
}

func eventually(t *testing.T, f func() bool, duration time.Duration, msgAndArgs ...interface{}) bool {
	return assert.Eventually(t, f, duration, 50*time.Millisecond, msgAndArgs...)
}

func checkMasterState(t *testing.T, masterAddr string, s *Sentinel, state masterInstanceState) {
	assert.Equal(t, state, getSentinelMaster(masterAddr, s).getState())
}

func masterStateIs(masterAddr string, s *Sentinel, state masterInstanceState) bool {
	s.mu.Lock()
	m := s.masterInstances[masterAddr]
	s.mu.Unlock()
	return state == m.getState()
}
