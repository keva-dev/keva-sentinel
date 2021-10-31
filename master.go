package sentinel

import (
	"context"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"
)

func (m *masterInstance) keepSendingPeriodRequest() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.stopSendingRequest
}

func (m *masterInstance) killed() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.isKilled
}

func (s *Sentinel) masterPingRoutine(m *masterInstance) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for !m.keepSendingPeriodRequest() {
		<-ticker.C
		_, err := m.client.Ping()
		if err != nil {
			if time.Since(m.lastSuccessfulPing) > m.sentinelConf.DownAfter {
				//notify if it is down
				if m.getState() == masterStateUp {
					m.mu.Lock()
					m.state = masterStateSubjDown
					m.downSince = time.Now()
					s.logger.Warnf("master %s is subjectively down", m.name)
					m.mu.Unlock()
					select {
					case m.subjDownNotify <- struct{}{}:
					default:
					}
				}
			}
			continue
		} else {
			m.mu.Lock()
			state := m.state
			m.lastSuccessfulPing = time.Now()
			m.mu.Unlock()
			if state == masterStateSubjDown || state == masterStateObjDown {
				m.mu.Lock()
				m.state = masterStateUp
				m.mu.Unlock()
				//select {
				//case m.reviveNotify <- struct{}{}:
				//default:
				//}
				//revive
			}

		}

	}
}

func (m *masterInstance) getFailOverStartTime() time.Time {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.failOverStartTime
}

func (m *masterInstance) getFailOverEpoch() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.failOverEpoch
}

func (m *masterInstance) getFailOverState() failOverState {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.failOverState
}
func (m *masterInstance) getAddr() string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return fmt.Sprintf("%s:%s", m.host, m.port)
}

func (m *masterInstance) getState() masterInstanceState {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.state
}

// for now, only say hello through master, redis also say hello through slaves
func (s *Sentinel) sayHelloRoutineToMaster(m *masterInstance, helloChan HelloChan) {
	for !m.keepSendingPeriodRequest() {
		time.Sleep(2 * time.Second)
		m.mu.Lock()
		masterName := m.name
		masterIP := m.host
		masterPort := m.port
		masterConfigEpoch := m.configEpoch
		m.mu.Unlock()
		info := fmt.Sprintf("%s,%s,%s,%d,%s,%s,%s,%d",
			s.conf.Binds[0], s.conf.Port, s.selfID(),
			s.getCurrentEpoch(), masterName, masterIP, masterPort, masterConfigEpoch,
		)
		err := helloChan.Publish(info)
		if err != nil {
			s.logger.Errorf("helloChan.Publish: %s", err)
		}
	}
}

/* Format is composed of 8 tokens:
 * 0=ip,1=port,2=runid,3=current_epoch,4=master_name,
 * 5=master_ip,6=master_port,7=master_config_epoch.
 */
// Todo: this function is per instance, not per master
func (s *Sentinel) subscribeHello(m *masterInstance) {
	helloChan := m.client.SubscribeHelloChan()
	defer helloChan.Close()
	selfID := s.selfID()
	go s.sayHelloRoutineToMaster(m, helloChan)
	for !m.keepSendingPeriodRequest() {
		newmsg, err := helloChan.Receive()
		if err != nil {
			s.logger.Errorf("helloChan.Receive: %s", err)
			continue
			//skip for now
		}
		parts := strings.Split(newmsg, ",")
		if len(parts) != 8 {
			s.logger.Errorf("helloChan.Receive: invalid format for newmsg: %s", newmsg)
			continue
		}
		runid := parts[2]
		if runid == selfID {
			continue
		}
		m.mu.Lock()
		_, ok := m.sentinels[runid]
		if !ok {
			client, err := newRPCClient(parts[0], parts[1])
			if err != nil {
				s.logger.Errorf("newRPCClient: cannot create new client to other sentinel with info: %s: %s", newmsg, err)
				m.mu.Unlock()
				continue
			}
			si := &sentinelInstance{
				mu:     sync.Mutex{},
				sdown:  false,
				client: client,
				runID:  runid,
			}

			m.sentinels[runid] = si
			s.logger.Infof("subscribeHello: connected to new sentinel: %s", newmsg)
		}
		m.mu.Unlock()
		//ignore if exist for now
	}
}

func (s *Sentinel) masterRoutine(m *masterInstance) {
	s.logger.Debugw(logEventMasterInstanceCreated,
		"master_name", m.name,
		"run_id", m.runID,
		"sentinel_run_id", s.runID,
		"epoch", m.configEpoch,
	)
	go s.masterPingRoutine(m)
	go s.subscribeHello(m)
	infoTicker := time.NewTicker(10 * time.Second)
	for !m.killed() {
		switch m.getState() {
		case masterStateUp:
			select {
			case <-m.shutdownChan:
				return
			case <-m.subjDownNotify:
				//notify allowing break select loop early, rather than waiting for 10s
				infoTicker.Stop()

			case <-infoTicker.C:
				info, err := m.client.Info()
				if err != nil {
					//TODO continue for now
					continue
				}
				roleSwitched, err := s.parseInfoMaster(m.getAddr(), info)
				if err != nil {
					s.logger.Errorf("parseInfoMaster: %v", err)
					continue
					// continue for now
				}
				if roleSwitched {
					// if master has been switched to slave, this loop should
					// should have been killed and started as a loop for slave
					panic("unimlemented")
					//TODO
					// s.parseInfoSlave()
				}
			}

		case masterStateSubjDown:
		SdownLoop:
			for {
				switch m.getState() {
				case masterStateSubjDown:
					// check if quorum as met
					s.askSentinelsIfMasterIsDown(m)
					s.checkObjDown(m)
					if m.getState() == masterStateObjDown {
						s.notifyMasterDownToSlaveRoutines(m)
						break SdownLoop
					}
				case masterStateObjDown:
					panic("no other process should set masterStateObjDown")
				case masterStateUp:
					break SdownLoop
				}
				time.Sleep(1 * time.Second)
			}
		case masterStateObjDown:

			// stopAskingOtherSentinels: used when the fsm state wnats to turn back to this current line
			// canceling the on going goroutine, or when the failover is successful
			ctx, stopAskingOtherSentinels := context.WithCancel(context.Background())
			go s.askOtherSentinelsEach1Sec(ctx, m)
			sleepToNextFailover := s.nextFailoverAllowed(m)
			timer := time.NewTimer(sleepToNextFailover)
			defer timer.Stop()
			select {
			case <-timer.C:
			case <-m.followerNewMasterNotify:
				stopAskingOtherSentinels()
				s.resetMasterInstance(m, true)

				// this is a follower sentinel and new master recognize
				// from leader sentinel through hello message
				return
			}

			// If any logic changing state of master to something != masterStateObjDown, this code below will be broken
		failOverFSM:
			for {
				switch m.getFailOverState() {
				// non-leader can not reach next step
				case failOverWaitLeaderElection:
					aborted, isleader := s.checkElectionStatus(m)
					if isleader {
						// to next state of fsm
						continue
					}
					if aborted {
						stopAskingOtherSentinels()
						break failOverFSM
					}
					ticker := time.NewTicker(1 * time.Second)

					select {
					case <-ticker.C:
					case <-m.followerNewMasterNotify:
						stopAskingOtherSentinels()
						s.resetMasterInstance(m, true)

						// this is a follower sentinel and new master recognize
						// from leader sentinel through hello message
						return
					}

				case failOverSelectSlave:
					slave := s.selectSlave(m)

					// abort failover, start voting for new epoch
					if slave == nil {
						s.abortFailover(m)
						stopAskingOtherSentinels()
					} else {
						s.promoteSlave(m, slave)
					}
				case failOverPromoteSlave:
					m.mu.Lock()
					promotedSlave := m.promotedSlave
					m.mu.Unlock()

					timeout := time.NewTimer(m.sentinelConf.FailoverTimeout)
					defer timeout.Stop()
					select {
					case <-timeout.C:
						s.abortFailover(m)
						// when new slave recognized to have switched role, it will notify this channel
						// or time out
					case <-promotedSlave.masterRoleSwitchChan:
						var (
							epoch int
						)
						locked(&m.mu, func() {
							m.failOverState = failOverReconfSlave
							m.failOverStateChangeTime = time.Now()
							epoch = m.failOverEpoch
							m.configEpoch = epoch
						})
						s.logger.Debugw(logEventSlavePromoted,
							"run_id", promotedSlave.runID,
							"epoch", epoch)
						s.logger.Debugw(logEventFailoverStateChanged,
							"epoch", m.getFailOverEpoch(),
							"new_state", failOverReconfSlave)

					}
				case failOverReconfSlave:
					s.reconfigRemoteSlaves(m)
					s.logger.Debugw(logEventFailoverStateChanged,
						"epoch", m.getFailOverEpoch(),
						"new_state", failOverResetInstance)
					s.resetMasterInstance(m, false)

				}
			}
		}
	}
}
func (s *Sentinel) resetMasterInstance(m *masterInstance, isfollower bool) {

	var (
		promoted                                                *slaveInstance
		oldAddr                                                 string
		epoch                                                   int
		name                                                    string
		promotedRunID, promotedAddr, promotedHost, promotedPort string
		sentinels                                               map[string]*sentinelInstance
	)
	locked(&m.mu, func() {
		if isfollower {
			epoch = m.leaderEpoch
		} else {
			epoch = m.failOverEpoch
		}
		promoted = m.promotedSlave
		name = m.name
		m.isKilled = true
		oldAddr = fmt.Sprintf("%s:%s", m.host, m.port)
		sentinels = m.sentinels
	})

	locked(&promoted.mu, func() {
		promotedAddr = promoted.addr
		promotedHost, promotedPort = promoted.host, promoted.port
		promotedRunID = m.promotedSlave.runID
	})

	cl, err := s.clientFactory(promotedAddr)
	if err != nil {
		panic("todo")
	}

	newSlaves := map[string]*slaveInstance{}
	newMaster := &masterInstance{
		runID:                   promotedRunID,
		sentinelConf:            m.sentinelConf,
		name:                    name,
		host:                    promotedHost,
		port:                    promotedPort,
		configEpoch:             m.configEpoch,
		mu:                      sync.Mutex{},
		client:                  cl,
		slaves:                  map[string]*slaveInstance{},
		sentinels:               sentinels,
		state:                   masterStateUp,
		lastSuccessfulPing:      time.Now(),
		subjDownNotify:          make(chan struct{}),
		followerNewMasterNotify: make(chan struct{}, 1),
	}

	locked(&m.mu, func() {
		for _, sl := range m.slaves {
			locked(&sl.mu, func() {
				// kill all running slaves routine, for simplicity
				sl.killed = true
				if sl.addr != promotedAddr {
					newslave, err := s.newSlaveInstance(promotedHost, promotedPort, sl.host, sl.port, sl.replOffset, newMaster)
					if err == nil {
						newSlaves[sl.addr] = newslave
					}
				}
			})
		}
		// turn dead master into slave as well
		newslave, err := s.newSlaveInstance(promotedHost, promotedPort, m.host, m.port, 0, newMaster)
		if err != nil {

		} else {
			newSlaves[oldAddr] = newslave
		}
		m.slaves = newSlaves
	})

	locked(s.mu, func() {
		delete(s.masterInstances, oldAddr)
		s.masterInstances[newMaster.getAddr()] = newMaster
	})

	for idx := range newSlaves {
		go s.slaveRoutine(newSlaves[idx])
	}
	s.logger.Debugw(logEventFailoverStateChanged,
		"new_state", failOverDone,
		"epoch", epoch,
	)
	go s.masterRoutine(newMaster)
}

// TODO: write test for this func
func (s *Sentinel) reconfigRemoteSlaves(m *masterInstance) error {
	m.mu.Lock()

	slaveList := make([]*slaveInstance, 0, len(m.slaves))
	var promotedHost, promotedPort string
	for runID := range m.slaves {
		slaveList = append(slaveList, m.slaves[runID])
	}
	m.mu.Unlock()
	sema := semaphore.NewWeighted(int64(m.sentinelConf.ParallelSync))

	errgrp := &errgroup.Group{}
	ctx, cancel := context.WithTimeout(context.Background(), m.sentinelConf.ReconfigSlaveTimeout)
	defer cancel()
	locked(&m.promotedSlave.mu, func() {
		promotedHost, promotedPort = m.promotedSlave.host, m.promotedSlave.port
	})
	for idx := range slaveList {
		slave := slaveList[idx]
		if slave.runID == m.promotedSlave.runID {
			continue
		}
		errgrp.Go(func() error {
			var done bool
			for {
				err := sema.Acquire(ctx, 1)
				if err != nil {
					return err
				}
				// goroutine exit if ctx is canceled
				select {
				case <-ctx.Done():
					return ctx.Err()
				default:
				}
				err = slave.client.SlaveOf(promotedHost, promotedPort)
				if err != nil {
					sema.Release(1)
					time.Sleep(100 * time.Millisecond)
					continue
				}
				slave.mu.Lock()
				slave.reconfigFlag |= reconfigSent
				slave.mu.Unlock()
				break
			}

			for {
				slave.mu.Lock()
				if slave.reconfigFlag&reconfigDone != 0 {
					slave.reconfigFlag = 0
					done = true
				}
				slave.mu.Unlock()
				if done {
					sema.Release(1)
					return nil
				}
				select {
				case <-ctx.Done():
					return ctx.Err()
				default:
				}
				// TODO: may use sync.Cond instead of ticking
				time.Sleep(1 * time.Second)
			}
		})
	}
	return errgrp.Wait()
	// update state no matter what
}

func (s *Sentinel) nextFailoverAllowed(m *masterInstance) time.Duration {
	// this code only has to wait in case failover timeout reached and block until the next failover is allowed to
	// continue (2* failover timeout)
	retry := false
	startTime := m.getFailOverStartTime()
	secondsLeft := 2*m.sentinelConf.FailoverTimeout - time.Since(startTime)
	if secondsLeft <= 0 {
		locked(s.mu, func() {
			locked(&m.mu, func() {
				if m.failOverStartTime.Equal(startTime) {
					// if some how, this sentinel receive new epoch from hello message
					// before this code block runs,
					// it will uncessarily increment the epoch one more time, causing already
					// voting candidates to fail, need logic to check this
					// TODO
					s.currentEpoch += 1
					if m.failOverState != failOverWaitLeaderElection {
						s.logger.Debugw(logEventFailoverStateChanged,
							"new_state", failOverWaitLeaderElection,
							"epoch", s.currentEpoch,
						)
						s.logger.Debugw(logEventRequestingElection,
							"epoch", s.currentEpoch,
							"sentinel_run_id", s.runID,
						)
					}
					m.failOverState = failOverWaitLeaderElection
					m.failOverStartTime = time.Now()
					m.failOverEpoch = s.currentEpoch
				} else {
					// some other thread has modify this start time already
					retry = true
				}
			})
		})
		// mostly, when obj down is met, multiples sentinels will try to send request for vote to be leader
		// to prevent split vote, sleep for a random small duration
		secondsLeft = 0
	}
	if retry {
		return s.nextFailoverAllowed(m)
	}
	return secondsLeft + time.Duration(rand.Intn(SENTINEL_MAX_DESYNC)*int(time.Millisecond))
}

func (s *Sentinel) checkElectionStatus(m *masterInstance) (aborted bool, isLeader bool) {
	leader, epoch := s.checkWhoIsLeader(m)
	isLeader = leader != "" && leader == s.selfID()

	if !isLeader {
		//abort if failover take too long
		if time.Since(m.getFailOverStartTime()) > m.sentinelConf.FailoverTimeout {
			s.abortFailover(m)
			return true, false
		}
		return false, false
	}

	// do not call cancel(), keep receiving update from other sentinel
	s.logger.Debugw(logEventBecameTermLeader,
		"run_id", leader,
		"epoch", epoch)

	m.mu.Lock()
	m.failOverState = failOverSelectSlave
	m.mu.Unlock()

	s.logger.Debugw(logEventFailoverStateChanged,
		"new_state", failOverSelectSlave,
		"epoch", epoch, // epoch = failover epoch = sentinel current epoch
	)
	return false, true
}

func (s *Sentinel) promoteSlave(m *masterInstance, slave *slaveInstance) {
	m.mu.Lock()
	m.promotedSlave = slave
	m.failOverState = failOverPromoteSlave
	m.failOverStateChangeTime = time.Now()
	epoch := m.failOverEpoch
	m.mu.Unlock()
	slave.mu.Lock()
	slaveAddr := fmt.Sprintf("%s:%s", slave.host, slave.port)
	slaveID := slave.runID
	client := slave.client
	slave.mu.Unlock()
	s.logger.Debugw(logEventSelectedSlave,
		"slave_addr", slaveAddr,
		"slave_id", slaveID,
		"epoch", epoch,
	)

	s.logger.Debugw(logEventFailoverStateChanged,
		"new_state", failOverPromoteSlave,
		"epoch", epoch,
	)
	err := client.SlaveOfNoOne()
	if err != nil {
		s.logger.Errorf("send slave of no one to slave %s return error %s", slaveAddr, err.Error())
	}
}
func (s *Sentinel) abortFailover(m *masterInstance) {
	m.mu.Lock()
	m.failOverState = failOverNone
	m.failOverStateChangeTime = time.Now()
	epoch := m.failOverEpoch
	m.mu.Unlock()

	s.logger.Debugw(logEventFailoverStateChanged,
		"new_state", failOverNone,
		"epoch", epoch,
	)
}

func (s *Sentinel) notifyMasterDownToSlaveRoutines(m *masterInstance) {
	m.mu.Lock()
	for idx := range m.slaves {
		m.slaves[idx].masterDownNotify <- struct{}{}
	}
	m.mu.Unlock()
}

func (s *Sentinel) checkWhoIsLeader(m *masterInstance) (string, int) {
	instancesVotes := map[string]int{}
	currentEpoch := s.getCurrentEpoch()
	// instancesVoters := map[string][]string{}
	m.mu.Lock()
	for _, sen := range m.sentinels {
		sen.mu.Lock()
		if sen.leaderID != "" && sen.leaderEpoch == currentEpoch {
			instancesVotes[sen.leaderID] = instancesVotes[sen.leaderID] + 1
			// instancesVoters[sen.leaderID] = append(instancesVoters[sen.leaderID], sen.runID)
		}
		sen.mu.Unlock()
	}
	totalSentinels := len(m.sentinels) + 1
	m.mu.Unlock()
	maxVote := 0
	var winner string
	for runID, vote := range instancesVotes {
		if vote > maxVote {
			maxVote = vote
			winner = runID
		}
	}
	epoch := m.getFailOverEpoch()
	var leaderEpoch int
	var votedByMe string
	if winner != "" {
		// vote for the most winning candidate
		leaderEpoch, votedByMe = s.voteLeader(m, epoch, winner)
	} else {
		leaderEpoch, votedByMe = s.voteLeader(m, epoch, s.selfID())
	}
	if votedByMe != "" && leaderEpoch == epoch {
		instancesVotes[votedByMe] = instancesVotes[votedByMe] + 1
		if instancesVotes[votedByMe] > maxVote {
			maxVote = instancesVotes[votedByMe]
			winner = votedByMe
		}
		// instancesVoters[votedByMe] = append(instancesVoters[votedByMe], s.selfID())
	}
	quorum := totalSentinels/2 + 1
	if winner != "" && (maxVote < quorum || maxVote < m.sentinelConf.Quorum) {
		winner = ""
	}

	return winner, currentEpoch
}

func (s *Sentinel) checkObjDown(m *masterInstance) {
	m.mu.Lock()
	defer m.mu.Unlock()
	quorum := m.sentinelConf.Quorum
	if len(m.sentinels)+1 < quorum {
		panic(fmt.Sprintf("quorum too large (%d) or too few sentinel instances (%d)", quorum, len(m.sentinels)))
		// TODO: log warning only
	}

	down := 1
	for _, sentinel := range m.sentinels {
		sentinel.mu.Lock()
		if sentinel.sdown {
			down++
		}
		sentinel.mu.Unlock()
	}
	if down >= quorum {
		m.state = masterStateObjDown
		m.stopSendingRequest = true
	}
}

func (s *Sentinel) askOtherSentinelsEach1Sec(ctx context.Context, m *masterInstance) {
	m.mu.Lock()
	masterName := m.name
	masterIp := m.host
	masterPort := m.port

	for idx := range m.sentinels {
		sentinel := m.sentinels[idx]
		go func() {
			var loggedForNeighbor bool
			for m.getState() >= masterStateSubjDown {
				select {
				case <-ctx.Done():
					return
				default:
				}

				s.mu.Lock()
				currentEpoch := s.currentEpoch //epoch may change during failover
				selfID := s.runID              // locked as well for sure
				s.mu.Unlock()

				// do not ask for vote if has not started failover
				if m.getFailOverState() == failOverNone {
					selfID = ""
				}
				reply, err := sentinel.client.IsMasterDownByAddr(IsMasterDownByAddrArgs{
					Name:         masterName,
					IP:           masterIp,
					Port:         masterPort,
					CurrentEpoch: currentEpoch,
					SelfID:       selfID,
				})
				if err != nil {
					s.logger.Errorf("sentinel.client.IsMasterDownByAddr: %s", err)
					//skip for now
				} else {
					sentinel.mu.Lock()
					sentinel.sdown = reply.MasterDown

					if reply.VotedLeaderID != "" {
						if !loggedForNeighbor {
							s.logger.Debugw(logEventNeighborVotedFor,
								"neighbor_id", sentinel.runID,
								"voted_for", reply.VotedLeaderID,
								"epoch", reply.LeaderEpoch,
							)
							loggedForNeighbor = true
						}
						sentinel.leaderEpoch = reply.LeaderEpoch
						sentinel.leaderID = reply.VotedLeaderID
					}

					sentinel.mu.Unlock()
				}
				time.Sleep(1 * time.Second)
			}

		}()
	}
	m.mu.Unlock()

}

func (s *Sentinel) askSentinelsIfMasterIsDown(m *masterInstance) {
	s.mu.Lock()
	currentEpoch := s.currentEpoch
	s.mu.Unlock()

	m.mu.Lock()
	masterName := m.name
	masterIp := m.host
	masterPort := m.port

	for _, sentinel := range m.sentinels {
		go func(sInstance *sentinelInstance) {
			sInstance.mu.Lock()
			lastReply := sInstance.lastMasterDownReply
			sInstance.mu.Unlock()
			if time.Since(lastReply) < 1*time.Second {
				return
			}
			if m.getState() == masterStateSubjDown {
				reply, err := sInstance.client.IsMasterDownByAddr(IsMasterDownByAddrArgs{
					Name:         masterName,
					IP:           masterIp,
					Port:         masterPort,
					CurrentEpoch: currentEpoch,
					SelfID:       "",
				})
				if err != nil {
					//skip for now
				} else {
					sInstance.mu.Lock()
					sInstance.lastMasterDownReply = time.Now()
					sInstance.sdown = reply.MasterDown
					sInstance.mu.Unlock()
				}
			} else {
				return
			}

		}(sentinel)
	}
	m.mu.Unlock()

}

type masterInstance struct {
	sentinelConf       MasterMonitor
	isKilled           bool
	stopSendingRequest bool
	name               string
	mu                 sync.Mutex
	runID              string
	slaves             map[string]*slaveInstance
	promotedSlave      *slaveInstance
	sentinels          map[string]*sentinelInstance
	host               string
	port               string
	shutdownChan       chan struct{}
	client             InternalClient
	// infoClient   internalClient

	state                   masterInstanceState
	subjDownNotify          chan struct{}
	followerNewMasterNotify chan struct{}
	downSince               time.Time

	lastSuccessfulPing time.Time

	failOverState           failOverState
	failOverEpoch           int
	failOverStartTime       time.Time
	failOverStateChangeTime time.Time

	leaderEpoch int    // epoch of current leader, only set during failover
	leaderID    string // current leader id
	// failOverStartTime time.Time

	configEpoch int
}

type failOverState int

var (
	failOverNone               failOverState = 0
	failOverWaitLeaderElection failOverState = 1
	failOverSelectSlave        failOverState = 2
	failOverPromoteSlave       failOverState = 3
	failOverReconfSlave        failOverState = 4
	failOverResetInstance      failOverState = 5
	failOverDone               failOverState = 6
	failOverStateValueMap                    = map[string]failOverState{
		"none":                 failOverNone,
		"wait_leader_election": failOverWaitLeaderElection,
		"select_slave":         failOverSelectSlave,
		"promote_slave":        failOverPromoteSlave,
		"reconfig_slave":       failOverReconfSlave,
		"failover_done":        failOverDone,
		"reset_instance":       failOverResetInstance,
	}
	failOverStateMap = map[failOverState]string{
		0: "none",
		1: "wait_leader_election",
		2: "select_slave",
		3: "promote_slave",
		4: "reconfig_slave",
		5: "reset_instance",
		6: "failover_done",
	}
)

func (s failOverState) String() string {
	return failOverStateMap[s]
}
