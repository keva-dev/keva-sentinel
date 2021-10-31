package sentinel

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

func (s *slaveInstance) shutdown() {
	s.mu.Lock()
	s.killed = true
	s.mu.Unlock()
}

func (s *slaveInstance) iskilled() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.killed
}

// This function is called in context current master is subj down only
// Redis version of this function also supports when master is alive, so some logic is missing
func (s *Sentinel) selectSlave(m *masterInstance) *slaveInstance {
	m.mu.Lock()
	defer m.mu.Unlock()
	// TODO
	maxDownTime := time.Since(m.downSince) + m.sentinelConf.DownAfter*10
	qualifiedSlaves := slaveCandidates{}
	for idx := range m.slaves {
		slave := m.slaves[idx]
		locked(&slave.mu, func() {
			if slave.sDown {
				return
			}
			if time.Since(slave.lastSucessfulPingAt) > 5*time.Second {
				return
			}
			if slave.slavePriority == 0 {
				return
			}
			infoValidityTime := 5 * time.Second
			if time.Since(slave.lastSucessfulInfoAt) > infoValidityTime {
				return
			}

			// accept the fact that this slave still does not see master is down somehow
			if slave.masterDownSince > maxDownTime {
				return
			}

			// copy to compare, avoid locking
			qualifiedSlaves = append(qualifiedSlaves, slaveCandidate{
				slavePriority: slave.slavePriority,
				replOffset:    slave.replOffset,
				runID:         slave.runID,
				findBack:      slave,
			})
		})
	}
	if len(qualifiedSlaves) > 0 {
		sort.Sort(qualifiedSlaves)
		chosen := qualifiedSlaves[len(qualifiedSlaves)-1]
		return chosen.findBack
	}
	return nil
}

func (s *Sentinel) slaveInfoRoutine(sl *slaveInstance) {
	infoDelay := time.Duration(10 * time.Second)
	timer := time.NewTimer(infoDelay)
	for !sl.iskilled() {
		info, err := sl.client.Info()
		if err != nil {
			s.logger.Errorf("sl.client.Info: %s", err)
			//TODO continue for now
		} else {
			sl.mu.Lock()
			sl.lastSucessfulInfoAt = time.Now()
			sl.mu.Unlock()
			roleSwitched, err := s.parseInfoSlave(sl.reportedMaster, sl.addr, info)
			if err != nil {
				s.logger.Errorf("parseInfoslave error: %v", err)
				continue
				// continue for now
			}

			// slave change master post failover logic
			// TODO

			if roleSwitched {

				timeout := time.NewTimer(1 * time.Second)
				defer timeout.Stop()
				select {
				case <-timeout.C:
					// treat it as if it is still slave
				case sl.masterRoleSwitchChan <- struct{}{}:
					// master routine is waiting for this signal, send and return, this will become new master
					//TODO: what non-leader sentinel behaves when seeing this
					return
				}
			}
		}

		timer.Reset(infoDelay)
		select {
		case <-sl.masterDownNotify:
			infoDelay = 1 * time.Second
		case <-timer.C:
		}
	}
}
func (s *Sentinel) newSlaveInstance(masterHost, masterPort, host, port string, replOffset int, master *masterInstance) (*slaveInstance, error) {
	newslave := &slaveInstance{
		masterHost:           masterHost,
		masterPort:           masterPort,
		host:                 host,
		port:                 port,
		addr:                 fmt.Sprintf("%s:%s", host, port),
		replOffset:           replOffset,
		reportedMaster:       master,
		masterDownNotify:     make(chan struct{}, 1),
		masterRoleSwitchChan: make(chan struct{}),
	}
	err := s.slaveFactory(newslave)
	if err != nil {
		s.logger.Errorf("s.slaveFactory: %s", err)
		return nil, err
	}
	cl, err := s.clientFactory(fmt.Sprintf("%s:%s", host, port))
	if err != nil {
		return nil, err
	}
	newslave.client = cl
	return newslave, nil
}
func (s *Sentinel) sayHelloRoutineToSlave(sl *slaveInstance, helloChan HelloChan) {
	for !sl.iskilled() {
		time.Sleep(2 * time.Second)
		m := sl.reportedMaster
		var (
			masterName, masterIP, masterPort string
			masterConfigEpoch                int
		)
		locked(&m.mu, func() {
			masterName = m.name
			if m.promotedSlave != nil {
				locked(&m.promotedSlave.mu, func() {
					masterIP = m.promotedSlave.host
					masterPort = m.promotedSlave.port
				})
			} else {
				masterIP = m.host
				masterPort = m.port
			}
			masterConfigEpoch = m.configEpoch
		})

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
 * 5=master_ip,6=master_port,7=master_config_epoch. */
func (s *Sentinel) slaveHelloRoutine(sl *slaveInstance) {
	//TODO: handle connection lost/broken pipe
	helloChan := sl.client.SubscribeHelloChan()
	defer helloChan.Close()

	selfID := s.selfID()
	go s.sayHelloRoutineToSlave(sl, helloChan)
	for !sl.iskilled() {
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
		m := sl.reportedMaster
		m.mu.Lock()
		masterName := m.name
		_, ok := m.sentinels[runid]
		if !ok {
			client, err := newRPCClient(parts[0], parts[1])
			if err != nil {
				s.logger.Errorf("newRPCClient: cannot create new client to other sentinel with info: %s: %s\ndebug: %s", newmsg, err, newmsg)
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

		masterNameInMsg, newHost, newPort := parts[4], parts[5], parts[6]
		if masterName != masterNameInMsg {
			continue
		}

		neighborEpoch, err := strconv.Atoi(parts[3])
		if err != nil {
			continue
		}
		neighborConfigEpoch, err := strconv.Atoi(parts[7])
		if err != nil {
			continue
		}
		s.mu.Lock()
		currentEpoch := s.currentEpoch
		if neighborEpoch > currentEpoch {
			s.currentEpoch = neighborEpoch
			// Not sure if this needs reseting anything in fsm
			//TODO panic("not implemented")
		}
		s.mu.Unlock()
		m.mu.Lock()
		currentConfigEpoch := m.configEpoch
		var switched bool
		if currentConfigEpoch < neighborConfigEpoch {
			m.configEpoch = neighborConfigEpoch
			host, port := m.host, m.port

			if port != newPort || host != newHost {
				promotedSlave, exist := m.slaves[fmt.Sprintf("%s:%s", newHost, newPort)]
				if exist {
					switched = true
					m.promotedSlave = promotedSlave
				}
			}
		}
		m.mu.Unlock()
		if switched {
			select {
			case m.followerNewMasterNotify <- struct{}{}:
			default:
			}
		}
	}
}

func (s *Sentinel) slaveRoutine(sl *slaveInstance) {
	go s.slaveInfoRoutine(sl)
	go s.slaveHelloRoutine(sl)
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for !sl.iskilled() {
		_, err := sl.client.Ping()
		sl.mu.Lock()
		if err != nil {
			if time.Since(sl.lastSucessfulInfoAt) > sl.reportedMaster.sentinelConf.DownAfter {
				sl.sDown = true
			}
		} else {
			sl.lastSucessfulPingAt = time.Now()
		}
		sl.mu.Unlock()
		<-ticker.C
	}
}

type slaveCandidates []slaveCandidate
type slaveCandidate struct {
	slavePriority int
	replOffset    int
	runID         string
	findBack      *slaveInstance
}

func (sl slaveCandidates) Len() int      { return len(sl) }
func (sl slaveCandidates) Swap(i, j int) { sl[i], sl[j] = sl[j], sl[i] }
func (sl slaveCandidates) Less(i, j int) bool {
	sli, slj := sl[i], sl[j]
	if sli.slavePriority != slj.slavePriority {
		return sli.slavePriority-slj.slavePriority < 0
	}
	if sli.replOffset > slj.replOffset {
		return false
	} else if sli.replOffset < slj.replOffset {
		return true
	}
	// equal replication offset, compare lexicongraphically
	cmp := strings.Compare(sli.runID, slj.runID)
	switch cmp {
	case -1:
		return true
	case 0:
		return true
	default:
		return false
	}
}
