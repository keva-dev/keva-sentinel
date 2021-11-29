package sentinel

import (
	"fmt"
	"time"

	"github.com/stretchr/testify/assert"
	"go.uber.org/zap/zaptest/observer"
)

func (suite *testSuite) handleLogEventSentinelVotedFor(instanceIdx int, log observer.LoggedEntry) {
	ctxMap := log.ContextMap()

	votedFor := ctxMap["voted_for"].(string)
	term := int(ctxMap["epoch"].(int64))
	currentInstanceID := suite.mapIdxtoRunID[instanceIdx]

	suite.mu.Lock()
	defer suite.mu.Unlock()
	_, exist := suite.termsVote[term]
	if !exist {
		suite.termsVote[term] = make([]termInfo, len(suite.instances))
	}
	termInfo := suite.termsVote[term][instanceIdx]
	if termInfo.selfVote != "" {
		if termInfo.selfVote != votedFor {
			suite.t.Fatalf("instance %s voted for multiple instances (%s and %s) in the same term %d",
				currentInstanceID, termInfo.selfVote, votedFor, term)
		}
	}
	termInfo.selfVote = votedFor
	if termInfo.neighborVotes == nil {
		termInfo.neighborVotes = map[string]string{}
	}
	suite.termsVote[term][instanceIdx] = termInfo
}

func (suite *testSuite) handleLogEventSlaveInstanceCreated(instanceIdx int, log observer.LoggedEntry) {
	ctxMap := log.ContextMap()
	// TODO: add master name when sentinel can handle multile masters masterName := ctxMap["master_name"].(string)
	address := ctxMap["address"].(string)
	masterID := ctxMap["master_id"].(string)
	sentinelID := ctxMap["sentinel_run_id"].(string)
	suite.mu.Lock()
	defer suite.mu.Unlock()
	masterslaveMap, ok := suite.instancesMasterSlaveMap[sentinelID]
	if !ok {
		masterslaveMap = map[string][]string{}
	}
	masterslaveMap[masterID] = append(masterslaveMap[masterID], address)
	suite.instancesMasterSlaveMap[sentinelID] = masterslaveMap
}

func (suite *testSuite) handleLogEventMasterInstanceCreated(instanceIdx int, log observer.LoggedEntry) {
	ctxMap := log.ContextMap()
	// TODO: add master name when sentinel can handle multile masters masterName := ctxMap["master_name"].(string)
	runID := ctxMap["run_id"].(string)
	term := int(ctxMap["epoch"].(int64))
	sentinelRunID := ctxMap["sentinel_run_id"].(string)
	suite.mu.Lock()
	defer suite.mu.Unlock()
	previousMasterID, exist := suite.termsMasterID[term]
	if exist {
		if previousMasterID != runID {
			assert.Failf(suite.t, "conflict master id per term", "term %d has multiple master ID: %s and %s", previousMasterID,
				runID)

			return
		}
	}
	suite.termsMasterID[term] = runID
	suite.termsMasterInstanceCreation[term] = append(suite.termsMasterInstanceCreation[term], sentinelRunID)
}

func (suite *testSuite) handleLogEventSlavePromoted(instanceIdx int, log observer.LoggedEntry) {
	ctxMap := log.ContextMap()
	promotedSlave := ctxMap["run_id"].(string)
	term := int(ctxMap["epoch"].(int64))
	suite.mu.Lock()
	defer suite.mu.Unlock()
	previousSelected, exist := suite.termsSelectedSlave[term]
	if !exist {
		assert.Failf(suite.t, "slave promoted without selection", "term %d has slave promoted without selected", term)
		return
	}
	if previousSelected != promotedSlave {
		assert.Failf(suite.t, "slave promoted different from slave selected", "term %d has slave promoted %d different from slave selected", term, previousSelected,
			promotedSlave)
	}
	suite.termsPromotedSlave[term] = promotedSlave
}

func (suite *testSuite) handleLogEventSelectedSlave(instanceIdx int, log observer.LoggedEntry) {
	ctxMap := log.ContextMap()
	selectedSlave := ctxMap["slave_id"].(string)
	term := int(ctxMap["epoch"].(int64))
	suite.mu.Lock()
	defer suite.mu.Unlock()
	previousSelected, exist := suite.termsSelectedSlave[term]
	if exist {
		assert.Failf(suite.t, "multiple slave selected per term", "term %d has multiple selected slave recognition event, previously is %s and current is %s",
			term, previousSelected, selectedSlave)
	}
	suite.termsSelectedSlave[term] = selectedSlave
	suite.currentLeader = selectedSlave
}

func (suite *testSuite) handleLogEventRequestingElection(instanceIdx int, log observer.LoggedEntry) {
	ctxMap := log.ContextMap()
	runid := ctxMap["sentinel_run_id"].(string)
	term := int(ctxMap["epoch"].(int64))
	suite.mu.Lock()
	defer suite.mu.Unlock()
	suite.termsVoters[term] = append(suite.termsVoters[term], runid)
}

func (suite *testSuite) handleLogEventBecameTermLeader(instanceIdx int, log observer.LoggedEntry) {
	ctxMap := log.ContextMap()
	leader := ctxMap["run_id"].(string)
	term := int(ctxMap["epoch"].(int64))
	suite.mu.Lock()
	defer suite.mu.Unlock()
	previousLeader, exist := suite.termsLeader[term]
	if exist {
		suite.t.Fatalf("term %d has multiple leader recognition event, previously is %s and current is %s",
			term, previousLeader, leader)
	}
	suite.termsLeader[term] = leader
	suite.currentLeader = leader
	suite.currentTerm = term
}

func (suite *testSuite) handleLogEventFailoverStateChanged(instanceIdx int, log observer.LoggedEntry) {
	ctxMap := log.ContextMap()
	newStateStr := ctxMap["new_state"].(string)
	newState, ok := failOverStateValueMap[newStateStr]

	if !ok {
		panic(fmt.Sprintf("unknown value for failover state: %s", newStateStr))
	}

	suite.mu.Lock()
	defer suite.mu.Unlock()
	oldState := suite.failOverStates[instanceIdx]
	switch newState {
	case failOverWaitLeaderElection:
		fallthrough
	case failOverSelectSlave:
		fallthrough
	case failOverPromoteSlave:
		fallthrough
	case failOverReconfSlave:
		fallthrough
	case failOverResetInstance:
		if newState-oldState != 1 {
			assert.Failf(suite.t, "log consume error", "invalid failover state transition from %s to %s", oldState, newState)
		}
	case failOverNone:
		if oldState != failOverWaitLeaderElection && oldState != failOverSelectSlave {
			assert.Failf(suite.t, "log consume error", "invalid failover state transition from %s to %s", oldState, newState)
		}
	case failOverDone:
		if oldState != failOverResetInstance &&
			oldState != failOverWaitLeaderElection &&
			oldState != failOverNone {
			assert.Failf(suite.t, "log consume error", "invalid failover state transition from %s to %s", oldState, newState)
		}
	default:
		assert.Failf(suite.t, "log consume error", "invalid failover state: %d", newState)
	}
	suite.failOverStates[instanceIdx] = newState
}

func (suite *testSuite) handleLogEventNeighborVotedFor(instanceIdx int, log observer.LoggedEntry) {
	ctxMap := log.ContextMap()
	term := int(ctxMap["epoch"].(int64))
	neighborID := ctxMap["neighbor_id"].(string)
	votedFor := ctxMap["voted_for"].(string)

	suite.mu.Lock()
	defer suite.mu.Unlock()
	_, exist := suite.termsVote[term]
	if !exist {
		suite.termsVote[term] = make([]termInfo, len(suite.instances))
	}
	termInfo := suite.termsVote[term][instanceIdx]

	if termInfo.neighborVotes == nil {
		termInfo.neighborVotes = map[string]string{}
	}
	previousRecordedVote := termInfo.neighborVotes[neighborID]

	// already record this neighbor vote before, check if it is consistent
	if previousRecordedVote != "" {
		if previousRecordedVote != votedFor {
			suite.t.Fatalf("neighbor %s is recorded to voted for different leaders (%s and %s) in the same term %d",
				neighborID, previousRecordedVote, votedFor, term,
			)
		}
	}
	termInfo.neighborVotes[neighborID] = votedFor
	suite.termsVote[term][instanceIdx] = termInfo
}

func (s *testSuite) consumeLogs(instanceIdx int, observer *observer.ObservedLogs) {
	for {
		logs := observer.TakeAll()
		for _, entry := range logs {
			switch entry.Message {
			case logEventRequestingElection:
				s.handleLogEventRequestingElection(instanceIdx, entry)
			case logEventBecameTermLeader:
				s.handleLogEventBecameTermLeader(instanceIdx, entry)
			case logEventVotedFor:
				s.handleLogEventSentinelVotedFor(instanceIdx, entry)
			case logEventNeighborVotedFor:
				s.handleLogEventNeighborVotedFor(instanceIdx, entry)
			case logEventFailoverStateChanged:
				s.handleLogEventFailoverStateChanged(instanceIdx, entry)
			case logEventSelectedSlave:
				s.handleLogEventSelectedSlave(instanceIdx, entry)
			case logEventSlavePromoted:
				s.handleLogEventSlavePromoted(instanceIdx, entry)
			case logEventMasterInstanceCreated:
				s.handleLogEventMasterInstanceCreated(instanceIdx, entry)
			case logEventSlaveInstanceCreated:
				s.handleLogEventSlaveInstanceCreated(instanceIdx, entry)
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	// all events that may change state of this suite from log stream appear here
}
