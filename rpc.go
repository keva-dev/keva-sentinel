package sentinel

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"time"
)

// use RPC for simplicity first
type rpcClient struct {
	*rpc.Client
}

// TODO
func newRPCClient(addr string, port string) (*rpcClient, error) {
	client, err := rpc.DialHTTP("tcp", fmt.Sprintf("%s:%s", addr, port))
	if err != nil {
		return nil, err
	}
	return &rpcClient{
		Client: client,
	}, nil
}

func (c *rpcClient) IsMasterDownByAddr(req IsMasterDownByAddrArgs) (IsMasterDownByAddrReply, error) {
	var reply IsMasterDownByAddrReply
	err := c.Client.Call("Sentinel.IsMasterDownByAddr", &req, &reply)
	return reply, err
}

func (s *Sentinel) getCurrentEpoch() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.currentEpoch
}

func (s *Sentinel) updateEpochIfNeeded(newEpoch int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if newEpoch > s.currentEpoch {

		fmt.Printf("updateing sentinel epoch: %s %d\n", s.runID, newEpoch)
		s.currentEpoch = newEpoch
	}
}

func (s *Sentinel) selfID() string {
	return s.runID
}

func (s *Sentinel) voteLeader(m *masterInstance, reqEpoch int, reqRunID string) (leaderEpoch int, leaderID string) {
	s.updateEpochIfNeeded(reqEpoch)
	selfID := s.selfID()

	m.mu.Lock()
	defer m.mu.Unlock()
	var voteGranted bool
	if m.leaderEpoch < reqEpoch {
		m.leaderID = reqRunID
		m.leaderEpoch = reqEpoch

		// failover start at some other sentinel that we have just voted for
		if m.leaderID != selfID {
			m.failOverStartTime = time.Now()
		}
		voteGranted = true
	}

	leaderEpoch = m.leaderEpoch
	leaderID = m.leaderID
	if voteGranted {
		s.logger.Debugw(logEventVotedFor,
			"voted_for", leaderID,
			"epoch", leaderEpoch,
		)
	}
	return
}

func (s *Sentinel) IsMasterDownByAddr(req *IsMasterDownByAddrArgs, reply *IsMasterDownByAddrReply) error {
	addr := fmt.Sprintf("%s:%s", req.IP, req.Port)
	s.mu.Lock()
	master, exist := s.masterInstances[addr]
	s.mu.Unlock()
	if !exist {
		err := fmt.Errorf("master does not exist")
		s.logger.Errorf(err.Error())
		return err
	}
	reply.MasterDown = master.getState() >= masterStateSubjDown

	if req.SelfID != "" {
		leaderEpoch, leaderID := s.voteLeader(master, req.CurrentEpoch, req.SelfID)
		reply.LeaderEpoch = leaderEpoch
		reply.VotedLeaderID = leaderID
	} else {
		// return its current known leader anyway
		master.mu.Lock()
		reply.LeaderEpoch = master.leaderEpoch
		reply.VotedLeaderID = master.leaderID
		master.mu.Unlock()
	}
	return nil
}

func (s *Sentinel) serveRPC() {
	serv := rpc.NewServer()
	serv.Register(s)

	mux := http.NewServeMux()
	mux.Handle(rpc.DefaultRPCPath, serv)
	l, e := net.Listen("tcp", fmt.Sprintf(":%s", s.conf.Port))
	if e != nil {
		log.Fatal("listen error:", e)
	}
	locked(s.mu, func() {
		s.listener = l
	})
	http.Serve(l, mux)
}
