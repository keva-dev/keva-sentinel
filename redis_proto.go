package sentinel

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/tidwall/redcon"
)

func (s *Sentinel) getCurrentEpoch() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.currentEpoch
}

func (s *Sentinel) updateEpochIfNeeded(newEpoch int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if newEpoch > s.currentEpoch {
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

func (s *Sentinel) serveTCP() {
	l, err := net.Listen("tcp", fmt.Sprintf(":%s", s.conf.Port))
	if err != nil {
		panic(err)
	}
	locked(s.protoMutex, func() {
		s.listener = l
	})
	for {
		//TODO check if listener is closed
		conn, err := l.Accept()
		if err != nil {
			var shutdown bool
			locked(s.protoMutex, func() {
				shutdown = s.tcpDone
			})
			if !shutdown {
				s.logger.Errorf("accepting connection: %s", err)
				continue
			}
			return
		}
		go s.handleTCPConn(conn)
	}
}

func (s *Sentinel) handleTCPConn(conn net.Conn) {
	r := redcon.NewReader(conn)
	w := redcon.NewWriter(conn)
	for {
		cmd, err := r.ReadCommand()
		if err != nil {
			if errors.Is(err, io.EOF) {
				conn.Close()
				return
			}
		}
		err = s.processCmd(cmd, w)
		if err != nil {
			w.WriteError(err.Error())
		}
		err = w.Flush()
		if err != nil {
			s.logger.Errorf("flushing input error: %s", err)
		}
		// w.WriteError(err)
		// err = writeRedisOutput(output, w)
		// if err != nil {
		// 	s.logger.Errorf("writing output to connection %s", err)
		// }
	}
}

func writeRedisOutput(output string, w *bufio.Writer) error {
	written, err := w.WriteString(fmt.Sprintf("%s\n", output))
	if written != len(output)+1 {
		return err
	}
	return w.Flush()
}

func writeRedisErr(errstr string, w *bufio.Writer) error {
	len, err := w.WriteString(fmt.Sprintf("-ERR %s\n", errstr))
	if len != 5 {
		return err
	}
	return w.Flush()
}

func (s *Sentinel) processCmd(cmd redcon.Command, w *redcon.Writer) error {
	parts := cmd.Args
	if len(parts) == 0 {
		return fmt.Errorf("invalid input")
	}

	if string(parts[0]) != "sentinel" {
		return fmt.Errorf("only sentinel command supported")
	}
	if len(parts) < 2 {
		return fmt.Errorf("require atleast 2 arguments")
	}
	switch string(parts[1]) {
	case "get-master-addr-by-name":
		if len(parts) < 3 {
			return fmt.Errorf("missing master name")
		}
		ret, err := s.tcpGetMasterAddrByname(string(parts[2]))
		if err != nil {
			return err
		}
		w.WriteString(ret)
		return nil
	case "is-master-down-by-addr":
		if len(parts[2:]) != 5 {
			return fmt.Errorf("is-master-down-by-addr require 5 args")
		}
		lines, err := s.tcpIsMasterDownByAddr(parts[2:])
		if err != nil {
			return err
		}
		w.WriteArray(len(lines))
		for _, item := range lines {
			w.WriteBulkString(item)
		}
		return nil
	default:
		return fmt.Errorf("unknown command %s", string(parts[1]))
	}
}

// used internal only, Redis does not have this
func (s *Sentinel) tcpIsMasterDownByAddr(args [][]byte) ([]string, error) {
	req := IsMasterDownByAddrArgs{}
	err := req.decode(args)
	if err != nil {
		return nil, err
	}
	var reply IsMasterDownByAddrReply
	err = s.IsMasterDownByAddr(&req, &reply)
	if err != nil {
		return nil, err
	}

	return reply.toLines(), nil
}

// Should be understood by redis client
func (s *Sentinel) tcpGetMasterAddrByname(name string) (string, error) {
	s.mu.Lock()
	temp := make([]*masterInstance, 0, len(s.masterInstances))
	count := 0
	for addr := range s.masterInstances {
		temp[count] = s.masterInstances[addr]
		count++
	}
	s.mu.Unlock()
	var (
		host  string
		port  string
		found bool
	)

	for idx := range temp {
		curMaster := temp[idx]
		locked(&curMaster.mu, func() {
			if curMaster.name == name {
				host = curMaster.host
				port = curMaster.port
				found = true
			}
		})
		if found {
			break
		}
	}
	if !found {
		return "_\n", nil
	}
	return fmt.Sprintf("%s\n%s\n", host, port), nil
}

type redisProtoInternalClient struct {
	cl *redis.SentinelClient
}

// TODO
func NewRedisProtoInternalClient(host string, port string) (*redisProtoInternalClient, error) {
	cl := redis.NewSentinelClient(&redis.Options{
		Addr:     fmt.Sprintf("%s:%s", host, port),
		PoolSize: 2,
	})

	return &redisProtoInternalClient{
		cl: cl,
	}, nil
}

func (c *redisProtoInternalClient) IsMasterDownByAddr(req IsMasterDownByAddrArgs) (IsMasterDownByAddrReply, error) {
	var ret IsMasterDownByAddrReply
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	cmd := redis.NewStringSliceCmd(ctx, "sentinel", "is-master-down-by-addr", req.Name, req.IP, req.Port, strconv.Itoa(req.CurrentEpoch), req.SelfID)
	_ = c.cl.Process(ctx, cmd)
	parts, err := cmd.Result()
	if err != nil {
		return ret, err
	}
	if len(parts) == 0 {
		return ret, fmt.Errorf("protocol error: %v", parts)
	}
	err = ret.decode(parts)
	if err != nil {
		return ret, fmt.Errorf("protocol error: %s", err)
	}
	return ret, nil
}
