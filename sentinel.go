package sentinel

import (
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/spf13/viper"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func newLogger() *zap.SugaredLogger {
	config := zap.NewDevelopmentConfig()
	config.EncoderConfig.EncodeLevel = zapcore.CapitalColorLevelEncoder
	logger, err := config.Build()
	if err != nil {
		panic(err)
	}
	return logger.Sugar()
}

type Config struct {
	MyID         string          `mapstructure:"my_id"`
	Binds        []string        `mapstructure:"binds"` //TODO: understand how to bind multiple network interfaces
	Port         string          `mapstructure:"port"`
	Masters      []MasterMonitor `mapstructure:"masters"`
	CurrentEpoch int             `mapstructure:"current_epoch"`
}

type MasterMonitor struct {
	RunID                string         `mapstructure:"run_id"`
	Name                 string         `mapstructure:"name"`
	Addr                 string         `mapstructure:"addr"`
	Quorum               int            `mapstructure:"quorum"`
	DownAfter            time.Duration  `mapstructure:"down_after"`
	FailoverTimeout      time.Duration  `mapstructure:"failover_timeout"`
	ReconfigSlaveTimeout time.Duration  `mapstructure:"reconfig_slave_timeout"`
	ConfigEpoch          int            `mapstructure:"config_epoch"` //epoch of master received from hello message
	LeaderEpoch          int            `mapstructure:"config_epoch"` //last leader epoch
	KnownReplicas        []KnownReplica `mapstructure:"known_replicas"`
	ParallelSync         int            `mapstructure:"parallel_sync"`
}
type KnownSentinel struct {
	ID   string
	Addr string
}

type KnownReplica struct {
	Addr string
}

type Sentinel struct {
	protoMutex *sync.Mutex
	tcpDone    bool

	mu              *sync.Mutex
	quorum          int
	conf            Config
	masterInstances map[string]*masterInstance //key by address ip:port
	currentEpoch    int
	runID           string

	//given a preassigned slaveInstance struct, assign missing fields to make it complete
	// - create client from given address, for example
	// slaveFactory func(*slaveInstance) error

	clientFactory func(string) (InternalClient, error)
	listener      net.Listener
	logger        *zap.SugaredLogger
}

func defaultSlaveFactory(sl *slaveInstance) error {
	client, err := newInternalClient(sl.addr)
	if err != nil {
		return err
	}
	sl.client = client
	return nil
}

func newInternalClient(addr string) (InternalClient, error) {
	return newRedisClient(addr), nil
}

func NewFromConfig(conf Config) (*Sentinel, error) {
	if conf.MyID == "" {
		conf.MyID = uuid.NewString()
	}
	return &Sentinel{
		runID:           conf.MyID,
		conf:            conf,
		mu:              &sync.Mutex{},
		protoMutex:      &sync.Mutex{},
		clientFactory:   newInternalClient,
		masterInstances: map[string]*masterInstance{},
		logger:          newLogger(),
	}, nil
}

func NewFromConfigFile(filepath string) (*Sentinel, error) {
	viper.SetConfigType("yaml")
	viper.SetConfigFile(filepath)
	err := viper.ReadInConfig()
	if err != nil {
		return nil, err
	}
	var conf Config
	err = viper.Unmarshal(&conf)
	if err != nil {
		return nil, err
	}
	return NewFromConfig(conf)
}

func (s *Sentinel) Start() error {
	if len(s.conf.Masters) != 1 {
		return fmt.Errorf("only support monitoring 1 master for now")
	}
	m := s.conf.Masters[0]
	parts := strings.Split(m.Addr, ":")
	masterIP, masterPort := parts[0], parts[1]
	cl, err := s.clientFactory(m.Addr)
	if err != nil {
		return err
	}

	// cl2, err := kevago.NewInternalClient(m.Addr)
	if err != nil {
		return err
	}

	//read master from config, contact that master to get its slave, then contact it slave and sta
	infoStr, err := cl.Info()
	if err != nil {
		return err
	}
	s.mu.Lock()
	master := &masterInstance{
		runID:                   m.RunID,
		sentinelConf:            m,
		name:                    m.Name,
		host:                    masterIP,
		port:                    masterPort,
		configEpoch:             m.ConfigEpoch,
		mu:                      sync.Mutex{},
		client:                  cl,
		slaves:                  map[string]*slaveInstance{},
		sentinels:               map[string]*sentinelInstance{},
		state:                   masterStateUp,
		lastSuccessfulPing:      time.Now(),
		subjDownNotify:          make(chan struct{}),
		followerNewMasterNotify: make(chan struct{}, 1),
	}
	s.masterInstances[m.Addr] = master
	s.mu.Unlock()
	switchedRole, err := s.parseInfoMaster(m.Addr, infoStr)
	if err != nil {
		return err
	}
	if switchedRole {
		return fmt.Errorf("reported address of master %s is not currently in master role", m.Name)
	}

	go s.serveTCP()
	go s.masterRoutine(master)
	return nil
}

func (s *Sentinel) Shutdown() {
	locked(s.protoMutex, func() {
		if s.listener != nil {
			s.listener.Close()
		}
		s.tcpDone = true
	})
}

type sentinelInstance struct {
	runID string
	mu    sync.Mutex
	// masterDown          bool
	client              sentinelClient
	lastMasterDownReply time.Time

	// these 2 must alwasy change together
	leaderEpoch int
	leaderID    string

	sdown bool
}

type masterInstanceState int

var (
	masterStateUp       masterInstanceState = 1
	masterStateSubjDown masterInstanceState = 2
	masterStateObjDown  masterInstanceState = 3
)

type slaveInstance struct {
	runID           string
	killed          bool
	mu              sync.Mutex
	masterDownSince time.Duration
	masterHost      string
	masterPort      string
	masterUp        bool

	host string
	port string
	addr string

	slavePriority  int //TODO
	replOffset     int
	reportedMaster *masterInstance
	sDown          bool

	lastSucessfulPingAt time.Time
	lastSucessfulInfoAt time.Time

	masterRoleSwitchChan chan struct{}
	//notify goroutines that master is down, to change info interval from 10 to 1s like Redis
	masterDownNotify chan struct{}

	client InternalClient

	reconfigFlag int
}

const (
	reconfigSent = 1 << iota
	reconfigInProgress
	reconfigDone
)

type instanceRole int

var (
	instanceRoleMaster instanceRole = 1
	instanceRoleSlave  instanceRole = 2
)

type sentinelClient interface {
	IsMasterDownByAddr(IsMasterDownByAddrArgs) (IsMasterDownByAddrReply, error)
}

type IsMasterDownByAddrArgs struct {
	Name         string
	IP           string
	Port         string
	CurrentEpoch int
	SelfID       string
}

func (req *IsMasterDownByAddrArgs) decode(parts [][]byte) error {
	if len(parts) != 5 {
		return fmt.Errorf("invalid arg")
	}
	req.Name, req.IP, req.Port, req.SelfID = string(parts[0]), string(parts[1]), string(parts[2]), string(parts[4])
	intepoch, err := strconv.Atoi(string(parts[3]))
	if err != nil {
		return err
	}
	req.CurrentEpoch = intepoch
	return nil
}

type IsMasterDownByAddrReply struct {
	MasterDown    bool
	VotedLeaderID string
	LeaderEpoch   int
}

func (res *IsMasterDownByAddrReply) decode(parts []string) error {
	if len(parts) != 3 {
		return fmt.Errorf("invalid arg")
	}
	res.MasterDown = false
	if parts[0] == "1" {
		res.MasterDown = true
	}
	leaderEpoch, err := strconv.Atoi(parts[2])
	if err != nil {
		return err
	}
	res.LeaderEpoch = leaderEpoch
	res.VotedLeaderID = parts[1]
	return nil
}

func (res *IsMasterDownByAddrReply) toLines() []string {
	down := "0"
	if res.MasterDown {
		down = "1"
	}
	return []string{
		down,
		res.VotedLeaderID,
		strconv.Itoa(res.LeaderEpoch),
	}

}

const (
	SENTINEL_MAX_DESYNC = 1000
)
