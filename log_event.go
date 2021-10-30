package sentinel

var (
	logEventRequestingElection    = "sentinel_requesting_leader_election"
	logEventBecameTermLeader      = "sentinel_became_leader"
	logEventNeighborVotedFor      = "neighbor_sentinel_voted_for"
	logEventVotedFor              = "sentinel_voted_for"
	logEventFailoverStateChanged  = "failover_state_change"
	logEventSelectedSlave         = "slave_selected"
	logEventSlavePromoted         = "slave_promoted"
	logEventMasterInstanceCreated = "master_instance_created"
)
