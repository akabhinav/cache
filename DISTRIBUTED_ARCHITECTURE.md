# Distributed Architecture

This document explains the distributed coordination mechanisms in the Redis-Java implementation.

## Overview

The system uses a **masterless, peer-to-peer architecture** with the following components:

1. **Gossip Protocol** - For node discovery and failure detection
2. **Leader Election** - For cluster coordination (using Bully Algorithm)
3. **Consistent Hashing** - For data distribution
4. **Async Replication** - For data availability

## Architecture Diagram

```
┌─────────────────────────────────────────────────────────────┐
│                     Redis Cluster                            │
│                                                              │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐ │
│  │   Node 1     │    │   Node 2     │    │   Node 3     │ │
│  │  (Leader)    │    │              │    │              │ │
│  ├──────────────┤    ├──────────────┤    ├──────────────┤ │
│  │ Redis Server │    │ Redis Server │    │ Redis Server │ │
│  │   :6379      │    │   :6380      │    │   :6381      │ │
│  ├──────────────┤    ├──────────────┤    ├──────────────┤ │
│  │   Gossip     │◄──►│   Gossip     │◄──►│   Gossip     │ │
│  │   :7379      │    │   :7380      │    │   :7381      │ │
│  ├──────────────┤    ├──────────────┤    ├──────────────┤ │
│  │  DataStore   │    │  DataStore   │    │  DataStore   │ │
│  │ + Replication│    │ + Replication│    │ + Replication│ │
│  └──────────────┘    └──────────────┘    └──────────────┘ │
│         │                    │                    │         │
│         └────────────────────┴────────────────────┘         │
│                    Consistent Hash Ring                      │
└─────────────────────────────────────────────────────────────┘
```

## 1. Gossip Protocol

### Purpose
- **Node Discovery**: Automatically detect new nodes joining the cluster
- **Failure Detection**: Identify nodes that have crashed or become unreachable
- **State Dissemination**: Spread cluster state information across all nodes

### How It Works

```
Every 1 second:
  1. Node selects 3 random peers
  2. Sends heartbeat with:
     - Own node info
     - Known node states
     - Timestamp
  3. Peers merge the state information
  4. Eventually, all nodes have consistent view

Failure Detection:
  - If no heartbeat for 3s → Mark as SUSPECTED
  - If no heartbeat for 5s → Mark as DEAD
  - Dead nodes are removed from cluster
```

### Message Types

- **HEARTBEAT**: Periodic "I'm alive" message with node states
- **JOIN_REQUEST**: New node wants to join
- **JOIN_RESPONSE**: Existing node sends cluster state to new node
- **NODE_UPDATE**: State change notification

### Implementation
See `GossipProtocol.java` - Uses UDP for efficient multicast communication

## 2. Leader Election (Bully Algorithm)

### Purpose
- **Coordination**: One node coordinates cluster-wide operations
- **Data Migration**: Leader assigns migration tasks during rebalancing
- **Cluster State**: Leader maintains authoritative cluster state

### Why Do We Need a Leader?

Even in a masterless architecture, we need coordination for:
- ✅ Coordinating data rebalancing when nodes join/leave
- ✅ Resolving conflicts in cluster state
- ✅ Triggering data synchronization
- ✅ Cluster-wide operations (e.g., resharding)

**Note**: The leader is NOT a single point of failure - it's just for coordination. Data operations work without the leader.

### How Bully Algorithm Works

```
1. Each node has a unique ID (lexicographically ordered)

2. When leader is suspected dead:
   - Node starts election
   - Sends ELECTION message to all nodes with higher IDs

3. If a higher ID node responds:
   - Give up and wait for that node to become leader

4. If no higher ID responds after timeout:
   - Declare self as leader
   - Announce leadership to cluster

5. Highest ID node always becomes leader

Example:
  Nodes: A, B, C (IDs in order)
  - C is always the leader
  - If C dies, B becomes leader
  - If C comes back, it challenges B and becomes leader again
```

### Leadership Responsibilities

```java
if (isLeader()) {
    // Coordinate cluster rebalancing
    replicationManager.synchronizeCluster();

    // Send periodic "I am leader" heartbeats
    announceLeadership();

    // Handle node join/leave events
    onNodeJoined(node) -> triggerRebalancing();
}
```

### Implementation
See `LeaderElection.java`

## 3. Consistent Hashing

### Purpose
- **Data Distribution**: Evenly distribute keys across nodes
- **Minimal Movement**: When nodes join/leave, only nearby keys move

### How It Works

```
1. Hash Ring: 2^64 positions (0 to Long.MAX_VALUE)

2. Virtual Nodes: Each physical node has 150 virtual nodes
   - Physical Node A → Virtual: A-1, A-2, ..., A-150
   - Spreads load more evenly

3. Key Placement:
   hash(key) → position on ring
   → Walk clockwise to first virtual node
   → That physical node owns the key

4. Replication:
   Continue walking clockwise for N more nodes
   → Those nodes are replicas
```

### Example

```
Ring positions: 0 ────────► 100 ────────► 200 ────────► 300 ─┐
                            ↑              ↑              ↑    │
                          Node A       Node B        Node C   │
                                                                │
                                                                └──► back to 0

Key "user:123" → hash = 150
  → Closest node clockwise = Node B (at 200)
  → Primary: Node B
  → Replica 1: Node C (at 300)
  → Replica 2: Node A (at 100, wrapping around)
```

### Benefits
- ✅ Only ~1/N of keys move when node joins/leaves (N = number of nodes)
- ✅ Virtual nodes ensure even distribution
- ✅ Deterministic - all nodes agree on key location

### Implementation
See `ConsistentHash.java`

## 4. Data Replication

### Purpose
- **Availability**: Data survives node failures
- **Load Distribution**: Reads can be served from replicas

### Strategy

```
Write Flow:
  1. Client writes to any node
  2. Node checks: "Am I primary for this key?"
     - If yes: Accept write
     - If no: Proxy to primary node (or reject)
  3. Primary writes to local storage
  4. Primary queues replication to N-1 replicas
  5. Async replication happens in background
  6. Return success to client (async replication)

Read Flow:
  1. Client reads from any node
  2. If node has the key (primary or replica): Return value
  3. Otherwise: Proxy to primary or return NOT FOUND
```

### Replication Factor

Configurable (default = 3):
- **Factor 1**: No replication (data loss on node failure)
- **Factor 2**: One backup copy
- **Factor 3**: Two backup copies (recommended)
- **Factor N**: N-1 backup copies

### Consistency Model

**Eventual Consistency**:
- Writes return immediately after primary write
- Replicas updated asynchronously
- Short window where replicas may be stale
- Eventually all replicas converge

### Implementation
See `ReplicationManager.java`

## 5. Cluster Operations

### Node Join

```
1. New node starts with seed node addresses
2. Sends JOIN_REQUEST to seed nodes
3. Receives JOIN_RESPONSE with cluster state
4. Gossip protocol spreads the news
5. All nodes add new node to consistent hash ring
6. Leader triggers data rebalancing
7. Keys now owned by new node are migrated
```

### Node Leave (Graceful)

```
1. Node announces LEAVING state via gossip
2. Leader coordinates data migration
3. Node's keys transferred to next nodes on ring
4. After migration complete, node shuts down
5. Other nodes remove it from hash ring
```

### Node Failure (Crash)

```
1. Nodes detect via missed heartbeats
2. After timeout, mark as DEAD
3. Remove from hash ring
4. Reads/writes automatically route to next node
5. Leader may trigger re-replication to restore replication factor
```

## 6. Split-Brain Handling

### Problem
Network partition splits cluster into two groups:
```
Partition 1: [Node A, Node B] - Leader A
Partition 2: [Node C, Node D] - Leader C

Both groups accept writes → Data diverges!
```

### Solution: Quorum-based writes (Future Enhancement)

```
Majority Quorum:
  - Cluster of N nodes needs N/2 + 1 for writes
  - 3 nodes → need 2
  - 5 nodes → need 3

Partition 1: 2 nodes → Can't reach quorum (2 < 3) → Read-only
Partition 2: 3 nodes → Has quorum → Accepts writes

When partition heals:
  - Minority partition's changes discarded or merged
```

## 7. Configuration Example

### Starting a 3-Node Cluster

**Node 1 (Seed)**:
```bash
java -jar redis-java.jar \
  --port 6379 \
  --gossip-port 7379 \
  --node-id node-1
```

**Node 2**:
```bash
java -jar redis-java.jar \
  --port 6380 \
  --gossip-port 7380 \
  --node-id node-2 \
  --seed localhost:7379:node-1
```

**Node 3**:
```bash
java -jar redis-java.jar \
  --port 6381 \
  --gossip-port 7381 \
  --node-id node-3 \
  --seed localhost:7379:node-1
```

### Programmatic Setup

```java
// Node 1
DistributedRedisServer node1 = new DistributedRedisServer.Builder()
    .port(6379)
    .gossipPort(7379)
    .nodeId("node-1")
    .replicationFactor(3)
    .build();
node1.start();

// Node 2
DistributedRedisServer node2 = new DistributedRedisServer.Builder()
    .port(6380)
    .gossipPort(7380)
    .nodeId("node-2")
    .addSeedNode("localhost", 7379, "node-1")
    .replicationFactor(3)
    .build();
node2.start();

// Node 3
DistributedRedisServer node3 = new DistributedRedisServer.Builder()
    .port(6381)
    .gossipPort(7381)
    .nodeId("node-3")
    .addSeedNode("localhost", 7379, "node-1")
    .replicationFactor(3)
    .build();
node3.start();
```

## 8. Comparison with Other Systems

### vs Redis Cluster
| Feature | Redis Cluster | This Implementation |
|---------|---------------|---------------------|
| Architecture | Master-Slave sharding | Peer-to-peer with leader |
| Coordination | Gossip + Sentinel | Gossip + Bully election |
| Consistency | Strong (within shard) | Eventual (async replication) |
| Failover | Automatic promotion | Automatic re-election |

### vs Cassandra
| Feature | Cassandra | This Implementation |
|---------|-----------|---------------------|
| Discovery | Gossip | Gossip |
| Leader | No leader | Bully algorithm |
| Replication | Configurable factor | Configurable factor |
| Consistency | Tunable quorum | Eventual (future: quorum) |

## 9. Trade-offs

### Pros
✅ **No SPOF**: Masterless design, leader just for coordination
✅ **Simple**: Easier to understand than Raft/Paxos
✅ **Fast**: Async replication for low latency writes
✅ **Scalable**: Add nodes without downtime

### Cons
❌ **Eventual consistency**: Short window of stale reads
❌ **Split-brain**: Needs quorum for strong consistency (not yet implemented)
❌ **Network overhead**: Gossip messages create background traffic

## 10. Monitoring

### Key Metrics

```java
// Cluster health
int aliveNodes = gossip.getAliveNodes().size();
ClusterNode leader = leaderElection.getLeader();

// Replication lag
ReplicationStats stats = replicationManager.getStats();
int queueSize = stats.queueSize(); // Should be near 0

// Key distribution
for (String key : importantKeys) {
    ClusterNode primary = cluster.getNodeForKey(key);
    List<ClusterNode> replicas = cluster.getNodesForKey(key);
}
```

## Summary

The coordination architecture follows these principles:

1. **Gossip** discovers nodes and detects failures (UDP, 1s intervals)
2. **Leader** coordinates cluster operations (Bully algorithm, highest ID wins)
3. **Consistent Hashing** distributes data with minimal movement
4. **Replication** provides availability (async, configurable factor)

This creates a robust, self-healing distributed cache system that's:
- Resilient to node failures
- Automatically rebalances data
- Requires no external coordination service (ZooKeeper, etcd)
- Simple to deploy and operate
