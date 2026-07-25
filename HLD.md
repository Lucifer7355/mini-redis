# HLD - Mini Redis

Maine ye project isliye banaya kyuki Redis ka internal design samajhna tha - LRU, TTL, AOF/RDB, replication, sharding. Ye production Redis nahi hai. Same JVM ke andar nodes simulate kiye hain taaki concepts clear rahein.

---

## Problem

Simple sa distributed cache chahiye tha jo:

- get/set fast kare (memory me)
- capacity full hone pe purani keys hataaye (LRU)
- keys expire ho sake (TTL)
- process band hone pe data poora na udd jaye (persistence)
- ek backup node rakhe (leader/follower)
- keys alag-alag nodes pe baant sake (sharding)
- channel pe message bhej sake (pubsub)

---

## Big picture

Client seedha ClusterManager ko bolta hai. Manager key ka hash nikal ke usko sahi shard pe bhej deta hai.

Har shard ek ClusterNode hai. Uske andar:
- LruCache (actual data)
- Persistence (disk)
- PubSub (optional)
- Replication (agar leader hai to followers ko sync)

```
Client
  -> ClusterManager (consistent hash)
       -> shard-1 (leader) ---> follower-1
       -> shard-2 (leader)
       -> shard-3 (leader)
```

Followers write nahi lete. Sirf read + leader se aaye updates.

---

## Components (short)

**LruCache**  
Asli store. LinkedHashMap access-order pe. Capacity cross hui to sabse purani (least recently used) nikal jaati hai. TTL bhi yahi handle hota hai.

**PersistenceManager**  
Do cheezein:
1. Snapshot / RDB style file (`dump.rdb`) - poora dump ek baar me
2. AOF (`appendonly.aof`) - har write ek line me append

Restart pe pehle snapshot load, phir AOF replay.

**ReplicationLeader / Follower**  
Leader pe write hoti hai, phir command followers ko bhejta hai. Naya follower attach hote hi full snapshot milta hai, uske baad incremental SET/SETEX/DEL.

**ConsistentHashRing**  
Key -> node. Virtual nodes use kiye taaki distribution thodi even rahe. Seedha `hash % N` nahi kiya kyuki node add/remove pe almost sab keys shuffle ho jaati.

**ClusterManager**  
Upar wala API. `set/get/delete` ko sahi node tak pahunchata hai.

**PubSubHub**  
Cache se alag. Subscribe/publish. Message store nahi hota, sirf listeners ko milta hai.

---

## Main flows

### Write
1. ClusterManager key se shard nikalta hai
2. Leader cache me set karta hai
3. Agar AOF on hai to disk pe append
4. Followers ko replicate

### Read
Shard locate -> cache.get. Expired/evicted ho to miss.

### Restart
`dump.rdb` load -> `appendonly.aof` replay -> ready

### Follower attach
Leader apna current data bhejta hai + sequence number. Uske baad normal replication stream.

---

## Tradeoffs (jo maine consciously liye)

- AOF har write pe flush - safe but slow. Real Redis me `everysec` wagaira hota hai.
- Replication async feel - leader wait nahi karta follower ack ka (yahan in-process hai).
- Resharding / key migration nahi hai. Naya shard add kiya to nayi keys uspe jaayengi, purani move nahi hongi automatically.
- Network protocol nahi likha (RESP). Focus internals pe rakha.

---

## Scaling mentally kaise sochu

| Need | Kya karna padega |
|---|---|
| Zyada memory / writes | shards badhao |
| Zyada reads | followers lagao |
| Kam data loss | AOF rakho + frequent snapshot |
| Failover | abhi manual - election nahi hai |

---

## Scope se bahar

- Redis wire protocol
- Auto leader election
- Multi-key transactions across shards
- Live resharding

Agar interview me poochhe ki "production me kya missing hai" - yahi bolna.

---

Code padhne ka order: `LruCache` -> persistence -> replication -> hash ring -> `ClusterNode` -> `ClusterManager` -> `Main`.

LLD ke liye dekh: [LLD.md](./LLD.md)
