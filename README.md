# NonBlockKV
This is an async native LSM tree built in C from a first principles perspective. It focuses on maxiumum throughput with nvme ssd on OLTP write skewed workloads without sacrificing reads. It is under active development.

# note
This project represents a 1 year self directed process learning storage engines from complete scratch. As such, some parts of the code are poor quality or rushed. It has been rewritten several times as my domain knowledge has expanded. Older modules may not reflect best coding practice, and a major refactor is ongoing as I implement research-inspired features.

# 1.0 Roadmap

## ✅ Completed
- [x] LSM-tree core
- [x] Skiplist (memtable implementation)
- [x] Custom memory manager
- [x] Buffer pool manager
- [x] io_uring integration
- [x] Leveled compaction implementation
- [x] Coroutine framework (async runtime)
- [x] Partioned indexes
- [x] Basic compression support
- [x] German String implementation
- [x] Register Blocked Bloom Filters
- [x] CLOCK implementation

## 🚧 In Progress
- [ ] Key value seperation
- [ ] Manifest
- [ ] General refactor
# Planned 
- [ ] Skip-List replacement
- [ ] Row Cache
- [ ] Shared-Nothing multi core overhaul
- [ ] Calvin style distributed transactions + MVCC
- [ ] CLOCK PRO implementation
- [ ] IO_scheduler enchancements and prefetch hints
# Stretch Goals
- [ ] io_uring raw block device + k poll mode
- [ ] Learned indexes implementation


# Core features
Below are some explanations on system engineering choices designed to improve sota. 
### Memory and Buffer Management
The bufferpool manager uses variable sized pages with single pointer dereferences. The leanstore umbra buffer-pool manager uses variable sized pages with size tiering and madvise() for eviction. madvise incurs >1us of latency per page on eviction from syscall without using ExMap kernel module. Additionally, madvise imposes tlb flush which cannot scale on many core cpu. As such, the umbra design enchanced with pinned buddy memory allocator at the cost of allocation fails from discontingious virtual pages. Eviction cost is reduced to 50-100ns per page. The discontingious virtual pages problem could be augmented by representing large memory regions as a linked list of 4kb pages + prefetch during memory ops, but this reduces external library compatiblity and reduces memory operation preformance by 15% on buffer sizes >1MB. I have not decided whether this is worth the risk


Additionally, this design allows for registered buffers with io uring, providing 10-15% increase in 4kb iops 

### IO & Asynchronous Execution
The core I/O is built on a modern, high-performance, asynchronous foundation to maximize throughput and minimize latency. Segemented stackful coroutines are used and incur 10-15ns asymetrical context switch costs. Segmented stack was chosen over stackful to improve memory efficeny, as each stack could now have as little as 64 bytes per coroutine. This comes at the cost of a unique programming style; stack use must be mimimized during yield points. Stackful coroutines require atleast two 4kb virtual pages even when not in use. This memory savings allows for more memory allocation to the buffer-pool, which arguably improves preformance more than saving 5ns on a context switch. A stackful coroutine impelmentation like PhotonLibOS is forced to use madvise or suffer page faults to free pages in high volume coroutine use. 2 millon active coroutines costs just 128 MB with segemented stacks; with stackful coroutines, 25 GB.


Another benefit from asynchronous design is total control over threads and io scheduling. Giving this to the storage engine allows for latency hiding through prefetch directives to the scheduler and ease of control over rate limiting background operations. Combined with collascing contingious io requests, varible sized buffer pool pages, and the skew of random io bandwidth to sequential io bandwidth on modern nvme drives, this design allows for potentially massive preformance boosts over a standard OS scheduler with fixed size pages. This comes with the hard requirement of a blisteringly fast scheduler implementation; ideally one implemented using some sort of SIMD design.


### Key Value Sep
A large problem with lsm trees is write preformance being ruined by write amplification from rewriting keys and values repeatly during compaction. A naive solution is complete seperation of keys and values like studied in WISC KV and implemented in engines like badger. This inlines a value pointer into the key which allows for only keys to be rewritten during compaction. The value is typically stored in a seperate log.

This design incurs point read and scan cost increases by requiring an extra read to obtain the value from the value pointer. It is impossible to hide the latency of the second read. The second read sucks up additional drive bandwidth and scans cannot take advantage of locality. A solution is to differ the treatment of values placement based on size, like is done in parallax or diffKV; store small values inline, medium values in an ordered log, and large values in an unordered log. The ordered log must occasionally be rewritten to account for key reordering during compaction, but this can be offset by waiting until lower levels in the lsm tree. 
These designs offer a balance of reduced write amplification without hurting read preformance. 


# References: 
Several papers serve as inspiration for the design:  https://arxiv.org/abs/2412.03131 https://dl.acm.org/doi/10.1145/3472883.3487012, https://www.vldb.org/pvldb/vol18/p4295-haas.pdf, https://db.in.tum.de/~leis/papers/leanstore.pdf https://www.vldb.org/pvldb/vol16/p2090-haas.pdf https://scholar.harvard.edu/files/stratos/files/monkeykeyvaluestore.pdf