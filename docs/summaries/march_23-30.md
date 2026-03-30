S3 storage layer stabilization — A big push to get the S3 backend production-ready. Fixes for upload double-writes, temp file races, retry queues,  
  manifest aggregation, cached backend reliability, task monitoring on shutdown, and general correctness across the storage layer (#25–#31, #40, #50).
                                                                                                                                                      
  Live mode hardening — Several fixes around reorg handling (atomic broadcasts, deep reorgs exceeding retention), catchup status tracking, snapshot   
  transaction safety, temp file cleanup, and graceful shutdown (#41, #51, #53, #30).
                                                                                                                                                      
  Error handling & robustness sweep — Replaced panics with proper Result propagation throughout: config parsing, U256 overflows, integer overflow in  
  decoding, channel failures, null address handling, and DB type mapping fixes (#42, #43, #49, #54, #63).
                                                                                                                                                      
  Major code cleanup & deduplication — Substantial refactoring wave to break apart large modules and remove repeated patterns. execution.rs and the   
  eth_calls collector were decomposed into focused submodules. Storage manifests, parquet readers, decoded-data indexing, storage paths, and
  BlockRange were consolidated into shared abstractions. Macros replaced repetitive type extraction code (#36–#39, #58–#62, #64, #65).                
                  
  Clippy & type safety pass — Introduced domain-specific context structs (decoder output, matcher, eth_call, collection pipeline, engine config)      
  across the codebase, replacing loose tuples and ad-hoc groupings (#44–#48).
                                                                                                                                                      
  New features — Three additions to the pipeline: startup validation of handler call dependencies (#55), buffering/replaying factory-skipped event    
  triggers (#56), and propagating reverted eth_call status through the pipeline (#57).
                                                                                                                                                      
  Performance — Three targeted optimizations: sending decoded data and receipt RangeComplete signals to downstream before writing parquet (reducing   
  latency), and pipelining block collector range processing (#67–#69). Also fixed blocking file I/O that was stalling the async runtime (#70).
                                                                                                                                                      
  Docs — Restructured documentation into an overview + features index format (#72).