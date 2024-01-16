# influxdb-rs
InfluxDB compatible project

## tsdb storage layer
https://miseqxhpf9.feishu.cn/docx/DnpmdedZUossDKxycL2csnEbn4e

## Optimize

1. 对于非活动状态的shard， 倒排索引不会compaction log部分，并且log会有对应的内存结构，导致内存不会释放
2. 倒排索引有的LRU缓存，命中率低，且采用db粒度全局锁
3. series 正排索引迁移到shard内，解决series不能过期，耗尽磁盘，不适合长期运行
4. series字典化，可以减少 wal、tsm、cache的空间占用
5. 解决influxdb 宕机后，一直重启oom问题，本身是golang gc问题，但也说明influxdb在内存使用上不严谨

## Design defect

1. MMAP是一个速成的数据访问方案，简化了缓存设计，但其io阻塞golang运行时，且在io引发的load飚高问题上排查问题困难
2. tsm index部分启动需要读取加载，而且读取的index数据是不连续，导致随机读，启动时间非常慢
3. series和index都采用rhh方式存储，而且是采用mmap映射，大量随机查询导致mmap缓存命中率很差， load飚高
4. influxdb的倒排索引有个LRU cache，是全局锁，性能非常差，反向优化案例
5. series和revert index明显是感觉是后面加上的，没有crc校验，在节点宕机时，可能出现文件不完整，无法重启，需要手工修复
6. series wal & index wal & tsm wal 没法做到事务性，容易造成文件损坏
    1. 比如写完series wal，然后获取series的id去构建index ，结果crash，series wal损坏，不能采用截断方式处理

## Why reimplement InfluxDB

1. review代码不一定得到事实真像，rewrite深刻了解设计细节
2. 深度体验与流式数据库融合的