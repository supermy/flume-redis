flume-redis
===========

2017-07-01
----------

性能优化，集群专用

        RedisClusterZSetSink  redis zset 数据zadd处理
        RedisClusterZrangeByScoreSink  redis zset 数据zrangeByScore查询
        RedisClusterSource  redis list 数据 lrange 消费处理
        RedisClusterEVALSink redis 各种数据支持，通过脚本执行，性能是原来2/3强，依赖 redis 的计算能力。
        

2017-06-28
----------

增加清除过期数据 每次数据 增加清除过期数据；

    Calendar date = Calendar.getInstance();
    date.set(Calendar.DATE, date.get(Calendar.DATE) - 4);
    SimpleDateFormat fmt = new SimpleDateFormat("yyyyMMddHHmmss");
    String remdate=fmt.format(date.getTime());
    
    p.zremrangeByScore(key,"0",remdate);


2017-06-27
----------

优化码表，只保留两天数据；

    看内存占用；
        # Memory
        used_memory:53323763720
        used_memory_human:49.66G
        used_memory_rss:73160110080
        used_memory_rss_human:68.14G
        used_memory_peak:61589164888
        used_memory_peak_human:57.36G
        total_system_memory:135224197120
        total_system_memory_human:125.94G
        used_memory_lua:67584
        used_memory_lua_human:66.00K
        maxmemory:0
        maxmemory_human:0B
        maxmemory_policy:noeviction
        mem_fragmentation_ratio:1.37
        mem_allocator:jemalloc-4.0.3
        
        keys 1*
        // eval "return redis.call('KEYS',ARGV[1])" 0 1*
        // for lua
        // 三天之前的时间
        // ZREMRANGEBYSCORE key 0 3day-before
        //
          for i, v in pairs(keys) do
                    --每个概率区间为奖品概率乘以1000（把三位小数转换为整,小数位只有一位的*10）再乘以剩余奖品数量
                    local count = redis.call('hget', KEYS[2], v) --获取参数数量  v=产品 and 概率
                    totalPro = totalPro + v * count; -- 概率*数量
                    table.insert(proSection, totalPro)
                    table.insert(pp, v)
          end

        
    看查询效率；

2017-06-26
----------

通过更改 RedisSource multi 为 pepiline,提升获取数据的效率。内置支持 json 数据格式。

2017-06-22
----------

增加 RedisZSetSink 命令直接入库，解决 Redis-Lua eval 独占问题；
    
    
                 Pipeline p = jedis.pipelined();
                 byte[][] redisEvents = new byte[batchEvents.size()][];
                 for (byte[] redisEvent : batchEvents) {
 
                     String json = new String(redisEvent);
                     Map m=gson.fromJson(json, HashMap.class);
                     String key =  m.get("key").toString();
                     String score =  m.get("score").toString();
                     String member =  m.get("member").toString();
 
                     p.zadd(key,new Double(score),member);//key is ip 地址，所以这个是不对的。
 
                 }
                  p.sync();

        
                    


2017-06-22
----------

支持 Redis-lua 脚本，通信执行引擎优化；
支持 pipelin 压缩 script 优化处理效率；


2017-06-22
----------

Flume 升级到1.7

Flume-redis-core 升级到1.0.1；
    jedis 升级之后，类包有变动；
    

2017-06-21
----------

Redis-lua 执行通用的命令；
    两个配置参数，一个脚本，一个数据；
    通过脚本定义要自信的 redis 命令；
    通过参数传递所需的数据；
    用 redis lua 执行；    

命令示例：

    eval "return redis.call('ZADD','KEYS[1]',ARGV[1],ARGV[2])" 1 keyset   时间参数  实体 ID
    eval "return redis.call('ZADD','KEYS[1]',ARGV[1],ARGV[2])" 1 keyset   123  u123
    
    为了减少带宽的消耗， Redis 实现了 EVALSHA 命令，它的作用和 EVAL 一样，都用于对脚本求值，但它接受的第一个参数不是脚本，
    而是脚本的 SHA1 校验和(sum)。
    
    zcard 113.232.197.185
    zrange 113.232.197.185 0 -1
    zrange 113.232.197.185 0 -1 withscores
 