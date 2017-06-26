flume-redis
===========

2017-06-26
----------

通过更改 RedisSource multi 为 pepiline,提升获取数据的效率。

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
 