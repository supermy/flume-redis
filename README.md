flume-redis
===========

将采集到数据通过 Redis Lua 进行 ETL,千亿级的数据进行统计与抽取进行毫秒级的实时处理。

使用 Flume Filter 拦截器 构造Redis Lus 脚本

    Gson gson = new Gson();
    
    Map full= new HashMap();
    full.put("script","return redis.call('ZADD',KEYS[2],KEYS[1],KEYS[3])");
    full.put("args",new ArrayList());
    full.put("keys",split);
    
    String json = gson.toJson(full);   

支持 redis-lua 脚本以及参数传递

    a1.sinks.k1.type = com.supermy.redis.flume.redis.sink.RedisEVALSink
    a1.sinks.k1.host = 132.194.43.153
    a1.sinks.k1.key = jplist
    a1.sinks.k1.batch_size = 10000


License
-------



Example configuration
---------------------

Example source configuration:

    agent.sources.redisSource.type = com.supermy.redis.flume.redis.source.RedisSource
    agent.sources.redisSource.host = localhost
    agent.sources.redisSource.key = logstash
    agent.sources.redisSource.batch_size = 500
    agent.sources.redisSource.serializer = com.supermy.redis.flume.redis.source.serializer.LogstashDeSerializer


Example sink configuration:

    agent.sinks.redisSink.type = com.supermy.redis.flume.redis.sink.RedisSink
    agent.sinks.redisSink.host = localhost
    agent.sinls.redisSink.key = logstash
    agent.sinks.redisSink.batch_size = 500
    agent.sinks.redisSink.serializer = com.supermy.redis.flume.redis.sink.serializer.LogstashSerializer



Building
--------

This project uses maven for building all the artefacts.
You can build it with the following command:
    mvn clean install

This will build the following artefacts:
* flume-redis-dist/target/flume-redis-1.0.0-SNAPSHOT-dist.tar.gz
  The tarball can be directly unpacked into Apache Flume plugins.d directory

* flume-redis-dist/target/rpm/tango-flume-redis/RPMS/noarch/tango-flume-redis-1.0.0-SNAPSHOT*.noarch.rpm
  This package will install itself on top of Apache Flume package and be ready for use right away.



 