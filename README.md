flume-redis
===========

Flume Source and sink for Redis

    redis-source [flume-redis2log.conf]消费数据的使用的测试数据
    127.0.0.1:6379> LPUSH jplist '{"message":1}'
    127.0.0.1:6379> LPUSH jplist '{"message":2}'
    127.0.0.1:6379> LPUSH jplist '{"message":2,"tags":["xyz"],"type":"abc"}'
    redis-sink [flume-netcat2redis.conf]生产数据
    telnet 44444  //生成数据
    127.0.0.1:6379>  rpop jplist //消费数据   
    

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



