flume-redis
===========

Flume Source and sink for Redis


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



