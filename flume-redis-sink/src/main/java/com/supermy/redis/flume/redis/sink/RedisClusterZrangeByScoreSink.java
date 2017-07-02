/**
 *  Copyright 2014 TangoMe Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package com.supermy.redis.flume.redis.sink;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.gson.Gson;
import com.supermy.redis.flume.redis.core.redis.JedisPoolFactory;
import com.supermy.redis.flume.redis.core.redis.JedisPoolFactoryImpl;
import com.supermy.redis.flume.redis.sink.serializer.RedisSerializerException;
import com.supermy.redis.flume.redis.sink.serializer.Serializer;
import com.yam.redis.JedisClusterPipeline;
import org.apache.commons.lang.StringUtils;
import org.apache.flume.*;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.*;
import redis.clients.jedis.exceptions.JedisConnectionException;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/*
 * Simple sink which read events from a channel and lpush them to redis
 */
public class RedisClusterZrangeByScoreSink extends AbstractSink implements Configurable {

    private static final Logger logger = LoggerFactory.getLogger(RedisClusterZrangeByScoreSink.class);

    /**
     * Configuration attributes
     */
    private String host = null;
    private Integer port = null;
    private Integer timeout = null;
    private String password = null;
    private Integer database = null;
    private byte[] redisKey = null;
    private Integer batchSize = null;
    private Serializer serializer = null;

    private String ports = null;

//    private final JedisPoolFactory jedisPoolFactory;
//    private JedisPool jedisPool = null;
    private Gson gson = null;
    private JedisCluster cluster = null;

    public RedisClusterZrangeByScoreSink() {
//        jedisPoolFactory = new JedisPoolFactoryImpl();
    }

//    @VisibleForTesting
//    public RedisClusterZrangeByScoreSink(JedisPoolFactory _jedisPoolFactory) {
//        if (_jedisPoolFactory == null) {
//            throw new IllegalArgumentException("JedisPoolFactory cannot be null");
//        }
//
//        this.jedisPoolFactory = _jedisPoolFactory;
//    }

    @Override
    public synchronized void start() {
//
//        logger.info("Starting");
//        if (jedisPool != null) {
//            jedisPool.destroy();
//        }
//
//        JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
//        jedisPool = jedisPoolFactory.create(jedisPoolConfig, host, port, timeout, password, database);


        String[] hosts = host.split(";");
        String[] portlist = ports.split(";");
        Set<HostAndPort> jedisClusterNodes = new HashSet<HostAndPort>();

        if (portlist.length>=2){ //支持本机及群多端口
            for (int i = 0; i < portlist.length; i++) {
                jedisClusterNodes.add(new HostAndPort(host,new Integer(portlist[i])));
            }
        }else {
            for (int i = 0; i < hosts.length; i++) {
                jedisClusterNodes.add(new HostAndPort(hosts[i],port));
            }
        }
        // 构造池
        cluster= new JedisCluster(jedisClusterNodes);
        logger.debug(jedisClusterNodes.toString());

        cluster.set("bar","foo");


        super.start();
    }

    @Override
    public synchronized void stop() {
        logger.info("Stoping");

//        if (jedisPool != null) {
//            jedisPool.destroy();
//        }


        //关闭集群链接
        if (cluster != null) {
            try {
                cluster.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        super.stop();
    }

    @Override
    public Status process() throws EventDeliveryException {
        Status status = Status.READY;

//        if (jedisPool == null) {
//            throw new EventDeliveryException("Redis connection not established. Please verify your configuration");
//        }

        //List<byte[]> batchEvents = new ArrayList<byte[]>(batchSize);
        Set<byte[]> batchEvents = new HashSet<byte[]>(batchSize);

        Channel channel = getChannel();
        Transaction txn = channel.getTransaction();
//        Jedis jedis = jedisPool.getResource();

        JedisClusterPipeline jcp=null;

        try {
            txn.begin();

            for (int i = 0; i < batchSize && status != Status.BACKOFF; i++) {
                Event event = channel.take();
                if (event == null) {
                    status = Status.BACKOFF;
                } else {
                    try {
                        batchEvents.add(serializer.serialize(event));
                    } catch (RedisSerializerException e) {
                        logger.error("Could not serialize event " + event, e);
                    }
                }
            }


            jcp = JedisClusterPipeline.pipelined(cluster);
            jcp.refreshCluster();


            /**
             * Only send events if we got any
             */
            if (batchEvents.size() > 0) {
                if (logger.isDebugEnabled()) {
                    logger.debug("Sending " + batchEvents.size() + " events");
                }

//                Pipeline p = jedis.pipelined();


                //硬编码 提高效率
                //eval "local obj=redis.call('ZRANGEBYSCORE',KEYS[1],ARGV[1],ARGV[2],'LIMIT',ARGV[3],ARGV[4]);
                //if obj[1] == nil then return false end;local objlen=string.len(obj[1]);local objend=string.sub(obj[1],objlen-3);if objend=='@End' then local act=string.sub(obj[1],1,objlen-4);local netlogact=KEYS[1]..'|'..ARGV[1]..'|'..act;redis.call('lpush', KEYS[2],netlogact ) ;return netlogact; else return false ; end"
                // 2
                // 113.230.118.55 netlogactlist  20170609190320 +inf 0 1

                byte[][] redisEvents = new byte[batchEvents.size()][];

                Map<String,Response<Set<String>>> responses = new HashMap<String,Response<Set<String>>>(batchEvents.size());

                for (byte[] redisEvent : batchEvents) {

                    String json = new String(redisEvent);
                    Map m=gson.fromJson(json, HashMap.class);
                    String key =  m.get("key").toString();
                    String arg =  m.get("arg").toString();

                    responses.put(key+"|"+arg, jcp.zrangeByScore(key, arg, "+inf", 0, 1));
                }

                jcp.sync();

                Set<String> result = new HashSet<String>();

                for(String k : responses.keySet()) {
                    Set<String> lines = responses.get(k).get();
                    for (String line:lines) {
                        //返回的数据进行逻辑处理  一般只有一行
                        if(!StringUtils.isEmpty(line)){
                            if(line.endsWith("@End")){

                                StringBuffer sb = new StringBuffer();
                                sb.append(k).append("|");
                                sb.append(line.replace("@End",""));

                                result.add(sb.toString());

                                //p.lpush("netlogacts",sb.toString());

                            }
                        }
                    }
                }


                String[] desc = new String[result.size()];
                result.toArray(desc);
                jcp.lpush("netlogacts",desc);

                jcp.sync();



            }

            txn.commit();
        } catch (JedisConnectionException e) {
            txn.rollback();
//            jedisPool.returnBrokenResource(jedis);
            logger.error("Error while shipping events to redis", e);
        } catch (Throwable t) {
            txn.rollback();
            logger.error("Unexpected error", t);
        } finally {
            txn.close();
            jcp.close();
//            jedisPool.returnResource(jedis);
        }

        return status;
    }

    @Override
    public void configure(Context context) {
        gson = new Gson();

        logger.info("Configuring");
        host = context.getString(RedisSinkConfigurationConstant.HOST);
        Preconditions.checkState(StringUtils.isNotBlank(host),
                "host cannot be empty, please specify in configuration file");

        port = context.getInteger(RedisSinkConfigurationConstant.PORT, Protocol.DEFAULT_PORT);
        ports = context.getString(RedisSinkConfigurationConstant.PORTS, "6379");

        timeout = context.getInteger(RedisSinkConfigurationConstant.TIMEOUT, Protocol.DEFAULT_TIMEOUT);
        database = context.getInteger(RedisSinkConfigurationConstant.DATABASE, Protocol.DEFAULT_DATABASE);
        password = context.getString(RedisSinkConfigurationConstant.PASSWORD);
        redisKey = context.getString(RedisSinkConfigurationConstant.KEY, RedisSinkConfigurationConstant.DEFAULT_KEY)
                .getBytes();
        batchSize = context.getInteger(RedisSinkConfigurationConstant.BATCH_SIZE,
                RedisSinkConfigurationConstant.DEFAULT_BATCH_SIZE);
        String serializerClassName = context.getString(RedisSinkConfigurationConstant.SERIALIZER,
                RedisSinkConfigurationConstant.DEFAULT_SERIALIZER_CLASS_NAME);

        Preconditions.checkState(batchSize > 0, RedisSinkConfigurationConstant.BATCH_SIZE
                + " parameter must be greater than 1");

        try {
            /**
             * Instantiate serializer
             */
            @SuppressWarnings("unchecked") Class<? extends Serializer> clazz = (Class<? extends Serializer>) Class
                    .forName(serializerClassName);
            serializer = clazz.newInstance();

            /**
             * Configure it
             */
            Context serializerContext = new Context();
            serializerContext.putAll(context.getSubProperties(RedisSinkConfigurationConstant.SERIALIZER_PREFIX));
            serializer.configure(serializerContext);

        } catch (ClassNotFoundException e) {
            logger.error("Could not instantiate event serializer", e);
            Throwables.propagate(e);
        } catch (InstantiationException e) {
            logger.error("Could not instantiate event serializer", e);
            Throwables.propagate(e);
        } catch (IllegalAccessException e) {
            logger.error("Could not instantiate event serializer", e);
            Throwables.propagate(e);
        }

    }
}
