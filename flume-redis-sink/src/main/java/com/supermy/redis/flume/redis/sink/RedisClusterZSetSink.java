/**
 * Copyright 2014 TangoMe Inc.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
import redis.clients.util.JedisClusterCRC16;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/*
 * Simple sink which read events from a channel and lpush them to redis
 */
public class RedisClusterZSetSink extends AbstractSink implements Configurable {

    private static final Logger logger = LoggerFactory.getLogger(RedisClusterZSetSink.class);

    /**
     * Configuration attributes
     */
    private String host = null;
    private String ports = null;
    private Integer port = null;
    private Integer timeout = null;
    private String password = null;
    private Integer database = null;
    private byte[] redisKey = null;
    private Integer batchSize = null;
    private Serializer serializer = null;

    //    private final JedisPoolFactory jedisPoolFactory;
//    private JedisPool jedisPool = null;
    private Gson gson = null;
    private JedisCluster cluster = null;
    private JedisClusterPipeline jcp = null;


//    private ExecutorService executorService = null;


    public RedisClusterZSetSink() {
        //jedisPoolFactory = new JedisPoolFactoryImpl();
    }

//    @VisibleForTesting
//    public RedisClusterZSetSink(JedisPoolFactory _jedisPoolFactory) {
//        if (_jedisPoolFactory == null) {
//            throw new IllegalArgumentException("JedisPoolFactory cannot be null");
//        }
//
//        this.jedisPoolFactory = _jedisPoolFactory;
//    }

    /**
     * 初始化变量
     *
     */
    @Override
    public synchronized void start() {
//        executorService = Executors.newFixedThreadPool(100);

        logger.info("Starting");
//        if (jedisPool != null) {
//            jedisPool.destroy();
//        }

        //JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
        //智能判断是否集群
        logger.debug("********************");
        logger.debug(host);
        logger.debug(ports);

        String[] hosts = host.split(";");
        String[] portlist = ports.split(";");
        Set<HostAndPort> jedisClusterNodes = new HashSet<HostAndPort>();

        if (portlist.length >= 2) { //支持本机及群多端口
            for (int i = 0; i < portlist.length; i++) {
                jedisClusterNodes.add(new HostAndPort(host, new Integer(portlist[i])));
            }
        } else {
            for (int i = 0; i < hosts.length; i++) {
                jedisClusterNodes.add(new HostAndPort(hosts[i], port));
            }
        }
        // 构造池
        cluster = new JedisCluster(jedisClusterNodes);
        logger.debug(jedisClusterNodes.toString());

        cluster.set("bar", "foo");

        jcp = JedisClusterPipeline.pipelined(cluster);
        jcp.refreshCluster();

        super.start();
    }

    /**
     * 销毁变量
     */
    @Override
    public synchronized void stop() {
        logger.info("Stoping");

//        executorService.shutdown();
//        while (!executorService.isTerminated()) {
//            logger.debug("Waiting for ExecTail executor service to stop");
//            try {
//                executorService.awaitTermination(500, TimeUnit.MILLISECONDS);
//            } catch (InterruptedException e) {
//                logger.debug("Interrupted while waiting for ExecTail executor service "
//                        + "to stop. Just exiting.");
//                Thread.currentThread().interrupt();
//            }
//        }


//        if (jedisPool != null) {
//            jedisPool.destroy();
//        }

        if (jcp == null) {
            logger.error("finally 什么都不做......");
        } else {
            jcp.close();
        }

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
        if (cluster == null) {
            throw new EventDeliveryException("Redis cluster connection not established. Please verify your configuration");
        }

        Set<byte[]> batchEvents = new HashSet<byte[]>(batchSize);

        Channel channel = getChannel();
        Transaction txn = channel.getTransaction();

        //Jedis jedis = jedisPool.getResource();

        //JedisClusterPipeline jcp = null;


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


            /**
             * Only send events if we got any
             */
            if (batchEvents.size() > 0) {
                if (logger.isDebugEnabled()) {
                    logger.debug("Sending " + batchEvents.size() + " events");
                }

                jcp.setJedisCluster(cluster);
                jcp = JedisClusterPipeline.pipelined(cluster);
                jcp.refreshCluster();

//                Pipeline p = jedis.pipelined();

//                jcp.refreshCluster();

                byte[][] redisEvents = new byte[batchEvents.size()][];
//                int index = 0;
//                Map<String,Double> scoreMembers = new HashMap<String,Double>();

                Calendar date = Calendar.getInstance();
                date.set(Calendar.DATE, date.get(Calendar.DATE) - 3);
                SimpleDateFormat fmt = new SimpleDateFormat("yyyyMMddHHmmss");
//                logger.debug("==========================");
//                logger.debug(new Date().getTime());
//                logger.debug(fmt);
//                logger.debug(date);
                String remdate = fmt.format(date.getTime());

                for (byte[] redisEvent : batchEvents) {

                    String json = new String(redisEvent);
                    Map m = gson.fromJson(json, HashMap.class);
                    String key = m.get("key").toString();
                    String score = m.get("score").toString();
                    String member = m.get("member").toString();

                    jcp.zadd(key.getBytes(), new Double(score), member.getBytes());//key is ip 地址，所以这个是不对的。

                    //ZREMRANGEBYSCORE key min max    数据量小直接在此进行处理

                    jcp.zremrangeByScore(key.getBytes(), "0".getBytes(), remdate.getBytes());

                }

                //jcp.sync();
                jcp.syncAndReturnAll();

//                for (byte[] redisEvent : batchEvents) {
//
//                    String json = new String(redisEvent);
//                    Map m = gson.fromJson(json, HashMap.class);
//                    String key = m.get("key").toString();
//                    String score = m.get("score").toString();
//                    String member = m.get("member").toString();
//
//                    //jcp.zadd(key.getBytes(), new Double(score), member.getBytes());//key is ip 地址，所以这个是不对的。
//
//                    //ZREMRANGEBYSCORE key min max    数据量小直接在此进行处理
//
//                    jcp.zremrangeByScore(key.getBytes(), "0".getBytes(), remdate.getBytes());
//
//                }
//
//                jcp.sync();

            }

            txn.commit();
        } catch (JedisConnectionException e) {
            e.printStackTrace();
            txn.rollback();


            //           jcp.close();
//            jedisPool.returnBrokenResource(jedis);
            logger.error("Error while shipping events to redis cluster...", e);
        } catch (Throwable t) {
            txn.rollback();
            logger.error("Unexpected error", t);
        } finally {

            txn.close();

            if (jcp == null) {
                logger.error("finally 什么都不做......");
            }else{
                jcp.close();
            }


        }

        return status;
    }

    /**
     *
     * 初始化配置参数
     *
     * @param context
     */
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
