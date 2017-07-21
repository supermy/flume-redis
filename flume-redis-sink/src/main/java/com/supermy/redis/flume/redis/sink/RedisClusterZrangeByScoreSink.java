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
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

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
    private JedisClusterPipeline jcp=null;

    private ExecutorService executorService = null;
    private int threadNum=10;
    private int threadPool=100;


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

//        jcp = JedisClusterPipeline.pipelined(cluster);
//        jcp.refreshCluster();

        executorService = Executors.newFixedThreadPool(threadPool);

        super.start();
    }

    @Override
    public synchronized void stop() {
        logger.info("Stoping");

//        if (jedisPool != null) {
//            jedisPool.destroy();
//        }

        if (jcp!=null){
            jcp.close();
        }//

        //关闭集群链接
        if (cluster != null) {
            try {
                cluster.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        executorService.shutdown();

        super.stop();
    }

    @Override
    public Status process() throws EventDeliveryException {
        logger.info("process");

        Status status = Status.READY;

        //List<byte[]> batchEvents = new ArrayList<byte[]>(batchSize);

        final Channel channel = getChannel();


        logger.info("thread num {}",threadNum);
        logger.info("每批的数量 batch size {}",batchSize);

        final AtomicInteger ai = new AtomicInteger(0);
        final CountDownLatch cdl = new CountDownLatch(threadNum);
        long s = System.currentTimeMillis();


        for (int i = 0; i < threadNum; i++) {
            final int finalI = i;

            executorService.submit(new Runnable() {
                @Override
                public void run() {
                     Set<byte[]> batchEvents = new HashSet<byte[]>(batchSize);

                    logger.info("thread id: {}",Thread.currentThread().getId());

                    JedisClusterPipeline jcp = null;
//                    try {
//                        jcp = JedisClusterPipeline.pipelined(cluster);
//                        jcp.refreshCluster();
//
//                        System.out.println("///" + finalI);
//
//                        for (int j = 0; j < 60000; j++) {
//                            ai.incrementAndGet();
//                            //jedisCluster.incr("incrNum".getBytes());
//
//                            jcp.zadd((key + finalI + j).getBytes(), new Double(finalI + j), ("member" + finalI + j).getBytes());
//
//                        }
//
//                        List<Object> objects = jcp.syncAndReturnAll();
//
//                    } finally {
//                        jcp.close();
//                    }

                    Transaction txn = channel.getTransaction();

                    try {
                        txn.begin();

                        long ss=System.currentTimeMillis();
                        Status status = Status.READY;
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
                        long ee=System.currentTimeMillis()-ss;

                        /**
                         * Only send events if we got any
                         */
                        if (batchEvents.size() > 0) {
                            if (logger.isDebugEnabled()) {
                                logger.info("Sending " + batchEvents.size() + " events");
                            }
                            logger.info("话费的时间 {} 秒,实际数据的数量 batch size {}",ee/1000,batchEvents.size());

                            jcp = JedisClusterPipeline.pipelined(cluster);
                            jcp.refreshCluster();

//                            byte[][] redisEvents = new byte[batchEvents.size()][];

                            Map<String, Response<Set<String>>> responses = new HashMap<String, Response<Set<String>>>(batchEvents.size());

                            for (byte[] redisEvent : batchEvents) {

                                String json = new String(redisEvent);
                                Map m = gson.fromJson(json, HashMap.class);
                                String key = m.get("key").toString();
                                String arg = m.get("arg").toString();

                                ai.incrementAndGet();

                                //时间序列查询
                                responses.put(key + "|" + arg, jcp.zrangeByScore(key, arg, "+inf", 0, 1));
                            }

                            jcp.sync();

                            Set<String> result = new HashSet<String>();

                            //查询结果处理
                            for (String k : responses.keySet()) {
                                Set<String> lines = responses.get(k).get();
                                for (String line : lines) {
                                    //返回的数据进行逻辑处理  一般只有一行
                                    if (!StringUtils.isEmpty(line)) {
                                        if (line.endsWith("@End")) {

                                            StringBuffer sb = new StringBuffer();
                                            sb.append(k).append("|");
                                            sb.append(line.replace("@End", ""));

                                            result.add(sb.toString());

                                            //p.lpush("netlogacts",sb.toString());

                                        }
                                    }
                                }
                            }


                            //查询结果队列回存
                            if (result.size() > 1) {
                                String[] desc = new String[result.size()];
                                result.toArray(desc);
                                if (desc.length > 0) {
                                    jcp.lpush("netlogacts", desc);
                                }
                            }

                            jcp.sync();


                        }

                        txn.commit();
                    } catch (JedisConnectionException e) {
                        txn.rollback();
                        logger.error("Error while shipping events to redis", e);
                    } catch (Throwable t) {
                        txn.rollback();
                        logger.error("Unexpected error", t);
                    } finally {
                        txn.close();

                        if (jcp != null) {
                            jcp.close();
                        }
                    }

                    cdl.countDown();
                }
            });
        }


        try {
            cdl.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        if (ai.get() == 0) {
            status = Status.BACKOFF;
        }


        long t = System.currentTimeMillis() - s;

        logger.info("处理完成的数据量：{}",ai.intValue());
        logger.info("每秒处理的数据量：{}",ai.intValue()/(t/1000));



        return status;
    }

    @Override
    public void configure(Context context) {
        logger.info("configure......");
        logger.info("Configuring");

        gson = new Gson();

        host = context.getString(RedisSinkConfigurationConstant.HOST);
        Preconditions.checkState(StringUtils.isNotBlank(host),
                "host cannot be empty, please specify in configuration file");

        port = context.getInteger(RedisSinkConfigurationConstant.PORT, Protocol.DEFAULT_PORT);
        ports = context.getString(RedisSinkConfigurationConstant.PORTS, "6379");

        //增加线程与线程池数量可配置；
        threadNum = context.getInteger(RedisSinkConfigurationConstant.THREAD_NUM, 10);
        threadPool = context.getInteger(RedisSinkConfigurationConstant.THREAD_POOL, 100);


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
