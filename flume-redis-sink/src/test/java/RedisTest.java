import com.yam.redis.JedisClusterPipeline;
import redis.clients.jedis.*;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by moyong on 17/6/28.
 */
public class RedisTest {

    /**
     * 单机 多线程+pipeline 性能每秒20万;zset 数据集群更具优势；set 数据单机可达30万每秒。
     * @param args
     * @throws InterruptedException
     * @throws ExecutionException
     */
    public static void main(String[] args) throws InterruptedException, ExecutionException {


        String value = null;

        JedisPool pool = null;
        Jedis jedis = null;
        try {
            pool = getPool();
            jedis = pool.getResource();


            long start = System.currentTimeMillis();

            jedis.del("incrNum".getBytes());

            final AtomicInteger atomicInteger = new AtomicInteger(0);
            final CountDownLatch countDownLatch = new CountDownLatch(20);
            ExecutorService executorService = Executors.newFixedThreadPool(100);

            for (int i = 0; i < 20; i++) {
//                System.out.println(i);

                final int finalI = i;
                final JedisPool finalPool = pool;
                executorService.submit(new Runnable() {
                    @Override
                    public void run() {
                        //每个线程增加1000次，每次加1


                        Jedis finalJedis = finalPool.getResource();

                        Pipeline pipelined = finalJedis.pipelined();
                        for (int j = 0; j < 60000; j++) {
                            atomicInteger.incrementAndGet();
                            //pipelined.incr("incrNum".getBytes());

                            pipelined.zadd(("key" + finalI + j).getBytes(), new Double(finalI + j), ("member" + finalI + j).getBytes());
                        }

                        pipelined.sync();

                        countDownLatch.countDown();

                    }
                });
            }

            countDownLatch.await();
            // 关闭线程池
            executorService.shutdown();


            long end = System.currentTimeMillis();

            System.out.println("处理" + atomicInteger + "条记录");
            System.out.println("处理" + atomicInteger.intValue() + "条记录");
            System.out.println("having times 毫秒:" + (end - start));
            System.out.println("having times 秒:" + (end - start) / 1000);
            System.out.println("having times 分钟:" + (end - start) / 1000 / 60);
            System.out.println("having times 毫秒/zadd:" + 200000 / (end - start));
            System.out.println("每秒处理" + (atomicInteger.intValue() / ((end - start) / 1000)) + "条记录");


        } catch (Exception e) {
            //释放redis对象
            jedis.close();
            e.printStackTrace();
        } finally {
            //返还到连接池
            returnResource(pool, jedis);
        }
//
//
//        System.out.println("//////////////////////////////////////////////////////////////");
//        int taskSize = 5;
//        // 创建一个线程池
//        ExecutorService pool = Executors.newFixedThreadPool(taskSize);
//        // 创建多个有返回值的任务
//        List<Future> list = new ArrayList<Future>();
//        for (int i = 0; i < taskSize; i++) {
//            Callable c = new MyCallable(i + " ");
//            // 执行任务并获取Future对象
//            Future f = pool.submit(c);
//            // System.out.println(">>>" + f.get().toString());
//            list.add(f);
//        }
//        // 关闭线程池
//        pool.shutdown();
//
//        // 获取所有并发任务的运行结果
//        for (Future f : list) {
//            // 从Future对象上获取任务的返回值，并输出到控制台
//            System.out.println(">>>" + f.get().toString());
//        }



    }




    private static JedisPool pool = null;

    /**
     * 构建redis连接池
     *
     * @param ip
     * @param port
     * @return JedisPool
     */
    public static JedisPool getPool() {
        if (pool == null) {
            JedisPoolConfig config = new JedisPoolConfig();
            //控制一个pool可分配多少个jedis实例，通过pool.getResource()来获取；
            //如果赋值为-1，则表示不限制；如果pool已经分配了maxActive个jedis实例，则此时pool的状态为exhausted(耗尽)。
            config.setMaxTotal(500);
            //控制一个pool最多有多少个状态为idle(空闲的)的jedis实例。
            config.setMaxIdle(5);
            //表示当borrow(引入)一个jedis实例时，最大的等待时间，如果超过等待时间，则直接抛出JedisConnectionException；
            config.setMaxWaitMillis(1000 * 100);
            //在borrow一个jedis实例时，是否提前进行validate操作；如果为true，则得到的jedis实例均是可用的；
            config.setTestOnBorrow(true);
            pool = new JedisPool(config, "127.0.0.1", 6379);
        }
        return pool;
    }

    /**
     * 返还到连接池
     *
     * @param pool
     * @param redis
     */
    public static void returnResource(JedisPool pool, Jedis redis) {
        redis.close();
    }


}
