import com.yam.redis.JedisClusterPipeline;
import redis.clients.jedis.*;

import java.io.*;
import java.lang.reflect.Method;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by moyong on 17/6/28.
 */
public class RedisCluster {
    static String filename ="/Users/moyong/project/env-myopensource/3-tools/mytools/alpine/flume/conf/data/01_01_20170701110231740.txt";

    /**
     * 集群+pipeline 每秒20万
     * @param args
     * @throws InterruptedException
     * @throws ExecutionException
     */
    public static void main(String[] args) throws InterruptedException, ExecutionException {
//        String host="127.0.0.1";
//        String[] hosts = host.split(";");



        String port="6381;6382;6383;6384;6385;6386";
        String[] ports = port.split(";");

//        if (hosts.length>=2){
        //只给集群里一个实例就可以
        Set<HostAndPort> jedisClusterNodes = new HashSet<HostAndPort>();
        for (int i = 0; i < ports.length; i++) {
//            System.out.println(ports[i]);
            jedisClusterNodes.add(new HostAndPort("192.168.0.122",new Integer(ports[i])));
//            System.out.println(jedisClusterNodes);
        }
        // 构造池
        final JedisCluster jedisCluster= new JedisCluster(jedisClusterNodes);
//        final JedisCluster jedisCluster= new JedisCluster(jedisClusterNodes);



        final String key =  "132.194.43.146";
        String score =  "2017080912345";
        String member =  "account@acc";

        long start=System.currentTimeMillis();

        //ExecutorService e = Executors.newCachedThreadPool();
        //ExecutorService e = Executors.newSingleThreadExecutor();
        //ExecutorService e = Executors.newFixedThreadPool(10);
        // 第一种是可变大小线程池，按照任务数来分配线程，
        // 第二种是单线程池，相当于FixedThreadPool(1)
        // 第三种是固定大小线程池。
        // 然后运行
//        e.execute(new MyRunnableImpl());

        //for (int i = 0; i < 20; i++) {

            //e.execute();
          //  Long zadd = jedisCluster.zadd((key+i).getBytes(), new Double(i), "member+i".getBytes());

        //}


        jedisCluster.del("incrNum".getBytes());
        final AtomicInteger atomicInteger = new AtomicInteger(0);
        final CountDownLatch countDownLatch = new CountDownLatch(10);
        ExecutorService executorService = Executors.newFixedThreadPool(100);
        for(int i = 0 ; i < 10 ; i ++){
            final int finalI = i;
            executorService.submit(new Runnable() {
                @Override
                public void run() {
                    //每个线程增加1000次，每次加1
                    for(int j = 0 ; j < 60000 ; j ++){
                        atomicInteger.incrementAndGet();
                        //jedisCluster.incr("incrNum".getBytes());

                        jedisCluster.zadd((key+ finalI +j).getBytes(), new Double(finalI +j), ("member"+ finalI +j).getBytes());
                    }

                    countDownLatch.countDown();

                }
            });
        }
        System.out.println(countDownLatch.getCount());

        countDownLatch.await();
       // System.out.println(new String(jedisCluster.get("incrNum".getBytes())));
        // 关闭线程池
        executorService.shutdown();



        long end=System.currentTimeMillis();

        System.out.println("处理"+atomicInteger+"条记录");
        System.out.println("处理"+atomicInteger.intValue()+"条记录");
        System.out.println("having times 毫秒:"+(end-start));
        System.out.println("having times 秒:"+(end-start)/1000);
        System.out.println("having times 分钟:"+(end-start)/1000/60);
        System.out.println("having times 毫秒/zadd:"+200000/(end-start));
        System.out.println("处理"+(atomicInteger.intValue()/((end-start)/1000))+"条记录");


        System.out.println("//////////////////////////////////////////////////////////////");

        final AtomicInteger atomicInteger1 = new AtomicInteger(0);
        final CountDownLatch countDownLatch1 = new CountDownLatch(10);
        ExecutorService executorService1 = Executors.newFixedThreadPool(100);
        long s = System.currentTimeMillis();


        List<Object> batchResult = null;

        for (int i = 0; i < 10; i++) {
            final int finalI = i;
            executorService1.submit(new Runnable() {
                @Override
                public void run() {
                    JedisClusterPipeline jcp=null;
                    try {
                        jcp = JedisClusterPipeline.pipelined(jedisCluster);
                        jcp.refreshCluster();

                        System.out.println("///"+finalI);

                        for (int j = 0; j < 60000; j++) {
                            atomicInteger1.incrementAndGet();
                            //jedisCluster.incr("incrNum".getBytes());

                            jcp.zadd((key + finalI + j).getBytes(), new Double(finalI + j), ("member" + finalI + j).getBytes());
                        }

                        List<Object> objects = jcp.syncAndReturnAll();

                    } finally {
                        jcp.close();
                    }

                    countDownLatch1.countDown();

                }
            });
        }

        countDownLatch1.await();
        executorService1.shutdown();

//        try {
//            // batch write
//            for (int i = 0; i < 200000; i++) {
//                //jcp.set("k" + i, "v1" + i);
//                jcp.zadd((key +i).getBytes(), new Double(i), ("member" +i).getBytes());
//
//            }
//            jcp.sync();
//
//            // batch read
//            for (int i = 0; i < 200000; i++) {
//                jcp.get("k" + i);
//            }
//            batchResult = jcp.syncAndReturnAll();
//        } finally {
//            jcp.close();
//        }

        // output time
        long t = System.currentTimeMillis() - s;
        System.out.println(t);

//        System.out.println(batchResult.size());
//        System.out.println("每秒："+batchResult.size()/(t/1000));

        System.out.println(atomicInteger1.intValue());

        System.out.println("每秒："+atomicInteger1.intValue()/(t/1000));

        // 实际业务代码中，close要在finally中调，这里之所以没这么写，是因为懒
//        try {
//            jedisCluster.close();
//        } catch (IOException e) {
//            e.printStackTrace();
//        }

        System.out.println("//////////////////////////////////////////////////////////////");











        int taskSize = 5;
        // 创建一个线程池
        ExecutorService pool = Executors.newFixedThreadPool(taskSize);
        // 创建多个有返回值的任务
        List<Future> list = new ArrayList<Future>();
        for (int i = 0; i < taskSize; i++) {
            Callable c = new MyCallable(i + " ");
            // 执行任务并获取Future对象
            Future f = pool.submit(c);
            // System.out.println(">>>" + f.get().toString());
            list.add(f);
        }
        // 关闭线程池
        pool.shutdown();

        // 获取所有并发任务的运行结果
        for (Future f : list) {
            // 从Future对象上获取任务的返回值，并输出到控制台
            System.out.println(">>>" + f.get().toString());
        }



        //Iterator<JedisPool> iterator = cluster.getClusterNodes().values().iterator();

        //while (iterator.hasNext())

                    //System.out.println(iterator.next().getNumActive());

//            cluster.set("a","b");




//        Collection<JedisPool> values = jedisCluster.getClusterNodes().values();
//        for (JedisPool jedisPool : values) {
//            Jedis jedis = jedisPool.getResource();
//            try {
//                //assertEquals(clientName, jedis.clientGetname());
//            } finally {
//                jedis.close();
//            }
//        }


//        try {
//            jc = new JedisCluster(jedisClusterNode, DEFAULT_TIMEOUT, DEFAULT_TIMEOUT,
//                    DEFAULT_REDIRECTIONS, "cluster", DEFAULT_CONFIG);
//            jc.set("51", "foo");
//        } finally {
//            if (jc != null) {
//                jc.close();
//            }
//        }


//        final JedisCluster jc = new JedisCluster(jedisClusterNode, DEFAULT_TIMEOUT, DEFAULT_TIMEOUT,
//                DEFAULT_REDIRECTIONS, "cluster", DEFAULT_CONFIG);
//        jc.set("foo", "bar");
//
//        ThreadPoolExecutor executor = new ThreadPoolExecutor(10, 100, 0, TimeUnit.SECONDS,
//                new ArrayBlockingQueue<Runnable>(10));
//        List<Future<String>> futures = new ArrayList<Future<String>>();
//        for (int i = 0; i < 50; i++) {
//            executor.submit(new Callable<String>() {
//                @Override
//                public String call() throws Exception {
//                    // FIXME : invalidate slot cache from JedisCluster to test
//                    // random connection also does work
//                    return jc.get("foo");
//                }
//            });
//        }
//
//        for (Future<String> future : futures) {
//            String value = future.get();
//            assertEquals("bar", value);
//        }
//
//        jc.close();
//
        //.clusterReset(Reset.SOFT);

    }

    static class MyCallable implements Callable<Object> {
        private String taskNum;

        MyCallable(String taskNum) {
            this.taskNum = taskNum;
        }

        public Object call() throws Exception {
            System.out.println(">>>" + taskNum + "任务启动");
            Date dateTmp1 = new Date();
            Thread.sleep(1000);
            Date dateTmp2 = new Date();
            long time = dateTmp2.getTime() - dateTmp1.getTime();
            System.out.println(">>>" + taskNum + "任务终止");
            return taskNum + "任务返回运行结果,当前任务时间【" + time + "毫秒】";
        }
    }

    private static void nio() {
        MappedByteBuffer buffer=null;
        RandomAccessFile rw = null;

        try {
             rw = new RandomAccessFile(filename, "rw");

            buffer= rw.getChannel().map(FileChannel.MapMode.READ_WRITE, 0, 1253244);
            int sum=0;
            int n;
            long t1=System.currentTimeMillis();
            for(int i=0;i<1253244;i++){
                n=0x000000ff&buffer.get(i);
                sum+=n;
            }
            long t=System.currentTimeMillis()-t1;
            System.out.println("sum:"+sum+" time:"+t);
            buffer.asReadOnlyBuffer();
        } catch (FileNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }finally {
            //rw.close();
        }
    }

    public void copyFile(String filename,String srcpath,String destpath)throws IOException {
        File source = new File(srcpath+"/"+filename);
        File dest = new File(destpath+"/"+filename);
        FileChannel in = null, out = null;
        try {
            in = new FileInputStream(source).getChannel();
            out = new FileOutputStream(dest).getChannel();
            long size = in.size();
            MappedByteBuffer buf = in.map(FileChannel.MapMode.READ_ONLY, 0, size);
            out.write(buf);
            in.close();
            out.close();
            clean(buf); //解决安全问题
            source.delete();//文件复制完成后，删除源文件
        }catch(Exception e){
            e.printStackTrace();
        } finally {
            in.close();
            out.close();
        }
    }

    public static void clean(final Object buffer) throws Exception {
        AccessController.doPrivileged(new PrivilegedAction() {
            public Object run() {
                try {
                    Method getCleanerMethod = buffer.getClass().getMethod("cleaner",new Class[0]);
                    getCleanerMethod.setAccessible(true);
                    sun.misc.Cleaner cleaner =(sun.misc.Cleaner)getCleanerMethod.invoke(buffer,new Object[0]);
                    cleaner.clean();
                } catch(Exception e) {
                    e.printStackTrace();
                }
                return null;}});

    }
}
