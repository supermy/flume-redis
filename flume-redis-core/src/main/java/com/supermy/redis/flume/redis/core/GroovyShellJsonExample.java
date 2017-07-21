package com.supermy.redis.flume.redis.core;

/**
 * Created by moyong on 16/11/24.
 */

import groovy.lang.Binding;
import groovy.lang.GroovyShell;
import groovy.lang.Script;
import org.codehaus.groovy.runtime.InvokerHelper;

import java.io.File;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 测试内存是否存在问题
 * 很长时间之后
 * Exception: java.lang.OutOfMemoryError thrown from the UncaughtExceptionHandler in thread "main"
 * <p/>
 * 位置前移解决问题
 * static Map<String, Object> scriptCache = new ConcurrentHashMap<String, Object>();
 */
public class GroovyShellJsonExample {
    static Map<String, Object> scriptCache = new ConcurrentHashMap<String, Object>();

    public static void main(String args[]) {

//        oral();
        //JSON 的测试例子
        int i = 0;
        while (true) {
            i++;
            String json = "{ \"name\": \"James Mo\" }";
            Binding binding = new Binding();
            binding.setVariable("p1", "200");
            binding.setVariable("p2", json);


            try {
//            String script = "println\"Welcome to $language\"; y = x * 2; z = x * 3; return x ";
//            Object hello = GroovyShellJsonExample.getShell("hello", script, binding);
                String scriptname = "jsontest";
                String script = "/Users/moyong/project/env-myopensource/3-tools/flume-rule-interceptor/src/main/resources/hello.groovy";
//                File script = new ClassPathResource("hello.groovy").getFile();
                File f = new File(script);

                System.out.println("f.lastModified():"+f.lastModified());


                Object hello = GroovyShellJsonExample.getShell(scriptname+f.lastModified(), f, binding);

                System.out.println(hello);
                System.out.println(i);

            } catch (Exception e) {
                e.printStackTrace();
            }
        }


    }

    /**
     * 为Groovy Script增加缓存
     * 解决Binding的线程安全问题
     *
     * @return
     */
    public static Object getShell(String cacheKey, File f, Binding binding) {


        Object scriptObject = null;
        try {


            Script shell = null;
            if (scriptCache.containsKey(cacheKey)) {
                //System.out.println("===============scriptCache:"+cacheKey);

                shell = (Script) scriptCache.get(cacheKey);
            } else {
                //System.out.println("===============");
                shell = new GroovyShell().parse(f);
                scriptCache.put(cacheKey, shell);

//                shell = cache(cacheKey, script);
            }

            //shell.setBinding(binding);
            //scriptObject = (Object) shell.run();

            scriptObject = (Object) InvokerHelper.createScript(shell.getClass(), binding).run();


            // clear
            binding.getVariables().clear();
            binding = null;

            // Cache
            if (!scriptCache.containsKey(cacheKey)) {
                //shell.setBinding(null);
                scriptCache.put(cacheKey, shell);
            }
        } catch (Exception t) {
            t.printStackTrace();
            //System.out.println("groovy script eval error. script: " + script, t);
        }

        return scriptObject;
    }

    /**
     * 传统调用方式；
     */
    private static void oral() {
        Binding binding = new Binding();

        binding.setVariable("x", 10);

        binding.setVariable("language", "Groovy");

        GroovyShell shell = new GroovyShell(binding);

        Object value = shell.evaluate
                ("println\"Welcome to $language\"; y = x * 2; z = x * 3; return x ");

        assert value.equals(10);

        assert binding.getVariable("y").equals(20);

        assert binding.getVariable("z").equals(30);
    }
}
