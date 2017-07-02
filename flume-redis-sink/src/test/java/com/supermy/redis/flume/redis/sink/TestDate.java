package com.supermy.redis.flume.redis.sink;

import java.text.SimpleDateFormat;
import java.util.Calendar;

/**
 * Created by moyong on 17/6/28.
 */
public class TestDate {

    public static void main(String args[]) {
        Calendar date = Calendar.getInstance();
        date.set(Calendar.DATE, date.get(Calendar.DATE) - 4);
        SimpleDateFormat fmt = new SimpleDateFormat("yyyyMMddHHmmss");
        String remdate=fmt.format(date.getTime());
        System.out.println("Hello World!");
        System.out.println(remdate);
    }

}
