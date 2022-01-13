package com.zz.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * ************************************
 * create by Intellij IDEA
 *
 * @author Francis.zz
 * @date 2021-12-30 16:12
 * ************************************
 */
public class LogUtil {
    private static final Logger log = LoggerFactory.getLogger(LogUtil.class);

    public static void info(String msg) {
        log.info(msg);
    }

    public static void error(String msg) {
        log.error(msg);
    }

    public static void main(String[] args) {
        String format = "yyyy-MM-dd HH:mm:ss+0800";
        SimpleDateFormat format1 = new SimpleDateFormat(format);
        System.out.println(format1.format(new Date()));
    }
}
