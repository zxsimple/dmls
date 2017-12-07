package com.zxsimple.dmls.util;

import org.apache.hadoop.hive.ql.exec.UDF;
import scala.util.Random;


/**
 * Created by zxsimple on 2016/8/31.
 * hive UDF的函数，生成hash值
 */
public class CreateHash extends UDF{

    public Integer evaluate(String s) {

        s +=  new Random().nextInt(1000000) + System.currentTimeMillis();
        return Math.abs(s.hashCode());
    }

}
