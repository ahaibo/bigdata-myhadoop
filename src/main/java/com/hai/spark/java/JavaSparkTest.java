package com.hai.spark.java;

import com.alibaba.fastjson.JSONObject;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;

import java.util.Arrays;
import java.util.List;

/**
 * Created by Administrator on 2017/4/14.
 */
public class JavaSparkTest {
    public static void main(String[] args) {
        //get Env variable
        String path = System.getenv("PATH");
        System.out.println("PATH: " + path);

        SparkConf conf = new SparkConf();
        conf.setMaster("local[4]");
        conf.setAppName("JavaSparkTest");

        JavaSparkContext sparkContext = new JavaSparkContext(conf);
        JavaRDD<Integer> rdd = sparkContext.parallelize(Arrays.asList(1, 2, 3, 4));

        Integer first = rdd.first();
        List<Integer> list = rdd.take(3);
        System.out.println(first);
        System.out.println(JSONObject.toJSONString(list, true));

        rdd.foreach(new VoidFunction<Integer>() {
            @Override
            public void call(Integer val) throws Exception {
                String tname = Thread.currentThread().getName();
                System.out.println(tname + "\t" + val);
            }
        });
//        sparkContext.textFile("");
    }
}
