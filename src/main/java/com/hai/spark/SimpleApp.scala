package com.hai.spark

import org.apache.spark.{SparkConf, SparkContext}

object SimpleApp {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf
        conf.setMaster("local[4]")
        conf.setAppName("SparkTest")
        val sc = new SparkContext(conf);
        val testLog = sc.textFile("D:\\Logs\\test.log", 2).cache();
        val numAs = testLog.filter(line => line.contains("a")).count()
        val numBs = testLog.filter(line => line.contains("b")).count()
        println("Lines with a: %s, Lines with b: %s".format(numAs, numBs))
    }
}