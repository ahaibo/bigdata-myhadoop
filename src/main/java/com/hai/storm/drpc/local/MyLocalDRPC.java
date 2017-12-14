package com.hai.storm.drpc.local;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.LocalDRPC;
import org.apache.storm.drpc.LinearDRPCTopologyBuilder;

/**
 * Created by Administrator on 2017/4/13.
 */
public class MyLocalDRPC {
    public static void main(String[] args) {
        Config conf = new Config();
        LinearDRPCTopologyBuilder builder = new LinearDRPCTopologyBuilder("exclamation");
        builder.addBolt(new ExclaimBolt(), 3);

        LocalDRPC drpc = new LocalDRPC();
        LocalCluster cluster = new LocalCluster();

        cluster.submitTopology("drpc-demo", conf, builder.createLocalTopology(drpc));
        System.out.println("Results for 'hello':" + drpc.execute("exclamation", "hello"));

        cluster.shutdown();
        drpc.shutdown();
    }
}
