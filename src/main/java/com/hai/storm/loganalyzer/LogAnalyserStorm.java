package com.hai.storm.loganalyzer;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

import java.util.HashMap;
import java.util.Map;

//Create main class LogAnalyserStorm submit topology.
public class LogAnalyserStorm {

    public static void main(String[] args) throws Exception {

//        System.setProperty("java.io.tmpdir","D:\\tmp\\");
//        System.out.println("java.io.tmpdir: " + System.getProperty("java.io.tmpdir"));

        //Create Config instance for cluster configuration
        Config config = new Config();
        config.setDebug(true);

        //environment props
        Map<String, String> env = new HashMap<String, String>();
        env.put("storm.zookeeper.servers", "s4");//require
        config.setEnvironment(env);

        //builder
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("call-log-reader-spout", new FakeCallLogReaderSpout());
        builder.setBolt("call-log-creator-bolt", new CallLogCreatorBolt()).shuffleGrouping("call-log-reader-spout");
        builder.setBolt("call-log-counter-bolt", new CallLogCounterBolt()).fieldsGrouping("call-log-creator-bolt", new Fields("call"));

        //local cluster model
//        LocalCluster cluster = new LocalCluster();
//        cluster.submitTopology("LogAnalyserStorm", config, builder.createTopology());
//        Thread.sleep(10000);
//        //Stop the topology
//        cluster.shutdown();

        //submit to cluster
        StormSubmitter.submitTopology("LogAnalyserStorm", config, builder.createTopology());
    }

}