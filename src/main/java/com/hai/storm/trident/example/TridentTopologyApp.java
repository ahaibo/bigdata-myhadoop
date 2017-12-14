package com.hai.storm.trident.example;

import com.google.common.collect.ImmutableList;
import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.trident.Stream;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.testing.FeederBatchSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by Administrator on 2017/4/12.
 */
public class TridentTopologyApp {

    public static void main(String[] args) throws Exception {
        Config config = new Config();
        config.setNumWorkers(3);
//        config.setDebug(true);

        Map<String, String> env = new HashMap<String, String>();
        env.put("storm.zookeeper.servers", "s4");
//        env.put("storm.zookeeper.root", "2181");
//        env.put("storm.cluster.mode", "local");
//        env.put("topology.debug", "true");
        config.setEnvironment(env);

//        TopologyBuilder builder = new TopologyBuilder();

        TridentTopology topology = new TridentTopology();
        FeederBatchSpout feederBatchSpout = new FeederBatchSpout(ImmutableList.of("a", "b", "c", "d"));

        Stream stream = topology.newStream("spout", feederBatchSpout);
        stream.shuffle().each(new Fields("a", "b"), new CheckEvenSumFilter()).parallelismHint(2);

//        LocalCluster localCluster = new LocalCluster();
//        localCluster.submitTopology("TridentTopologyApp", config, topology.build());

        feederBatchSpout.feed(ImmutableList.of(new Values(1, 2, 3, 4)));
        feederBatchSpout.feed(ImmutableList.of(new Values(2, 3, 4, 5)));
        feederBatchSpout.feed(ImmutableList.of(new Values(3, 4, 5, 6)));
        feederBatchSpout.feed(ImmutableList.of(new Values(5, 6, 7, 8)));
        feederBatchSpout.feed(ImmutableList.of(new Values(6, 7, 8, 9)));

//        Thread.sleep(10000);
        //Stop the topology
//        localCluster.shutdown();

        //submit to cluster
        StormSubmitter.submitTopology("LogAnalyserStorm", config, topology.build());
    }
}
