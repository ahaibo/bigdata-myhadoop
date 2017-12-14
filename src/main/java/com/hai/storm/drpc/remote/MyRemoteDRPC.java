package com.hai.storm.drpc.remote;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.drpc.LinearDRPCTopologyBuilder;
import org.apache.storm.thrift.TException;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.DRPCClient;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by Administrator on 2017/4/13.
 */
public class MyRemoteDRPC {
    public static void main(String[] args) throws TException {
        Config conf = new Config();
        conf.setDebug(false);
        //org.apache.storm.security.auth.SimpleTransportPlugin is deprecated
        conf.put("storm.thrift.transport", "org.apache.storm.security.auth.plain.PlainSaslTransportPlugin");
        conf.put(Config.STORM_NIMBUS_RETRY_TIMES, 3);
        conf.put(Config.STORM_NIMBUS_RETRY_INTERVAL, 10);
        conf.put(Config.STORM_NIMBUS_RETRY_INTERVAL_CEILING, 20);
        conf.put(Config.DRPC_MAX_BUFFER_SIZE,1048576);
        conf.setNumWorkers(3);

        Map<String,String> env=new HashMap<String, String>();
        env.put("storm.zookeeper.servers","s4");
        env.put("drpc.servers","s4");
        conf.setEnvironment(env);

        LinearDRPCTopologyBuilder builder = new LinearDRPCTopologyBuilder("reach");
        builder.addBolt(new PartialUniquer(), 6).fieldsGrouping(new Fields("id", "follower"));

        StormSubmitter.submitTopology("exclamation-drpc", conf, builder.createRemoteTopology());

        DRPCClient client = new DRPCClient(conf, "s4", 3772);
        System.out.println(client.execute("reach", "remoteDRPCTest"));
    }
}
