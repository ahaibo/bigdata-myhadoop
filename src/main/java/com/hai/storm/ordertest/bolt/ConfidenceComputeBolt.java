package com.hai.storm.ordertest.bolt;

import com.hai.storm.ordertest.common.ConfKeys;
import com.hai.storm.ordertest.common.FieldNames;
import com.hai.storm.ordertest.common.ItemPair;
import org.apache.commons.collections.map.HashedMap;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import redis.clients.jedis.Jedis;

import java.util.Map;

/**
 * Created by as on 2017/4/2.
 */
public class ConfidenceComputeBolt extends BaseRichBolt {
    private static final long serialVersionUID = -4892618770764171808L;

    private OutputCollector collector;
    private Jedis jedis;
    private String host;
    private int port;
    private Map<ItemPair, Integer> pairCounts;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        this.host = (String) stormConf.get(ConfKeys.REDIS_HOST);
        this.port = Integer.valueOf((String) stormConf.get(ConfKeys.REDIS_PORT));
        this.pairCounts = new HashedMap();
        connectRedis();
    }

    @Override
    public void execute(Tuple input) {
        if (input.getFields().size() == 3) {

            String item1 = input.getStringByField(FieldNames.ITEM1);
            String item2 = input.getStringByField(FieldNames.ITEM2);
            int pairCount = input.getIntegerByField(FieldNames.PAIR_COUNT);
            pairCounts.put(new ItemPair(item1, item2), pairCount);

        } else if (input.getFields().get(0).equals(FieldNames.COMMAND)) {

            for (ItemPair itemPair : pairCounts.keySet()) {
                int item1Count = Integer.parseInt(jedis.hget("itemCounts", itemPair.getItem1()));
                int item2Count = Integer.parseInt(jedis.hget("itemCounts", itemPair.getItem2()));
                double itemConfidence = pairCounts.get(itemPair).intValue();
                if (item1Count < item2Count) {
                    itemConfidence /= item1Count;
                } else {
                    itemConfidence /= item2Count;
                }
                collector.emit(new Values(itemPair.getItem1(), itemPair.getItem2(), itemConfidence));
            }

        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(
                FieldNames.ITEM1,
                FieldNames.ITEM2,
                FieldNames.CONFIDENCE
        ));
    }

    private void connectRedis() {
        System.out.println("connect redis...");
        jedis = new Jedis(host, port);
        jedis.connect();
    }

    private void disconnectFromRedis() {
        System.out.println("disconnect redis...");
        jedis.disconnect();
    }
}
