package com.hai.storm.ordertest.bolt;

import com.hai.storm.ordertest.common.ConfKeys;
import com.hai.storm.ordertest.common.FieldNames;
import com.hai.storm.ordertest.common.ItemPair;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.json.simple.JSONObject;
import redis.clients.jedis.Jedis;

import java.util.Map;

/**
 * Created by as on 2017/4/2.
 */
public class FilterBolt extends BaseRichBolt {
    private static final long serialVersionUID = -1236307318929782412L;

    private static final double SUPPORT_THRESHOLD = 0.01;
    private static final double CONFIDENCE_THRESHOLD = 0.01;

    private OutputCollector collector;
    private String host;
    private int port;
    private Jedis jedis;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        this.host = (String) stormConf.get(ConfKeys.REDIS_HOST);
        this.port = Integer.valueOf((String) stormConf.get(ConfKeys.REDIS_PORT));
        connectRedis();
    }

    @Override
    public void execute(Tuple input) {
        String item1 = input.getStringByField(FieldNames.ITEM1);
        String item2 = input.getStringByField(FieldNames.ITEM2);
        ItemPair itemPair = new ItemPair(item1, item2);
        String pairString = itemPair.toString();

        double support = 0;
        double confidence = 0;
        if (input.getFields().get(2).equals(FieldNames.SUPPORT)) {
            support = input.getDoubleByField(FieldNames.SUPPORT);
            jedis.hset(FieldNames.SUPPORT, pairString, String.valueOf(support));
        } else if (input.getFields().get(2).equals(FieldNames.CONFIDENCE)) {
            confidence = input.getDoubleByField(FieldNames.CONFIDENCE);
            jedis.hset(FieldNames.CONFIDENCE, pairString, String.valueOf(confidence));
        }

        if (!jedis.hexists(FieldNames.SUPPORTS, pairString) || !jedis.hexists(FieldNames.CONFIDENCES, pairString)) {
            return;
        }

        support = Double.parseDouble(jedis.hget(FieldNames.SUPPORTS, pairString));
        confidence = Double.parseDouble(jedis.hget(FieldNames.CONFIDENCES, pairString));

        if (support >= SUPPORT_THRESHOLD && confidence >= CONFIDENCE_THRESHOLD) {
            JSONObject pairValue = new JSONObject();
            pairValue.put(FieldNames.SUPPORT, support);
            pairValue.put(FieldNames.CONFIDENCE, confidence);
            jedis.hset(FieldNames.RECOMMENDED_PAIRS, pairString, pairValue.toJSONString());

            collector.emit(new Values(item1, item2, support, confidence));
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(
                FieldNames.ITEM1,
                FieldNames.ITEM2,
                FieldNames.SUPPORT,
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
