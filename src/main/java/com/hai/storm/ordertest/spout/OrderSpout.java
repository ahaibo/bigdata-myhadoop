package com.hai.storm.ordertest.spout;

import com.hai.storm.ordertest.common.ConfKeys;
import com.hai.storm.ordertest.common.FieldNames;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import redis.clients.jedis.Jedis;

import java.util.Map;

/**
 * Created by as on 2017/4/2.
 */
public class OrderSpout extends BaseRichSpout {
    private static final long serialVersionUID = 8653028208532925421L;

    private Jedis jedis;
    private String host;
    private int port;
    private SpoutOutputCollector collector;

    @Override
    public void open(Map conf, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.collector = spoutOutputCollector;
        host = conf.get(ConfKeys.REDIS_HOST).toString();
        port = Integer.parseInt(conf.get(ConfKeys.REDIS_PORT).toString());
        connectRedis();
    }

    @Override
    public void nextTuple() {
        String content = jedis.rpop(FieldNames.ORDERS);
        if (null == content || FieldNames.NIL.equals(content)) {
            try {
                Thread.sleep(300);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        } else {
            JSONObject obj = (JSONObject) JSONValue.parse(content);
            String id = (String) obj.get(FieldNames.ID);
            JSONArray items = (JSONArray) obj.get(FieldNames.ITEMS);

            for (Object itemObj : items) {
                JSONObject item = (JSONObject) itemObj;
                String name = (String) item.get(FieldNames.NAME);
                Integer count = Integer.parseInt((String) item.get(FieldNames.COUNT));
                collector.emit(new Values(id, name, count));

                if (jedis.hexists(FieldNames.ITEM_COUNTS, name)) {
                    jedis.hincrBy(FieldNames.ITEM_COUNTS, name, 1);
                } else {
                    jedis.hset(FieldNames.ITEM_COUNTS, name, "1");
                }
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields(
                FieldNames.ID,
                FieldNames.NAME,
                FieldNames.COUNT
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
