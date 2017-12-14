package com.hai.storm.ordertest.bolt;

import com.hai.storm.ordertest.common.FieldNames;
import org.apache.commons.collections.map.HashedMap;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by as on 2017/4/2.
 */
public class SplitBolt extends BaseRichBolt {

    private static final long serialVersionUID = -7224995076612944536L;
    private OutputCollector collector;
    private Map<String, List<String>> orderItems;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
        orderItems = new HashedMap();
    }

    @Override
    public void execute(Tuple tuple) {
        String id = tuple.getStringByField(FieldNames.ID);
        String newItem = tuple.getStringByField(FieldNames.NAME);
        if (!orderItems.containsKey(id)) {
            ArrayList<String> items = new ArrayList<String>();
            items.add(newItem);
            orderItems.put(id, items);
            return;
        }

        List<String> items = orderItems.get(id);
        for (String existItme : items) {
            collector.emit(createPair(newItem, existItme));
        }
        items.add(newItem);
    }

    private Values createPair(String newItem, String existItme) {
        Values values = null;
        if (newItem.compareTo(existItme) > 0) {
            values = new Values(newItem, existItme);
        } else {
            values = new Values(existItme, newItem);
        }
        return values;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields(
                FieldNames.ITEM1,
                FieldNames.ITEM2
        ));
    }
}
