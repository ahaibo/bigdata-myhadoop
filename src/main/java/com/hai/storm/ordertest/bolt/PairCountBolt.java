package com.hai.storm.ordertest.bolt;

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

import java.util.Map;

/**
 * Created by as on 2017/4/2.
 */
public class PairCountBolt extends BaseRichBolt {
    private static final long serialVersionUID = -1327570173269957879L;

    private OutputCollector collector;
    private Map<ItemPair, Integer> pairCounts;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
        this.pairCounts = new HashedMap();
    }

    @Override
    public void execute(Tuple tuple) {
        String item1 = tuple.getStringByField(FieldNames.ITEM1);
        String item2 = tuple.getStringByField(FieldNames.ITEM2);

        ItemPair itemPair = new ItemPair(item1, item2);
        int pairCount = 0;
        if (pairCounts.containsKey(itemPair)) {
            pairCount = pairCounts.get(itemPair);
        }
        pairCount++;
        pairCounts.put(itemPair, pairCount);

        collector.emit(new Values(item1, item1, pairCount));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields(
                FieldNames.ITEM1,
                FieldNames.ITEM2,
                FieldNames.PAIR_COUNT
        ));
    }
}
