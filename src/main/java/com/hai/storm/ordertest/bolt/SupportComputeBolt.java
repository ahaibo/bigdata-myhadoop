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
public class SupportComputeBolt extends BaseRichBolt {
    private static final long serialVersionUID = -1307599842801620142L;

    private OutputCollector collector;
    private Map<ItemPair, Integer> pairCounts;
    private int pairTotalCount;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        this.pairCounts = new HashedMap();
        this.pairTotalCount = 0;
    }

    @Override
    public void execute(Tuple input) {
        if (input.getFields().get(0).equals(FieldNames.TOTAL_COUNT)) {
            pairTotalCount = input.getIntegerByField(FieldNames.TOTAL_COUNT);
        } else if (input.getFields().size() == 3) {

            String item1 = input.getStringByField(FieldNames.ITEM1);
            String item2 = input.getStringByField(FieldNames.ITEM2);
            int pairCount = input.getIntegerByField(FieldNames.TOTAL_COUNT);
            pairCounts.put(new ItemPair(item1, item2), pairCount);

        } else if (input.getFields().get(0).equals(FieldNames.COMMAND)) {

            for (ItemPair itemPair : pairCounts.keySet()) {
                double itemSupport = (double) (pairCounts.get(itemPair).intValue()) / pairTotalCount;
                collector.emit(new Values(itemPair.getItem1(), itemPair.getItem2(), itemSupport));
            }

        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(
                FieldNames.ITEM1,
                FieldNames.ITEM2,
                FieldNames.SUPPORT
        ));
    }
}
