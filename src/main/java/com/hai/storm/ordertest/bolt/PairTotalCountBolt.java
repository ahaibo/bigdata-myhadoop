package com.hai.storm.ordertest.bolt;

import com.hai.storm.ordertest.common.FieldNames;
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
public class PairTotalCountBolt extends BaseRichBolt {
    private static final long serialVersionUID = 1682784974733182350L;

    private OutputCollector collector;
    private int totalCount;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
        this.totalCount = 0;
    }

    @Override
    public void execute(Tuple tuple) {
        this.totalCount++;
        collector.emit(new Values(totalCount));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields(FieldNames.TOTAL_COUNT));
    }
}
