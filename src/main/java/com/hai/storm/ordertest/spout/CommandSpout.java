package com.hai.storm.ordertest.spout;

import com.hai.storm.ordertest.common.FieldNames;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.Map;

/**
 * Created by as on 2017/4/2.
 */
public class CommandSpout extends BaseRichSpout {
    private static final long serialVersionUID = -5432060444976343024L;

    private SpoutOutputCollector collector;

    @Override
    public void open(Map conf, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.collector = spoutOutputCollector;
    }

    @Override
    public void nextTuple() {
        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        collector.emit(new Values(FieldNames.STATISTIC));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields(FieldNames.COMMAND));
    }
}
