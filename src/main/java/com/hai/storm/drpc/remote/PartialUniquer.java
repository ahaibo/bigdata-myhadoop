package com.hai.storm.drpc.remote;

import org.apache.storm.coordination.BatchOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBatchBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class PartialUniquer extends BaseBatchBolt {
    private BatchOutputCollector _collector;
    private Object _id;
    private Set<String> _followers = new HashSet<String>();

    @Override
    public void prepare(Map conf, TopologyContext context, BatchOutputCollector collector, Object id) {
        _collector = collector;
        _id = id;
    }

    @Override
    public void execute(Tuple tuple) {
        _followers.add(tuple.getString(1));
    }

    @Override
    public void finishBatch() {
        _collector.emit(new Values(_id, _followers.size()));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("id", "partial-count"));
    }
}