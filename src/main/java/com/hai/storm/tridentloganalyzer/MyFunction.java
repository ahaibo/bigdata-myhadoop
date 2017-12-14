package com.hai.storm.tridentloganalyzer;

import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Values;

public class MyFunction extends BaseFunction {
    public void execute(TridentTuple tuple, TridentCollector collector) {
        int a = tuple.getInteger(0);
        int b = tuple.getInteger(1);
        collector.emit(new Values(a + b));
    }
}