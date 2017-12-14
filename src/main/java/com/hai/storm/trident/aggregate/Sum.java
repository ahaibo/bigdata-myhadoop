package com.hai.storm.trident.aggregate;

import clojure.lang.Numbers;
import org.apache.storm.trident.operation.CombinerAggregator;
import org.apache.storm.trident.tuple.TridentTuple;

public class Sum implements CombinerAggregator<Number> {
    private static final long serialVersionUID = 1L;

    public Number init(TridentTuple tridentTuple) {
        return (Number) tridentTuple.getValue(0);
    }

    public Number combine(Number number1, Number number2) {
        return Numbers.add(number1, number2);
    }

    public Number zero() {
        return 0;
    }
}