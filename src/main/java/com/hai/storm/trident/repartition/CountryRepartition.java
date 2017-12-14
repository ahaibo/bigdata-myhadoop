package com.hai.storm.trident.repartition;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.storm.generated.GlobalStreamId;
import org.apache.storm.grouping.CustomStreamGrouping;
import org.apache.storm.task.WorkerTopologyContext;
import org.apache.storm.trident.operation.BaseAggregator;
import org.apache.storm.trident.operation.ReducerAggregator;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Values;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

public class CountryRepartition implements CustomStreamGrouping, Serializable {

    private static final long serialVersionUID = 1L;
    private static final Map<String, Integer> countries =
            ImmutableMap
                    .of(
                            "India", 0,
                            "Japan", 1,
                            "United State", 2,
                            "China", 3,
                            "Brazil", 4
                    );
    private int tasks = 0;

    public void prepare(WorkerTopologyContext context, GlobalStreamId stream, List<Integer> targetTasks) {
        tasks = targetTasks.size();
    }

    public List<Integer> chooseTasks(int taskId, List<Object> values) {
        String country = (String) values.get(0);
        return ImmutableList.of(countries.get(country) % tasks);
    }

    /**
     * 自定义聚合函数
     */
    public static class Sum implements ReducerAggregator<Long> {
        private static final long serialVersionUID = 1L;

        //return the initial value zero
        public Long init() {
            return 0L;
        }

        //Iterates on the input tuples, calculate the sum and
        //produce the single tuple with single field as output
        public Long reduce(Long curr, TridentTuple tuple) {
            return curr + tuple.getLong(0);
        }
    }

    public static class SumAsAggregator extends BaseAggregator<SumAsAggregator.State> {
        private static final long serialVersionUID = 1L;

        // state class
        static class State {
            long count = 0;
        }

        // Initialize the state
        public State init(Object batchId, TridentCollector collector) {
            return new State();
        }

        // Maintain the state of sum into count variable.
        public void aggregate(State state, TridentTuple tridentTuple, TridentCollector tridentCollector) {
            state.count = tridentTuple.getLong(0) + state.count;
        }

        // return a tuple with single value as output
        // after processing all the tuples of given batch.
        public void complete(State state, TridentCollector tridentCollector) {
            tridentCollector.emit(new Values(state.count));
        }
    }

}
