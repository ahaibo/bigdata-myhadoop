package com.hai.storm.hbase;

import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.IBasicBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class StormHBaseBolt implements IBasicBolt {
    private static final long serialVersionUID = 2L;
    private HBaseOperations hbaseOperations;
    private String tableName;
    private List<String> columnFamilies;
    private String zkString;
    private List<String> zookeeperIPs;
    private int zkPort;

    /**
     * Constructor of StormHBaseBolt class
     *
     * @param tableName      HBaseTableNam
     * @param columnFamilies List of column families
     * @param zkString       like "s1:2181,s2:2181..."
     */
    public StormHBaseBolt(String tableName, List<String> columnFamilies, String zkString) {
        this.tableName = tableName;
        this.columnFamilies = columnFamilies;
        this.zkString = zkString;
    }

    public StormHBaseBolt(String tableName, List<String> columnFamilies, List<String> zookeeperIPs, int zkPort) {
        this.tableName = tableName;
        this.columnFamilies = columnFamilies;
        this.zookeeperIPs = zookeeperIPs;
        this.zkPort = zkPort;
    }

    public void execute(Tuple input, BasicOutputCollector
            collector) {
        Map<String, Map<String, Object>> record = new HashMap<String, Map<String, Object>>();
        Map<String, Object> personalMap = new HashMap<String, Object>();
        personalMap.put("firstName", input.getValueByField("firstName"));
        personalMap.put("lastName", input.getValueByField("lastName"));
        Map<String, Object> companyMap = new HashMap<String, Object>();
        companyMap.put("companyName", input.getValueByField("companyName"));
        record.put("personal", personalMap);
        record.put("company", companyMap);
        // call the inset method of HBaseOperations class to insert record into HBase
        hbaseOperations.insert(record, UUID.randomUUID().toString());
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        // create the instance of HBaseOperations class
        hbaseOperations = new HBaseOperations(zkString);
    }

    @Override
    public void cleanup() {
        // TODO Auto-generated method stub
    }
}