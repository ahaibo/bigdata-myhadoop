package com.hai.storm.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.io.Serializable;
import java.util.*;

public class HBaseOperations implements Serializable {
    private static final long serialVersionUID = 1L;
    // Instance of Hadoop Configuration class
    Configuration conf = null;
    Connection connection = null;
    HTable hTable = null;
    Table table = null;

    public HBaseOperations(/*String tableName, List<String> ColumnFamilies,*/ String zkString/*,List<String> zookeeperIPs, int zkPort*/) {
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", zkString);
        try {
            connection = ConnectionFactory.createConnection(conf);
        } catch (Exception e) {
            e.printStackTrace();
        }
        /*StringBuffer zookeeperIP = new StringBuffer();
        // Set the zookeeper nodes
        for (String zookeeper : zookeeperIPs) {
            zookeeperIP.append(zookeeper).append(",");
        }
        zookeeperIP.deleteCharAt(zookeeperIP.length() - 1);
        conf.set("hbase.zookeeper.quorum", zookeeperIP.toString());
        // Set the zookeeper client port
        conf.setInt("hbase.zookeeper.property.clientPort", zkPort);

        //s1:2181,s2:2181
        // call the createTable method to create a table into TxConstants.HBase.
        createTable(tableName, ColumnFamilies);
        try {
            // initialize the HTable.
            hTable = new HTable(conf, tableName);
        } catch (IOException e) {
            System.out.println("Error occurred while creating instanceof HTable class : " + e);
        }*/
    }

    /**
     * This method create a table into HBase
     *
     * @param tableName      Name of the HBase table
     * @param ColumnFamilies List of column families
     */
    public void createTable(String tableName, List<String> ColumnFamilies) {
        Admin admin = null;
        try {
            //admin = new HBaseAdmin(conf);
            admin = connection.getAdmin();
            // Set the input table in HTableDescriptor
            //HTableDescriptor tableDescriptor = new HTableDescriptor(Bytes.toBytes(tableName));
            HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
            for (String columnFamaliy : ColumnFamilies) {
                HColumnDescriptor columnDescriptor = new HColumnDescriptor(columnFamaliy);
                // add all the HColumnDescriptor into HTableDescriptor
                tableDescriptor.addFamily(columnDescriptor);
            }
            //execute the creaetTable(HTableDescriptor tableDescriptor) of HBaseAdmin class to createTable into HBase.
            admin.createTable(tableDescriptor);
            admin.close();
        } catch (TableExistsException tableExistsException) {
            System.out.println("Table already exist : " + tableName);
            if (admin != null) {
                try {
                    admin.close();
                } catch (IOException ioException) {
                    System.out.println("Error occurred while closing the HBaseAdmin connection :" + ioException);
                }
            }
        } catch (MasterNotRunningException e) {
            throw new RuntimeException("HBase master not running, table creation failed : ");
        } catch (ZooKeeperConnectionException e) {
            throw new RuntimeException("Zookeeper not running, table creation failed : ");
        } catch (IOException e) {
            throw new RuntimeException("IO error, table creation failed :");
        }

        try {
            // initialize the HTable.
            //hTable = new HTable(conf, tableName);
            table = connection.getTable(TableName.valueOf(tableName));
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    /**
     * This method insert the input record into HBase.
     *
     * @param record input record
     * @param rowId  unique id to identify each record uniquely.
     */
    public void insert(Map<String, Map<String, Object>> record, String rowId) {
        try {
            Put put = new Put(Bytes.toBytes(rowId));
            for (String cf : record.keySet()) {
                for (String column : record.get(cf).keySet()) {
                    //put.add(Bytes.toBytes(cf), Bytes.toBytes(column), Bytes.toBytes(record.get(cf).get(column).toString()));
                    put.addColumn(Bytes.toBytes(cf), Bytes.toBytes(column), Bytes.toBytes(record.get(cf).get(column).toString()));
                }
            }
            hTable.put(put);
        } catch (Exception e) {
            throw new RuntimeException("Error occurred while storing record into HBase");
        }
    }

    public static void main(String[] args) {
        List<String> cFs = new ArrayList<String>();
        cFs.add("cf1");
        cFs.add("cf2");

        //List<String> zks = new ArrayList<String>();
        //zks.add("127.0.0.1");

        Map<String, Map<String, Object>> record = new HashMap<String, Map<String, Object>>();
        Map<String, Object> cf1 = new HashMap<String, Object>();
        cf1.put("aa", "1");
        Map<String, Object> cf2 = new HashMap<String, Object>();
        cf2.put("bb", "1");
        record.put("cf1", cf1);
        record.put("cf2", cf2);

        String zkString = "s4:2181,s5:2181,s6:2181";
        HBaseOperations hbaseOperations = new HBaseOperations(zkString);
        //HBaseOperations hbaseOperations = new HBaseOperations("tableName", cFs, zks, 2181);
        hbaseOperations.createTable("test", cFs);
        hbaseOperations.insert(record, UUID.randomUUID().toString());
    }
}