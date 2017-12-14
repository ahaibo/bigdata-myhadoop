package com.hai.zookeeper;


import com.alibaba.fastjson.JSONObject;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 * Created by as on 2017/3/31.
 */
public class ZooKeeperTest {

    private static final String CONNECTION_STRING = "s4:2181";
    private static final int CONNECTION_TIMEOUT = 5000;
    private final CountDownLatch connectedSignal = new CountDownLatch(1);
    private ZooKeeper zookeeper;
    private Stat stat;

    @Before
    public void connect() throws IOException, InterruptedException {

        zookeeper = new ZooKeeper(CONNECTION_STRING, CONNECTION_TIMEOUT, new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {
                if (watchedEvent.getState() == Event.KeeperState.SyncConnected) {
                    connectedSignal.countDown();
                }
            }
        });

        connectedSignal.await();
        System.out.println("zookeeper info:\n" + JSONObject.toJSONString(zookeeper, true));
    }

    @Test
    public void create() {
        for (int i = 1; i <= 500; i++) {
            toCreateNode("/hai/node" + i, ("node" + i + ".data").getBytes());
        }
    }

    private void toCreateNode(String path, byte[] data) {
        zookeeper.create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT, new AsyncCallback.StringCallback() {
            @Override
            public void processResult(int i, String s, Object o, String s1) {
                System.out.println("processResult: i = " + i + "; s = " + s + "; obj = " + o + "; str = " + s1);
            }
        }, null);
    }

    @Test
    public void listAllNodes() throws KeeperException, InterruptedException {
        getChildrenNodes("/");
    }

    private void getChildrenNodes(String path) throws KeeperException, InterruptedException {
        List<String> nodes = zookeeper.getChildren(path, false);
        if (null != nodes && nodes.size() > 0) {
            path = path.equals("/") ? path : path + "/";
            for (String node : nodes) {
                System.out.println(path + node);
                getChildrenNodes(path + node);
            }
        }
    }

    @Test
    public void getData() throws InterruptedException, KeeperException {
        toGetData("/storm");
    }

    public void toGetData(String path) throws InterruptedException, KeeperException {

        zookeeper.getData(path, false, stat);
        if (null != stat) {
            System.out.println("stat info:\n" + JSONObject.toJSONString(stat, true));
        }

    }

    public void toGetData2(String path) throws InterruptedException {

        zookeeper.getData(path, new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {
                if (watchedEvent.getState() == Event.KeeperState.SyncConnected) {
                    connectedSignal.countDown();
                }
            }
        }, new AsyncCallback.DataCallback() {
            @Override
            public void processResult(int i, String s, Object o, byte[] bytes, Stat stat) {
                System.out.println("i: " + i + "; s: " + s + JSONObject.toJSONString(o) + "; bytes: " + bytes + "; stat: " + JSONObject.toJSONString(stat));
            }
        }, null);

        connectedSignal.await();
    }

    @Test
    public void delete() throws KeeperException, InterruptedException {
        for (int i = 3; i < 497; i++) {
            String path = "/hai/node" + i;
            System.out.println("delete zk node: " + path);
            zookeeper.delete(path, 0, new AsyncCallback.VoidCallback() {
                @Override
                public void processResult(int i, String s, Object o) {
                    System.out.println("delete zk node result:");
                    System.out.println("index: " + i + "\ns: " + s + "\nobj:\n" + JSONObject.toJSONString(o, true));
                }
            }, null);
        }
    }

    @After
    public void close() throws InterruptedException {
        if (null != zookeeper) {
            zookeeper.close();
        }
    }
}
