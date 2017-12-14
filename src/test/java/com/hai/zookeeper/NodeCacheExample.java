package com.hai.zookeeper;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.framework.recipes.cache.NodeCacheListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.apache.curator.utils.CloseableUtils;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Arrays;

/***
 *
 Node Cache
 Path Cache用来监控一个ZNode. 当节点的数据修改或者删除时，Node Cache能更新它的状态包含最新的改变。

 涉及到下面的三个类：

 NodeCache
 NodeCacheListener
 ChildData
 想使用cache，依然要调用它的start方法，不用之后调用close方法。

 getCurrentData()将得到节点当前的状态，通过它的状态可以得到当前的值。 可以使用public void addListener(NodeCacheListener listener)监控节点状态的改变。
 *
 */
public class NodeCacheExample {
    private static final String PATH = "/example/nodeCache";

    public static void main(String[] args) throws Exception {

        TestingServer server = new TestingServer();
        CuratorFramework client = null;
        NodeCache cache = null;
        try {
            RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
            client = CuratorFrameworkFactory.newClient(server.getConnectString(), retryPolicy);
            client.start();

            cache = new NodeCache(client, PATH);
            cache.start();
            processCommonds(client, cache);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            CloseableUtils.closeQuietly(cache);
            CloseableUtils.closeQuietly(client);
            CloseableUtils.closeQuietly(server);
        }
    }

    private static void processCommonds(CuratorFramework client, NodeCache cache) throws Exception {
        printHelp();
        addListenner(cache);
        BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
        boolean done = false;
        while (!done) {
            System.out.print("> ");
            String line = in.readLine();
            if (line == null) {
                break;
            }
            String command = line.trim();
            String[] parts = command.split("\\s");
            if (parts.length == 0) {
                continue;
            }
            String operation = parts[0];
            String args[] = Arrays.copyOfRange(parts, 1, parts.length);
            if (operation.equalsIgnoreCase("help") || operation.equalsIgnoreCase("?")) {
                printHelp();
            } else if (operation.equalsIgnoreCase("q") || operation.equalsIgnoreCase("quit")) {
                done = true;
            } else if (operation.equals("set")) {
                setValue(client, command, args);
            } else if (operation.equals("remove")) {
                remove(client);
            } else if (operation.equals("show")) {
                show(cache);
            }
            Thread.sleep(1000); // just to allow the console output to catch
            // up
        }
    }

    private static void addListenner(final NodeCache cache) {
        //changes
        NodeCacheListener listener = new NodeCacheListener() {
            @Override
            public void nodeChanged() throws Exception {
                if (cache.getCurrentData() != null) {
                    System.out.println("Node changed: " + cache.getCurrentData().getPath() + ", value: " + new String(cache.getCurrentData().getData()));
                }
            }
        };
        cache.getListenable().addListener(listener);
    }

    private static void setValue(CuratorFramework client, String command, String[] args) throws Exception {
        if (args.length != 1) {
            System.err.println("syntax error (expected set <value>): " + command);
            return;
        }
        byte[] bytes = args[0].getBytes();
        try {
            client.setData().forPath(PATH, bytes);
        } catch (Exception e) {
            client.create().creatingParentsIfNeeded().forPath(PATH, bytes);
        }
    }

    private static void remove(CuratorFramework client) throws Exception {
        client.delete().forPath(PATH);
    }

    private static void show(NodeCache cache) {
        if (cache.getCurrentData() != null)
            System.out.println(cache.getCurrentData().getPath() + " = " + new String(cache.getCurrentData().getData()));
        else
            System.out.println("cache don't set a value");
    }

    private static void printHelp() {
        System.out.println("An example of using PathChildrenCache. This example is driven by entering commands at the prompt:\n");
        System.out.println("set <value>: Adds or updates a node with the given name");
        System.out.println("remove: Deletes the node with the given name");
        System.out.println("show: Display the node's value in the cache");
        System.out.println("quit: Quit the example");
        System.out.println();
    }
}
