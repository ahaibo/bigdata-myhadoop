package com.hai.zookeeper;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.TreeCache;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent;
import org.apache.curator.framework.recipes.cache.TreeCacheListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.apache.curator.utils.CloseableUtils;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.KeeperException;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.Map;

/***
 Tree Node
 这种类型的即可以监控节点的状态，还监控节点的子节点的状态， 类似上面两种cache的组合。 这也就是Tree的概念。 它监控整个树中节点的状态。 涉及到下面四个类。

 TreeCache
 TreeCacheListener
 TreeCacheEvent
 ChildData
 而关键的TreeCache的构造函数为

 public TreeCache(CuratorFramework client, String path, boolean cacheData)
 想使用cache，依然要调用它的start方法，不用之后调用close方法。

 getCurrentChildren()返回cache的状态，类型为Map<String, ChildData>。 而getCurrentData()返回监控的path的数据。

 public void addListener(TreeCacheListener listener)可以增加listener来监控状态的改变。
 *
 */
public class TreeCacheExample {
    private static final String PATH = "/example/treeCache";

    public static void main(String[] args) throws Exception {
        TestingServer server = new TestingServer();
        CuratorFramework client = null;
        TreeCache cache = null;
        try {
            client = CuratorFrameworkFactory.newClient(server.getConnectString(), new ExponentialBackoffRetry(1000, 3));
            client.start();

            cache = new TreeCache(client, PATH);
            cache.start();
            processCommands(client, cache);
        } finally {
            CloseableUtils.closeQuietly(cache);
            CloseableUtils.closeQuietly(client);
            CloseableUtils.closeQuietly(server);
        }
    }

    private static void addListener(final TreeCache cache) {
        TreeCacheListener listener = new TreeCacheListener() {

            @Override
            public void childEvent(CuratorFramework client, TreeCacheEvent event) throws Exception {
                switch (event.getType()) {
                    case NODE_ADDED: {
                        System.out.println("TreeNode added: " + ZKPaths.getNodeFromPath(event.getData().getPath()) + ", value: "
                                + new String(event.getData().getData()));
                        break;
                    }
                    case NODE_UPDATED: {
                        System.out.println("TreeNode changed: " + ZKPaths.getNodeFromPath(event.getData().getPath()) + ", value: "
                                + new String(event.getData().getData()));
                        break;
                    }
                    case NODE_REMOVED: {
                        System.out.println("TreeNode removed: " + ZKPaths.getNodeFromPath(event.getData().getPath()));
                        break;
                    }
                    default:
                        System.out.println("Other event: " + event.getType().name());
                }
            }

        };

        cache.getListenable().addListener(listener);
    }

    private static void processCommands(CuratorFramework client, TreeCache cache) throws Exception {
        // More scaffolding that does a simple command line processor
        printHelp();
        addListener(cache);
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
                remove(client, command, args);
            } else if (operation.equals("list")) {
                list(cache);
            }
            Thread.sleep(1000); // just to allow the console output to catch
            // up
        }
    }

    private static void list(TreeCache cache) {
        if (cache.getCurrentChildren(PATH).size() == 0) {
            System.out.println("* empty *");
            return;
        }
        for (Map.Entry<String, ChildData> entry : cache.getCurrentChildren(PATH).entrySet()) {
            System.out.println(entry.getKey() + " = " + new String(entry.getValue().getData()));
        }
    }

    private static void remove(CuratorFramework client, String command, String[] args) throws Exception {
        if (args.length != 1) {
            System.err.println("syntax error (expected remove <path>): " + command);
            return;
        }
        String name = args[0];
        if (name.contains("/")) {
            System.err.println("Invalid node name" + name);
            return;
        }
        String path = ZKPaths.makePath(PATH, name);
        client.delete().forPath(path);
    }

    private static void setValue(CuratorFramework client, String command, String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("syntax error (expected set <path> <value>): " + command);
            return;
        }
        String name = args[0];
        if (name.contains("/")) {
            System.err.println("Invalid node name" + name);
            return;
        }
        String path = ZKPaths.makePath(PATH, name);
        byte[] bytes = args[1].getBytes();
        try {
            client.setData().forPath(path, bytes);
        } catch (KeeperException.NoNodeException e) {
            client.create().creatingParentsIfNeeded().forPath(path, bytes);
        }
    }

    private static void printHelp() {
        System.out.println("An example of using PathChildrenCache. This example is driven by entering commands at the prompt:\n");
        System.out.println("set <name> <value>: Adds or updates a node with the given name");
        System.out.println("remove <name>: Deletes the node with the given name");
        System.out.println("list: List the nodes/values in the cache");
        System.out.println("quit: Quit the example");
        System.out.println();
    }
}