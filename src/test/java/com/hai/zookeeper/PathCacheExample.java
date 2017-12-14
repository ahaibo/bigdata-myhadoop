package com.hai.zookeeper;

/***
 *
 可以利用ZooKeeper在集群的各个节点之间缓存数据。 每个节点都可以得到最新的缓存的数据。 Curator提供了三种类型的缓存方式：Path Cache,Node Cache 和Tree Cache。

 Path Cache
 Path Cache用来监控一个ZNode的子节点. 当一个子节点增加， 更新，删除时， Path Cache会改变它的状态， 会包含最新的子节点， 子节点的数据和状态。 这也正如它的名字表示的那样， 那监控path。

 实际使用时会涉及到四个类：

 PathChildrenCache
 PathChildrenCacheEvent
 PathChildrenCacheListener
 ChildData

 通过下面的构造函数创建Path Cache:

 public PathChildrenCache(CuratorFramework client, String path, boolean cacheData)
 想使用cache，必须调用它的start方法，不用之后调用close方法。 start有两个， 其中一个可以传入StartMode，用来为初始的cache设置暖场方式(warm)：

 NORMAL: 初始时为空。
 BUILD_INITIAL_CACHE: 在这个方法返回之前调用rebuild()。
 POST_INITIALIZED_EVENT: 当Cache初始化数据后发送一个PathChildrenCacheEvent.Type#INITIALIZED事件
 public void addListener(PathChildrenCacheListener listener)可以增加listener监听缓存的改变。

 getCurrentData()方法返回一个List<ChildData>对象，可以遍历所有的子节点。

 这个例子摘自官方的例子， 实现了一个控制台的方式操作缓存。 它提供了三个命令， 你可以在控制台中输入。

 set 用来新增或者更新一个子节点的值， 也就是更新一个缓存对象
 remove 是删除一个缓存对象
 list 列出所有的缓存对象
 另外还提供了一个help命令提供帮助。

 设置/更新、移除其实是使用client (CuratorFramework)来操作, 不通过PathChildrenCache操作：

 client.setData().forPath(path, bytes);
 client.create().creatingParentsIfNeeded().forPath(path, bytes);
 client.delete().forPath(path);
 而查询缓存使用下面的方法：

 for (ChildData data : cache.getCurrentData()) {
 System.out.println(data.getPath() + " = " + new String(data.getData()));
 }
 *
 */
public class PathCacheExample {
    public static void main(String[] args) {
        System.out.println("zookeeper path cache example...");
    }
}
