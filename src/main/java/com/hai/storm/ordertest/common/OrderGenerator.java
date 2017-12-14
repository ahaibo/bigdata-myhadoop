package com.hai.storm.ordertest.common;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import redis.clients.jedis.Jedis;

import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.UUID;

/**
 * Created by as on 2017/4/2.
 */
public class OrderGenerator {
    private static final String REDIS_HOST = "localhost";
    private static final int REDIS_PORT = 6379;
    private static final int ORDER_COUNT = 30;
    private static final int RANDOM_SEED = 1000;
    private static final String ITEMS_NAME[] = {
            "milk", "coffee", "egg", "flower", "icecream", "wine", "water", "fish", "golf", "CD", "beer"
    };
    private static Jedis jedis;
    private static Random random;

    public static void main(String[] args) {
        prepareRandom();
        connectRedis();
        pushTuples();
        disconnectFromRedis();
    }

    private static void prepareRandom() {
        System.out.println("prepared random...");
        random = new Random(RANDOM_SEED);
    }

    private static void connectRedis() {
        System.out.println("connect redis...");
        jedis = new Jedis(REDIS_HOST, REDIS_PORT);
        jedis.connect();
    }

    private static void pushTuples() {
        System.out.println("push tuples...");
        for (int i = 0; i < ORDER_COUNT; i++) {
            JSONObject orderTuple = new JSONObject();

            JSONArray items = new JSONArray();
            Set<String> selectedItems = new HashSet<String>();
            for (int j = 0; j < 5; j++) {
                JSONObject item = new JSONObject();
                while (true) {
                    int itemIndex = random.nextInt(ITEMS_NAME.length);
                    String itemName = ITEMS_NAME[itemIndex];

                    if (!selectedItems.contains(itemName)) {
                        item.put(FieldNames.NAME, itemName);
                        item.put(FieldNames.COUNT, random.nextInt(RANDOM_SEED));
                        items.add(item);
                        selectedItems.add(itemName);
                        break;
                    }
                }
            }

            orderTuple.put(FieldNames.ID, UUID.randomUUID().toString());
            orderTuple.put(FieldNames.ITEMS, items);

            String orderJsonString = orderTuple.toJSONString();
            jedis.rpush(FieldNames.ORDERS, orderJsonString);
        }

    }

    private static void disconnectFromRedis() {
        System.out.println("disconnect redis...");
        jedis.disconnect();
    }
}
