package com.hai.storm.trident.example;

import com.alibaba.fastjson.JSONObject;
import org.apache.storm.trident.operation.BaseFilter;
import org.apache.storm.trident.tuple.TridentTuple;

public class CheckEvenSumFilter extends BaseFilter {
    private static final long serialVersionUID = 7L;

    public boolean isKeep(TridentTuple tuple) {
        int number1 = tuple.getInteger(0);
        int number2 = tuple.getInteger(1);
        int sum = number1 + number2;
        System.out.println("isKeep: " + JSONObject.toJSONString(tuple));
        if (sum % 2 == 0) {
            return true;
        }
        return false;
    }
}