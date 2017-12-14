package com.hai;

import org.junit.Test;

import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 * Created by as on 2017/4/11.
 */
public class MyTest {

    @Test
    public void listSysProps() {
        Properties props = System.getProperties();
        Set<Map.Entry<Object, Object>> entries = props.entrySet();
        for (Map.Entry<Object, Object> entry : entries) {
            System.out.println(entry.getKey() + " = " + entry.getValue());
        }
    }
}
