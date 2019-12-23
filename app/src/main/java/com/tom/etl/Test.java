package com.tom.etl;

import org.apache.commons.lang.StringUtils;

import java.util.Map;
import java.util.TreeMap;

/**
 * ClassName: Test
 * Description:
 *
 * @author Mi_dad
 * @date 2019/12/21 16:13
 */
public class Test {
    public static void main(String[] args) {
        TreeMap<Long, String> map = new TreeMap<>();
        map.put(1L,"b");
        map.put(1L,"a");
        Map.Entry<Long, String> entry = map.firstEntry();
        System.out.println(map.size());
        System.out.println(map.firstKey()+map.get(map.firstKey()));
    }
}
