package com.wxingyl.es.util;

import org.apache.commons.dbutils.handlers.MapListHandler;

import java.util.*;
import java.util.function.BiConsumer;

/**
 * Created by xing on 15/8/17.
 * 常用的工具类
 */
public class CommonUtils {

    public static boolean isEmpty(String s) {
        return s == null || s.isEmpty() || s.trim().isEmpty();
    }

    public static boolean isEmpty(Collection collection) {
        return collection == null || collection.isEmpty();
    }

    public static boolean isEmpty(Map map) {
        return map == null || map.isEmpty();
    }

    public static <E> void addAll(Collection<E> src, Collection<? extends E> adds) {
        if (isEmpty(adds)) return;
        src.addAll(adds);
    }

    public static <T> Set<T> getSet(Map<String, Object> map, String key) {
        List<T> list = getList(map, key);
        if (list != null) return new HashSet<>(list);
        else return null;
    }

    @SuppressWarnings("unchecked")
    public static <T> List<T> getList(Map<String, Object> map, String key) {
        Object obj = map.get(key);
        if (obj == null) return null;
        List<T> list;
        if (obj instanceof List) {
            list = (List<T>) obj;
        } else {
            list = new ArrayList<>();
            list.add((T) obj);
        }
        if (list.isEmpty()) {
            return null;
        } else if (list.get(0) instanceof String) {
            final List<T> ret = new ArrayList<>(list.size());
            list.forEach(v -> {
                String val = v.toString();
                if (!isEmpty(val)) ret.add((T) val.trim());
            });
            return ret;
        } else {
            return list;
        }
    }

    public static String getStringVal(Map<String, Object> map, String key) {
        String str = (String) map.get(key);
        if (isEmpty(str)) {
            return null;
        } else {
            return str.trim();
        }
    }

    public static <T> RwLock<T> createRwLock(T lockObj) {
        return new RwLock<>(lockObj);
    }
}
