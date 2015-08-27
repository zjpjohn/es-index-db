package com.wxingyl.es.util;

import com.wxingyl.es.jdal.DbTableDesc;
import com.wxingyl.es.jdal.DbTableFieldDesc;

import java.util.*;

/**
 * Created by xing on 15/8/17.
 * 常用的工具类
 */
public abstract class CommonUtils {

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

    public static DbTableDesc getDbTable(Map<String, Object> map, String key, String defaultSchema) {
        String value = getStringVal(map, key);
        if (value == null) return null;
        int index;
        if ((index = value.indexOf('.')) > 0) {
            return DbTableDesc.build(value.substring(0, index), value.substring(index+1));
        } else {
            return DbTableDesc.build(defaultSchema, value);
        }
    }

    public static DbTableFieldDesc getDbTableField(String value, String defaultSchema, String defaultTable) {
        int index;
        String field = null;
        if ((index = value.lastIndexOf('.')) > 0) {
            field = value.substring(index+1);
            value = value.substring(0, index);
            defaultTable = value;
        }
        if ((index = value.lastIndexOf('.')) > 0) {
            defaultTable = value.substring(index+1);
            defaultSchema = value.substring(0, index);
        }
        return DbTableFieldDesc.build(defaultSchema, defaultTable, field);
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
