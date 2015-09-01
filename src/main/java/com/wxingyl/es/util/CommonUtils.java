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

    public static <T> List<List<T>> groupList(Collection<T> collection, int pageSize) {
        List<List<T>> ret = new ArrayList<>(collection.size() / pageSize + 1);
        ArrayList<T> list = new ArrayList<>(pageSize);
        for (T t : collection) {
            list.add(t);
            if (list.size() == pageSize) {
                ret.add(list);
                list = new ArrayList<>(pageSize);
            }
        }
        if (!list.isEmpty()) {
            list.trimToSize();
            ret.add(list);
        }
        return ret;
    }

    public static <E> void addAll(Collection<E> src, Collection<? extends E> adds) {
        if (isEmpty(adds)) return;
        src.addAll(adds);
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

    public static DbTableDesc getDbTable(Map<String, Object> map, String key) {
        String value = getStringVal(map, key);
        if (value == null) return null;
        int index;
        if ((index = value.indexOf('.')) > 0) {
            return new DbTableDesc(null, value.substring(0, index), value.substring(index + 1));
        } else {
            return new DbTableDesc(null, null, value);
        }
    }

    public static DbTableFieldDesc getDbTableField(String value, DbTableDesc defaultTable) {
        int index;
        String field = value;
        String defaultTableName = defaultTable.getTable();
        if ((index = value.lastIndexOf('.')) > 0) {
            field = value.substring(index + 1);
            value = value.substring(0, index);
            defaultTableName = value;
        }
        String defaultSchema = defaultTable.getSchema();
        if ((index = value.lastIndexOf('.')) > 0) {
            defaultTableName = value.substring(index + 1);
            defaultSchema = value.substring(0, index);
        }
        return new DbTableFieldDesc(new DbTableDesc(defaultTable.getUrlAddress(), defaultSchema, defaultTableName), field);
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
