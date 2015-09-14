package com.wxingyl.es.util;

import com.wxingyl.es.db.DbTableDesc;
import com.wxingyl.es.db.DbTableFieldDesc;
import com.wxingyl.es.index.doc.DocFields;

import java.util.*;
import java.util.function.Supplier;

/**
 * Created by xing on 15/8/17.
 * 常用的工具类
 */
public abstract class CommonUtils {

    public static boolean isEmpty(String s) {
        return s == null || s.isEmpty();
    }

    /**
     * @return s.{@link CommonUtils#isEmpty(String)} = true, return null, and s.trim.{@link String#isEmpty()} = true, return null
     * else return s.{@link String#trim()}
     */
    public static String emptyTrim(String s) {
        if (isEmpty(s)) return null;
        s = s.trim();
        return s.isEmpty() ? null : s;
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
                String val;
                if ((val = emptyTrim(v.toString())) != null) ret.add((T) val);
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

    public static Map<Object, List<Map<String, Object>>> groupListMap(List<Map<String, Object>> data,
                                                                      String keyField, boolean removeKeyField) {
        Map<Object, List<Map<String, Object>>> group = new HashMap<>();
        data.forEach(map -> {
            Object obj = map.get(keyField);
            List<Map<String, Object>> list = group.get(obj);
            if (list == null) {
                group.put(obj, list = new LinkedList<>());
            }
            if (removeKeyField) map.remove(keyField);
            list.add(map);
        });
        return group;
    }

    public static String getStringVal(Map<String, Object> map, String key) {
        return emptyTrim((String) map.get(key));
    }

    public static void mergeSlaveResult(String prefixConflict, DocFields masterField, Map<String, Object> slaveField) {
        for(String key : slaveField.keySet()) {
            if (masterField.containsKey(key)) {
                masterField.put(prefixConflict + key, slaveField.get(key));
            } else {
                masterField.put(key, slaveField.get(key));
            }
        }
    }

    public static void mergeSlaveResult(String prefixConflict, DocFields masterField, DocFields slaveField) {
        for(String key : slaveField.keySet()) {
            if (masterField.containsKey(key)) {
                masterField.put(prefixConflict + key, slaveField.get(key));
            } else {
                masterField.put(key, slaveField.get(key));
            }
        }
    }

    public static <T> RwLock<T> createRwLock(T lockObj) {
        return new RwLock<>(lockObj);
    }

    public static <T> ThreadLocal<T> createThreadLocal(Supplier<T> supplier) {
        if (supplier == null) return new ThreadLocal<>();
        else return new ThreadLocal<T>() {
            @Override
            protected T initialValue() {
                return supplier.get();
            }
        };
    }
}
