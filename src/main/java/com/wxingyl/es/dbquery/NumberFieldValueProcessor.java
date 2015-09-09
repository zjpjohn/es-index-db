package com.wxingyl.es.dbquery;

import com.wxingyl.es.conf.ConfigManager;
import com.wxingyl.es.conf.index.IndexTypeBean;
import com.wxingyl.es.util.CommonUtils;
import com.wxingyl.es.util.Listener;
import com.wxingyl.es.util.RwLock;

import java.util.*;

/**
 * Created by xing on 15/9/9.
 * db query result, the same type of number may be have different Class type, such Integer and Long
 * Note: default we only handle Integer to Long. Because float, double, BigDecimal used Map.key is rare, so we don't care
 */
public class NumberFieldValueProcessor implements FieldValueProcessor, Listener<Set<IndexTypeBean>> {

    private RwLock<Set<String>> keyFieldSetLock;

    public NumberFieldValueProcessor(ConfigManager configManager) {
        configManager.registerIndexTypeListener(this);
        keyFieldSetLock = CommonUtils.createRwLock(new HashSet<>());
    }

    @Override
    public Object handle(String fieldName, Object value) {
        if (!(value instanceof Number)) return value;
        if (!keyFieldSetLock.readOp(v -> v.contains(fieldName))) return value;
        Class cls = value.getClass();
        if (Integer.class == cls || Short.class == cls || Byte.class == cls) {
            return ((Number) value).longValue();
        } else {
            return value;
        }
    }

    @Override
    public void onChange(Set<IndexTypeBean> message) {
        List<String> changedFields = new LinkedList<>();
        message.forEach(type -> type.getAllTableInfo().forEach(v -> changedFields.add(v.getKeyField())));
        keyFieldSetLock.writeOp(v -> v.addAll(changedFields));
    }
}
