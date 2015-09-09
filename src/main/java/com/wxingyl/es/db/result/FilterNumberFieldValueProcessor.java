package com.wxingyl.es.db.result;

import com.wxingyl.es.conf.ConfigManager;
import com.wxingyl.es.conf.index.IndexTypeBean;
import com.wxingyl.es.util.CommonUtils;
import com.wxingyl.es.util.Listener;
import com.wxingyl.es.util.RwLock;

import java.util.*;

/**
 * Created by xing on 15/9/9.
 * only handle table key field
 */
public class FilterNumberFieldValueProcessor extends NumberFieldValueProcessor implements Listener<Set<IndexTypeBean>> {

    private RwLock<Set<String>> keyFieldSetLock;

    public FilterNumberFieldValueProcessor(ConfigManager configManager) {
        super();
        configManager.registerIndexTypeListener(this);
        keyFieldSetLock = CommonUtils.createRwLock(new HashSet<>());
    }

    @Override
    public Object handle(String fieldName, Object value) {
        if (!keyFieldSetLock.readOp(v -> v.contains(fieldName))) return value;
        return super.handle(fieldName, value);
    }

    @Override
    public void onChange(Set<IndexTypeBean> message) {
        List<String> changedFields = new LinkedList<>();
        message.forEach(type -> type.getAllTableInfo().forEach(v -> changedFields.add(v.getKeyField())));
        keyFieldSetLock.writeOp(v -> v.addAll(changedFields));
    }
}
