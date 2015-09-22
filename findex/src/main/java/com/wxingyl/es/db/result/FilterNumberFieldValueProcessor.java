package com.wxingyl.es.db.result;

import com.wxingyl.es.conf.ConfigManager;
import com.wxingyl.es.db.TableBaseInfo;
import com.wxingyl.es.index.IndexTypeBean;
import com.wxingyl.es.util.CommonUtils;
import com.wxingyl.es.util.Function;
import com.wxingyl.es.util.Listener;
import com.wxingyl.es.util.RwLock;

import java.util.*;

/**
 * Created by xing on 15/9/9.
 * only handle table key field
 */
public class FilterNumberFieldValueProcessor extends NumberFieldValueProcessor implements Listener<Set<IndexTypeBean>> {

    private RwLock<HashSet<String>> keyFieldSetLock;

    public FilterNumberFieldValueProcessor(ConfigManager configManager) {
        super();
        configManager.registerIndexTypeListener(this);
        keyFieldSetLock = CommonUtils.createRwLock(new HashSet<String>());
    }

    @Override
    public Object handle(final String fieldName, Object value) {
        if (!keyFieldSetLock.readOp(new Function<HashSet<String>, Boolean>() {
            @Override
            public Boolean apply(HashSet<String> input) {
                return input.contains(fieldName);
            }
        })) return value;
        return super.handle(fieldName, value);
    }

    @Override
    public void onChange(Set<IndexTypeBean> message) {
        final List<String> changedFields = new LinkedList<>();
        for (IndexTypeBean type : message) {
            for (TableBaseInfo v : type.getAllTableInfo()) {
                changedFields.add(v.getKeyField());
            }
        }
        keyFieldSetLock.writeOp(new Function<HashSet<String>, Void>() {
            @Override
            public Void apply(HashSet<String> input) {
                input.addAll(changedFields);
                return null;
            }
        });
    }
}
