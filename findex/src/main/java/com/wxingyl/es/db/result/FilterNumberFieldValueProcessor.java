package com.wxingyl.es.db.result;

import com.wxingyl.es.conf.ConfigManager;
import com.wxingyl.es.index.IndexTypeBean;
import com.wxingyl.es.index.db.SqlQueryCommon;
import com.wxingyl.es.util.CommonUtils;
import com.wxingyl.es.util.Listener;
import com.wxingyl.es.util.RwLock;
import org.elasticsearch.common.base.Function;
import org.elasticsearch.common.base.Supplier;

import java.util.*;

/**
 * Created by xing on 15/9/9.
 * only handle table key field
 */
public class FilterNumberFieldValueProcessor extends NumberFieldValueProcessor implements Listener<Set<IndexTypeBean>>, Function<Set<String>, Boolean> {

    private RwLock<Set<String>> keyFieldSetLock;

    private ThreadLocal<String> local = CommonUtils.createThreadLocal(null);

    public FilterNumberFieldValueProcessor(ConfigManager configManager) {
        super();
        configManager.registerIndexTypeListener(this);
        keyFieldSetLock = CommonUtils.createRwLock(new Supplier<Set<String>>() {
            @Override
            public Set<String> get() {
                return new HashSet<>();
            }
        });
    }

    @Override
    public Object handle(String fieldName, Object value) {
        local.set(fieldName);
        try {
            if (!keyFieldSetLock.readOp(this)) return value;
        } finally {
            local.remove();
        }
        return super.handle(fieldName, value);
    }

    @Override
    public void onChange(Set<IndexTypeBean> message) {
        final List<String> changedFields = new LinkedList<>();
        for (IndexTypeBean type : message) {
            for (SqlQueryCommon v : type.getAllTableQueryInfo()) {
                changedFields.add(v.getBaseInfo().getKeyField());
            }
        }
        keyFieldSetLock.writeOp(new Function<Set<String>, Void>() {
            @Override
            public Void apply(Set<String> input) {
                input.addAll(changedFields);
                return null;
            }
        });
    }

    @Override
    public Boolean apply(Set<String> input) {
        return input.contains(local.get());
    }

}
