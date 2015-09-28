package com.wxingyl.es.rtindex;

import com.wxingyl.es.conf.DefaultConfigManager;
import com.wxingyl.es.conf.index.DbTableConfigInfo;
import com.wxingyl.es.conf.index.TypeConfigInfo;
import com.wxingyl.es.db.DbTableDesc;
import com.wxingyl.es.db.query.SqlQueryHandle;
import com.wxingyl.es.index.IndexTypeDesc;
import com.wxingyl.es.util.CommonUtils;

import java.util.*;
import java.util.concurrent.ExecutionException;

/**
 * Created by xing on 15/9/25.
 * {@link com.wxingyl.es.conf.ConfigManager} implement, extend DefaultConfigManager and record
 * db table fields, to support real time index
 */
public class RecordDbFieldsConfigManager extends DefaultConfigManager implements RtIndexConfigManager {

    private Map<IndexTypeDesc, Map<DbTableDesc, List<String>>> typeTableFieldMap = new HashMap<>();

    @Override
    protected SqlQueryHandle verifyTypeTableConfig(TypeConfigInfo type, DbTableConfigInfo info) {
        SqlQueryHandle handle = super.verifyTypeTableConfig(type, info);
        List<String> list;
        if (info.getFields() != null) {
            list = new ArrayList<>();
            list.addAll(info.getFields());
        } else if (!CommonUtils.isEmpty(info.getForbidFields())){
            list = new ArrayList<>();
            try {
                for (String f : handle.getAllFields(info.getTable())) {
                    if (info.getForbidFields() != null && info.getForbidFields().contains(f)) continue;
                    list.add(f);
                }
            } catch (ExecutionException ignored) {
            }
        } else {
            list = Collections.emptyList();
        }
        addTableFieldList(type.getTypeDesc(), info.getTable(), list);
        return handle;
    }

    private void addTableFieldList(IndexTypeDesc type, DbTableDesc table, List<String> fields) {
        Map<DbTableDesc, List<String>> map = typeTableFieldMap.get(type);
        if (map == null) {
            typeTableFieldMap.put(type, map = new HashMap<>());
        }
        map.put(table, Collections.unmodifiableList(fields));
    }

    /**
     * @return fields list is unmodifiable, If all column is effect, return {@link Collections#emptyList()}
     *
     */
    public List<String> getTableFields(IndexTypeDesc type, DbTableDesc table) {
        if (typeTableFieldMap.containsKey(type)) {
            return typeTableFieldMap.get(type).get(table);
        } else {
            return null;
        }
    }
}
