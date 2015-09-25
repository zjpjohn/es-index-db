package com.wxingyl.es.rtindex;

import com.wxingyl.es.conf.DefaultConfigManager;
import com.wxingyl.es.conf.index.DbTableConfigInfo;
import com.wxingyl.es.conf.index.TypeConfigInfo;
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

    private Map<IndexTypeDesc, Map<String, List<String>>> typeTableFieldMap = new HashMap<>();

    @Override
    protected SqlQueryHandle verifyTypeTableConfig(TypeConfigInfo type, DbTableConfigInfo info) {
        SqlQueryHandle handle = super.verifyTypeTableConfig(type, info);
        List<String> list = new ArrayList<>();
        if (info.getFields() != null) {
            list.addAll(info.getFields());
        } else {
            try {
                for (String f : handle.getAllFields(info.getTable())) {
                    if (info.getForbidFields() != null && info.getForbidFields().contains(f)) continue;
                    list.add(f);
                }
            } catch (ExecutionException ignored) {
            }
        }
        Map<String, List<String>> map = typeTableFieldMap.get(type.getTypeDesc());
        if (map == null) {
            typeTableFieldMap.put(type.getTypeDesc(), map = new HashMap<>());
        }
        map.put(CommonUtils.tableToString(info.getTable()), Collections.unmodifiableList(list));
        return handle;
    }

    /**
     * @return fields list is unmodifiable
     */
    public List<String> getTableFields(IndexTypeDesc type, String table) {
        if (typeTableFieldMap.containsKey(type)) {
            return typeTableFieldMap.get(type).get(table);
        } else {
            return null;
        }
    }
}
