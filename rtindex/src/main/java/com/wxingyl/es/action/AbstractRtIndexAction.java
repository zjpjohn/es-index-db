package com.wxingyl.es.action;

import com.wxingyl.es.db.DbTableDesc;
import com.wxingyl.es.index.IndexTypeBean;
import com.wxingyl.es.index.IndexTypeDesc;
import com.wxingyl.es.util.CommonUtils;

import java.util.*;

/**
 * Created by xing on 15/9/29.
 * abstract implement class of TypeRtIndexAction
 */
public abstract class AbstractRtIndexAction implements DeployRtIndexAction {

    /**
     * In an action, a canal instance should only one index/type
     */
    protected Map<IndexTypeDesc, Entry> actionMap = new HashMap<>();

    @Override
    public List<DbTableDesc> supportTable(String instance) {
        List<DbTableDesc> list;
        for (Entry e : actionMap.values()) {
            if ((list = e.instanceTableMap.get(instance)) != null) {
                if (list.isEmpty()) return list;
                else return Collections.unmodifiableList(list);
            }
        }
        return null;
    }

    @Override
    public IndexTypeBean supportType(String instance) {
        for (Entry e : actionMap.values()) {
            if (e.instanceTableMap.get(instance) != null)
                return e.typeBean;
        }
        return null;
    }

    @Override
    public void registerTableAction(String instance, IndexTypeBean typeBean, List<DbTableDesc> tables) {
        if(CommonUtils.isEmpty(tables)) throw new IllegalArgumentException("tables can not empty");
        addTableAction(instance, typeBean, tables);
    }

    @Override
    public void registerTableAction(String instance, IndexTypeBean typeBean) {
        addTableAction(instance, typeBean, Collections.<DbTableDesc>emptyList());
    }

    private void addTableAction(String instance, IndexTypeBean typeBean, List<DbTableDesc> tables) {
        IndexTypeBean orgType = supportType(instance);
        if (!(orgType == null || orgType.getType().equals(typeBean.getType()))) {
            throw new IllegalArgumentException("tables can not empty");
        }
        IndexTypeDesc type = typeBean.getType();
        Entry entry = actionMap.get(type);
        if (entry == null) {
            entry = new Entry(typeBean);
            actionMap.put(type, entry);
        }
        if (tables.isEmpty()) {
            entry.instanceTableMap.put(instance, tables);
        } else {
            List<DbTableDesc> tableList = entry.instanceTableMap.get(instance);
            if (tableList == null) {
                tableList = new ArrayList<>();
                entry.instanceTableMap.put(instance, tableList);
            }
            tableList.addAll(tables);
        }
    }

    protected static class Entry {

        protected IndexTypeBean typeBean;

        protected Map<String, List<DbTableDesc>> instanceTableMap = new HashMap<>();

        public Entry(IndexTypeBean typeBean) {
            this.typeBean = typeBean;
        }

        public Map<String, List<DbTableDesc>> getInstanceTableMap() {
            return instanceTableMap;
        }

        public IndexTypeBean getTypeBean() {
            return typeBean;
        }
    }
}
