package com.wxingyl.es.action;

import com.wxingyl.es.db.DbTableDesc;
import com.wxingyl.es.index.IndexTypeBean;
import com.wxingyl.es.util.CommonUtils;

import java.util.*;

/**
 * Created by xing on 15/9/29.
 * abstract implement class of TypeRtIndexAction
 */
public abstract class AbstractRtIndexAction implements DeployRtIndexAction {

    /**
     * In an action, a canal instance should only one index/type
     * key: canal instance name, value: index/type entry
     */
    protected Map<String, TypeEntry> actionMap = new HashMap<>();

    @Override
    public List<DbTableDesc> supportTable(String instance) {
        TypeEntry entry = actionMap.get(instance);
        if (entry == null) return null;
        else if (entry.supportTable.isEmpty()) return Collections.emptyList();
        else return Collections.unmodifiableList(entry.supportTable);
    }

    @Override
    public IndexTypeBean supportType(String instance) {
        TypeEntry entry = actionMap.get(instance);
        return entry == null ? null : entry.typeBean;
    }

    @Override
    public void registerTableAction(String instance, IndexTypeBean typeBean, List<DbTableDesc> tables) {
        if (CommonUtils.isEmpty(tables)) throw new IllegalArgumentException("tables can not empty");
        addTableAction(instance, typeBean, tables);
    }

    @Override
    public void registerTableAction(String instance, IndexTypeBean typeBean) {
        addTableAction(instance, typeBean, Collections.<DbTableDesc>emptyList());
    }

    private void addTableAction(String instance, IndexTypeBean typeBean, List<DbTableDesc> tables) {
        TypeEntry entry = actionMap.get(instance);
        if (!(entry == null || entry.typeBean.getType().equals(typeBean.getType()))) {
            throw new IllegalArgumentException("tables can not empty");
        }
        if (entry == null) {
            actionMap.put(instance, entry = new TypeEntry(instance, typeBean));
        }

        if (tables.isEmpty()) {
            entry.supportTable = tables;
        } else {
            if (entry.supportTable == null) {
                entry.supportTable = new ArrayList<>();
            }
            entry.supportTable.addAll(tables);
        }
    }

    protected static class TypeEntry {

        protected String instance;

        protected IndexTypeBean typeBean;

        protected List<DbTableDesc> supportTable;

        public TypeEntry(String instance, IndexTypeBean typeBean) {
            this.typeBean = typeBean;
            this.instance = instance;
        }
    }
}
