package com.wxingyl.es.action;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.wxingyl.es.canal.ChangeDataEntry;
import com.wxingyl.es.command.RtCommand;
import com.wxingyl.es.db.DbTableDesc;
import com.wxingyl.es.db.query.QueryCondition;
import com.wxingyl.es.index.IndexTypeBean;
import com.wxingyl.es.index.IndexTypeDesc;
import com.wxingyl.es.index.db.SqlQueryCommon;
import com.wxingyl.es.util.CommonUtils;
import com.wxingyl.es.util.transfer.StrValTransfers;
import com.wxingyl.es.util.transfer.StrValueTransfer;

import java.util.*;

/**
 * Created by xing on 15/10/15.
 * abstract TableAction
 */
public abstract class AbstractTableAction implements TableAction {

    private DbTableDesc table;

    protected Map<IndexTypeDesc, Set<QueryCondition>> typeQueryCondition;

    protected final TableColumnIndex tableColumnIndex = new TableColumnIndex();

    protected Map<QueryCondition, StrValueTransfer> valueTransferMap;

    public AbstractTableAction(DbTableDesc table) {
        this.table = table;
    }

    /**
     * get value Transfer map
     */
    protected abstract Map<QueryCondition, StrValueTransfer> initValueTransferMap();

    public void addQueryCondition(IndexTypeBean type) {
        SqlQueryCommon common = type.getTableQueryInfo(table);
        Objects.requireNonNull(common);
        if (common.getConditions() != null) {
            addQueryCondition(type.getType(), common.getConditions());
        }
    }

    @Override
    public void addQueryCondition(IndexTypeDesc type, Set<QueryCondition> conditions) {
        if (CommonUtils.isEmpty(conditions)) return;
        if (typeQueryCondition == null) typeQueryCondition = new HashMap<>();
        typeQueryCondition.put(type, conditions);
    }

    public DbTableDesc getTable() {
        return table;
    }

    /**
     * @param type index/type name
     * @param list db data
     * @return index doc need return true, not need return false
     */
    protected boolean commonConditionVerify(IndexTypeDesc type, List<CanalEntry.Column> list) {
        if (typeQueryCondition == null || typeQueryCondition.get(type) == null) return true;
        if (tableColumnIndex.columnNum() != list.size()) {
            synchronized (tableColumnIndex) {
                if (tableColumnIndex.columnNum() != list.size()) {
                    tableColumnIndex.reload(list);
                    if (valueTransferMap == null) {
                        valueTransferMap = initValueTransferMap();
                    }
                }
            }
        }
        for (QueryCondition qc : typeQueryCondition.get(type)) {
            if (!qc.verifyValue(list.get(tableColumnIndex.getIndex(qc.getField())).getValue(),
                    valueTransferMap.get(qc) == null ? StrValTransfers.stringTransfer() : valueTransferMap.get(qc))) {
                return false;
            }
        }
        return true;
    }

    protected void deleteCommand(IndexTypeDesc type, List<CanalEntry.Column> list, List<RtCommand> appendRet) {

    }

    protected void insertCommand(IndexTypeDesc type, List<CanalEntry.Column> list, List<RtCommand> appendRet) {

    }

    protected void updateCommand(IndexTypeDesc type, CanalEntry.RowData rowData, List<RtCommand> appendRet) {
        final int count = rowData.getAfterColumnsCount();
        List<UpdateRowEntry> changeRows = new LinkedList<>();
        for (int i = 0; i < count; i++) {
            CanalEntry.Column column = rowData.getAfterColumns(i);
            if (column.getUpdated()) {
                changeRows.add(new UpdateRowEntry(column.getName(), column.getValue(), rowData.getBeforeColumns(i).getValue()));
            }
        }

    }

    @Override
    public List<RtCommand> createCommand(IndexTypeDesc type, List<ChangeDataEntry> data) {
        List<RtCommand> ret = new LinkedList<>();
        for (ChangeDataEntry entry : data) {
            CanalEntry.EventType eventType = entry.getEventType();
            if (eventType == CanalEntry.EventType.UPDATE) {
                for (CanalEntry.RowData r : entry.getRowData()) {
                    boolean beforeVerify = commonConditionVerify(type, r.getBeforeColumnsList());
                    boolean afterVerify = commonConditionVerify(type, r.getAfterColumnsList());
                    if (beforeVerify && afterVerify) {
                        updateCommand(type, r, ret);
                    } else if (beforeVerify) {
                        deleteCommand(type, r.getBeforeColumnsList(), ret);
                    } else {
                        insertCommand(type, r.getAfterColumnsList(), ret);
                    }
                }
            } else if (eventType == CanalEntry.EventType.DELETE) {
                for (CanalEntry.RowData r : entry.getRowData()) {
                    if (commonConditionVerify(type, r.getBeforeColumnsList())) {
                        deleteCommand(type, r.getBeforeColumnsList(), ret);
                    }
                }
            } else {
                //Insert EventType
                for (CanalEntry.RowData r : entry.getRowData()) {
                    if (commonConditionVerify(type, r.getAfterColumnsList())) {
                        insertCommand(type, r.getAfterColumnsList(), ret);
                    }
                }
            }
        }
        return ret;
    }

    protected class UpdateRowEntry {

        private String column;

        private String beforeVal;

        private String afterVal;

        public UpdateRowEntry(String column, String afterVal, String beforeVal) {
            this.afterVal = afterVal;
            this.beforeVal = beforeVal;
            this.column = column;
        }

        public String getAfterVal() {
            return afterVal;
        }

        public String getBeforeVal() {
            return beforeVal;
        }

        public String getColumn() {
            return column;
        }
    }

}
