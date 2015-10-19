package com.wxingyl.es.action;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.wxingyl.es.canal.ChangeDataEntry;
import com.wxingyl.es.command.RtCommand;
import com.wxingyl.es.db.DbTableDesc;
import com.wxingyl.es.db.query.QueryCondition;
import com.wxingyl.es.index.IndexTypeDesc;
import com.wxingyl.es.util.CommonUtils;
import com.wxingyl.es.util.transfer.StrValConverts;
import com.wxingyl.es.util.transfer.StrValueConvert;

import java.util.*;

/**
 * Created by xing on 15/10/15.
 * abstract TableAction
 */
public abstract class AbstractTableAction implements TableAction {

    protected final DbTableDesc table;

    protected Map<IndexTypeDesc, IndexTypeInfo.TableInfo> typeInfoMap = new HashMap<>();

    protected final TableColumnIndex tableColumnIndex = new TableColumnIndex();

    protected Map<String, StrValueConvert> valueConvertMap;

    public AbstractTableAction(DbTableDesc table, Map<String, StrValueConvert> valueConvertMap) {
        this.table = table;
        this.valueConvertMap = new HashMap<>(valueConvertMap);
    }

    @Override
    public void addIndexType(IndexTypeInfo typeInfo) {
        typeInfoMap.put(typeInfo.getTableInfo(table).getType(), typeInfo.getTableInfo(table));
    }

    /**
     * @param type index/type name
     * @param list db data
     * @return index doc need return true, not need return false
     */
    private boolean commonConditionVerify(IndexTypeDesc type, List<CanalEntry.Column> list) {
        if (CommonUtils.isEmpty(typeInfoMap.get(type).getQueryCondition())) return true;
        if (tableColumnIndex.columnNum() != list.size()) {
            synchronized (tableColumnIndex) {
                if (tableColumnIndex.columnNum() != list.size()) {
                    tableColumnIndex.reload(list);
                }
            }
        }
        for (QueryCondition qc : typeInfoMap.get(type).getQueryCondition()) {
            if (!qc.verifyValue(list.get(tableColumnIndex.getIndex(qc.getField())).getValue(),
                    valueConvertMap.get(qc.getField()) == null ? StrValConverts.stringConvert()
                            : valueConvertMap.get(qc.getField()))) {
                return false;
            }
        }
        return true;
    }

    protected abstract void deleteCommand(IndexTypeDesc type, List<CanalEntry.Column> list, List<RtCommand> appendRet);

    protected abstract void insertCommand(IndexTypeDesc type, List<CanalEntry.Column> list, List<RtCommand> appendRet);

    protected abstract void updateCommand(IndexTypeDesc type, CanalEntry.RowData rowData, List<RtCommand> appendRet);

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

}
