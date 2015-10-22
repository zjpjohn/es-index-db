package com.wxingyl.es.action;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.wxingyl.es.action.adapter.IndexTypeInfo;
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
        this.valueConvertMap = Collections.unmodifiableMap(new HashMap<>(valueConvertMap));
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

    protected abstract void updateCommand(IndexTypeDesc type, CanalEntry.RowData rowData, List<RtCommand> appendRet);

    protected void deleteCommand(IndexTypeDesc type, List<CanalEntry.Column> list, List<RtCommand> appendRet) {
        RtCommand command = typeInfoMap.get(type).getActionAdapter().createDeleteRtCommand(list);
        if (isInvalid(command)) appendRet.add(command);
    }

    protected void insertCommand(IndexTypeDesc type, List<CanalEntry.Column> list, List<RtCommand> appendRet) {
        RtCommand command = typeInfoMap.get(type).getActionAdapter().createInsertRtCommand(list);
        if (isInvalid(command)) appendRet.add(command);
    }

    protected boolean isInvalid(RtCommand command) {
        return command == null || command.isInvalid();
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

    @Override
    public void addTypeTableInfo(IndexTypeInfo.TableInfo tableInfo) {
        typeInfoMap.put(tableInfo.getType(), tableInfo);
    }

    @Override
    public DbTableDesc getTable() {
        return table;
    }

    @Override
    public Integer getColumnIndex(String column) {
        return tableColumnIndex.getIndex(column);
    }

    @Override
    public Map<String, Object> canalRowTransfer(List<CanalEntry.Column> list) {
        Map<String, Object> ret = new HashMap<>();
        for (CanalEntry.Column c : list) {
            String name = c.getName();
            if (valueConvertMap.get(name) == null) {
                ret.put(name, c.getValue());
            } else {
                ret.put(name, valueConvertMap.get(name).convert(c.getValue()));
            }
        }
        return ret;
    }

}
