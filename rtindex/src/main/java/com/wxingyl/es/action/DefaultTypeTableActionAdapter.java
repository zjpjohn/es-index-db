package com.wxingyl.es.action;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.wxingyl.es.command.ChangedFieldEntry;
import com.wxingyl.es.command.UpdateRtCommand;
import com.wxingyl.es.command.UpdateRtCommandAction;
import com.wxingyl.es.index.IndexTypeDesc;
import org.elasticsearch.index.query.QueryBuilders;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by xing on 15/10/20.
 * default implement of TypeTableActionAdapter
 */
public class DefaultTypeTableActionAdapter extends AbstractTypeTableActionAdapter {

    private Map<String, String> dbDocFieldMap;

    public DefaultTypeTableActionAdapter(IndexTypeDesc type, Map<String, String> dbDocFieldMap) {
        super(type);
        this.dbDocFieldMap = new HashMap<>();
        this.dbDocFieldMap.putAll(dbDocFieldMap);
    }

    @Override
    public UpdateRtCommand createUpdateRtCommand(CanalEntry.RowData rowData) {
        final int count = rowData.getAfterColumnsCount();
        UpdateRtCommand rtCommand = new UpdateRtCommandAction(tableInfo);
        for (int i = 0; i < count; i++) {
            CanalEntry.Column afterColumn = rowData.getAfterColumns(i);
            if (afterColumn.getUpdated()) {
                rtCommand.addChangeField(new ChangedFieldEntry(dbDocFieldMap.get(afterColumn.getName()),
                        rowData.getBeforeColumns(i).getValue(), afterColumn.getValue()));
            }
        }
        String keyField = tableInfo.getKeyField();
        rtCommand.addPreQuery(QueryBuilders.termQuery(dbDocFieldMap.get(keyField),
                rowData.getBeforeColumns(tableColumnIndex.getIndex(keyField)).getValue()));
        return rtCommand.isInvalid() ? null : rtCommand;
    }
}
