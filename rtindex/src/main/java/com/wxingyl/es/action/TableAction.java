package com.wxingyl.es.action;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.wxingyl.es.action.adapter.IndexTypeInfo;
import com.wxingyl.es.action.adapter.TableActionAdapter;
import com.wxingyl.es.canal.ChangeDataEntry;
import com.wxingyl.es.command.RtCommand;
import com.wxingyl.es.db.DbTableDesc;
import com.wxingyl.es.index.IndexTypeDesc;

import java.util.List;
import java.util.Map;

/**
 * Created by xing on 15/9/30.
 * table deal change, will chose mapping {@link TableActionAdapter}
 * to create {@link RtCommand}
 * every table should have an one, different also should have different obj
 */
public interface TableAction {

    List<RtCommand> createCommand(IndexTypeDesc type, List<ChangeDataEntry> data);

    void addTypeTableInfo(IndexTypeInfo.TableInfo tableInfo);

    DbTableDesc getTable();

    Integer getColumnIndex(String column);

    Map<String, Object> canalRowTransfer(List<CanalEntry.Column> list);

    Object canalValueTransfer(String dbFieldName, String strValue);
}
