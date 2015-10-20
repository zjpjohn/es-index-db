package com.wxingyl.es.action;

import com.wxingyl.es.canal.ChangeDataEntry;
import com.wxingyl.es.command.RtCommand;
import com.wxingyl.es.index.IndexTypeDesc;

import java.util.List;

/**
 * Created by xing on 15/9/30.
 * table deal change
 * every table should have an one, different also should have different obj
 */
public interface TableAction {

    List<RtCommand> createCommand(IndexTypeDesc type, List<ChangeDataEntry> data);

    void addTypeTableInfo(IndexTypeInfo.TableInfo tableInfo);

    TableColumnIndex tableColumnIndex();

    IndexTypeInfo.TableInfo getTableAction(IndexTypeDesc type);
}
