package com.wxingyl.es.action;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.wxingyl.es.command.delete.DeleteRtCommand;

import java.util.List;

/**
 * Created by xing on 15/10/20.
 * abstract implement
 */
public abstract class AbstractTypeTableActionAdapter implements TypeTableActionAdapter {

    protected final IndexTypeInfo.TableInfo tableInfo;

    public AbstractTypeTableActionAdapter(IndexTypeInfo.TableInfo tableInfo) {
        this.tableInfo = tableInfo;
        tableInfo.setActionAdapter(this);
    }


    //TODO other delete need implement
    @Override
    public DeleteRtCommand createDeleteRtCommand(List<CanalEntry.Column> list) {
        if (tableInfo.isMasterTable()) {

        }
        return null;
    }
}
