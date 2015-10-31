package com.wxingyl.es.command;

import com.wxingyl.es.action.adapter.IndexTypeInfo;
import org.elasticsearch.client.Client;

/**
 * Created by xing on 15/10/21.
 * abstract RtCommand implement
 */
public abstract class AbstractRtAction implements RtCommand {

    protected final IndexTypeInfo.TableInfo tableInfo;

    public AbstractRtAction(IndexTypeInfo.TableInfo tableInfo) {
        this.tableInfo = tableInfo;
    }

    protected final Client getClient() {
        return tableInfo.getIndexManager().getClient();
    }

    protected final String getTypeTableMsg() {
        return "type: " + tableInfo.getType() + ", table: " + tableInfo.getTable();
    }
}
