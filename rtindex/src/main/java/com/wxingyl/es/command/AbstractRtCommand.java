package com.wxingyl.es.command;

import com.wxingyl.es.action.adapter.IndexTypeInfo;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.index.query.QueryBuilder;

import java.util.LinkedList;
import java.util.List;

/**
 * Created by xing on 15/10/21.
 * abstract RtCommand implement
 */
public abstract class AbstractRtCommand implements RtCommand {

    protected final IndexTypeInfo.TableInfo tableInfo;

    public AbstractRtCommand(IndexTypeInfo.TableInfo tableInfo) {
        this.tableInfo = tableInfo;
    }

    protected Client getClient() {
        return tableInfo.getIndexManager().getClient();
    }

    protected String getTypeTableMsg() {
        return "type: " + tableInfo.getType() + ", table: " + tableInfo.getTable();
    }
}
