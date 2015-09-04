package com.wxingyl.es.index;

import com.wxingyl.es.conf.index.IndexTypeBean;
import com.wxingyl.es.jdal.TableQueryResult;
import com.wxingyl.es.jdal.DbTableDesc;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by xing on 15/8/30.
 * index document filler
 */
public class IndexDocFiller implements IndexDocFill {

    private Map<DbTableDesc, TableQueryResultListener> tableQueryResultListenerMap = new HashMap<>();

    @Override
    public void fill(IndexTypeBean typeBean) {
        TableDependQuery query = new TableDependQuery(typeBean.getMasterTable());
        while (query.hasNext()) {
//            TableQueryResult ret = query.next();
        }
    }

    @Override
    public boolean addTableQueryResultListener(TableQueryResultListener listener) {
        for (DbTableDesc table : listener.supportTable()) {
            if (tableQueryResultListenerMap.containsKey(table)) {
                throw new IllegalArgumentException("table: " + table + " has TableQueryResultListener: "
                        + tableQueryResultListenerMap.get(table) + ", support tables: " + tableQueryResultListenerMap.get(table).supportTable());
            } else {
                tableQueryResultListenerMap.put(table, listener);
            }
        }
        return true;
    }
}
