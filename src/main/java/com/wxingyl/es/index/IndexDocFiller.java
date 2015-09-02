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
            TableQueryResult ret = query.next();
        }
    }
}
