package com.wxingyl.es.index;

import com.wxingyl.es.conf.index.IndexTypeBean;
import com.wxingyl.es.exception.IndexDocException;
import com.wxingyl.es.jdal.DbQueryResult;

/**
 * Created by xing on 15/8/30.
 * index document filler
 */
public class IndexDocFiller implements IndexDocFill {

    @Override
    public void fill(IndexTypeBean typeBean) throws IndexDocException {
        TableDependQuery query = new TableDependQuery(typeBean.getMasterTable());
        while (query.hasNext()) {
            DbQueryResult ret = query.next();
        }
    }
}
