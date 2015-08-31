package com.wxingyl.es.index;

import com.wxingyl.es.conf.index.IndexTypeBean;
import com.wxingyl.es.exception.IndexDocException;
import com.wxingyl.es.jdal.DbQueryResult;
import com.wxingyl.es.jdal.SqlQueryParam;
import com.wxingyl.es.jdal.handle.SqlQueryHandle;
import org.elasticsearch.client.Client;

import java.sql.SQLException;
import java.util.Iterator;

/**
 * Created by xing on 15/8/30.
 * index document filler
 */
public class IndexDocFiller implements IndexDocFill {

    private Client client;

    @Override
    public void fill(IndexTypeBean typeBean) throws IndexDocException {
        IndexTypeBean.TableQuery masterTable = typeBean.getMasterTable();
        TableDependQuery query = TableDependQuery.build(masterTable);
        while (query.hasNext()) {
            DbQueryResult ret = query.next();
        }
    }
}
