package com.wxingyl.es.index;

import com.wxingyl.es.conf.index.IndexTypeBean;
import com.wxingyl.es.exception.IndexRuntimeException;
import com.wxingyl.es.jdal.DbQueryResult;
import com.wxingyl.es.jdal.SqlQueryParam;
import com.wxingyl.es.jdal.handle.SqlQueryHandle;
import org.elasticsearch.client.Client;

import java.sql.SQLException;

/**
 * Created by xing on 15/8/30.
 * index document filler
 */
public class IndexDocFiller implements IndexDocFill {

    private Client client;

    @Override
    public void fill(IndexTypeBean typeBean) {
        IndexTypeBean.TableQuery masterTable = typeBean.getMasterTable();
        SqlQueryParam<Void> masterParam = SqlQueryParam.createMasterQueryParam(masterTable);
        SqlQueryHandle handle = masterTable.getQueryHandler();
        DbQueryResult queryResult;
        int page = 0;
        do {
            masterParam.setStart(page);
            try {
                queryResult = handle.query(masterParam);
            } catch (SQLException e) {
                throw new IndexRuntimeException("query data have error from: " + masterParam, e);
            }
            //TODO deal db queryResult
            page++;
        } while (queryResult.needContinue());
    }
}
