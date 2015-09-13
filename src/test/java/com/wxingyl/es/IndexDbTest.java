package com.wxingyl.es;

import com.wxingyl.es.db.query.BaseQueryParam;
import com.wxingyl.es.db.query.QueryCondition;
import com.wxingyl.es.db.query.SqlQueryOperator;
import com.wxingyl.es.index.IndexTypeBean;
import com.wxingyl.es.index.IndexTypeDesc;
import org.apache.commons.dbutils.handlers.ScalarHandler;
import org.junit.Assert;
import org.junit.Test;

import java.sql.SQLException;

/**
 * Created by xing on 15/8/11.
 * config test
 */
public class IndexDbTest extends AbstractIndexDbTest {

    @Test
    public void createIndex() throws SQLException {
        IndexTypeDesc typeDesc = new IndexTypeDesc("order_v1", "order_info");
        IndexTypeBean typeBean = configManager.findIndexTypeBean(typeDesc);
        Assert.assertNotNull("can't find " + typeDesc + " config", typeBean);
        OrderTypeDocPostProcessor docPostProcessor = new OrderTypeDocPostProcessor(typeBean);
        indexDocFactory.registerDocPostProcessor(docPostProcessor);

        long num = indexManager.indexFill(typeBean);
        System.out.println("create document: " + num);
        BaseQueryParam param = new BaseQueryParam();
        param.setTable(typeBean.getMasterTable().getQueryCommon().getTable());
        param.addField("count(1)");
        QueryCondition.buildSingle("seller_id", SqlQueryOperator.EQ, "1");
        param.addCondition(QueryCondition.buildSingle("seller_id", SqlQueryOperator.EQ, "1"));
        ScalarHandler<Long> scalarHandler = new ScalarHandler<>();
        long dbNum = typeBean.getMasterTable().getQueryHandler().query(param, scalarHandler);
        Assert.assertEquals(num, dbNum);
    }

}
