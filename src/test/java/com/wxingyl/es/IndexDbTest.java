package com.wxingyl.es;

import com.wxingyl.es.conf.index.IndexTypeBean;
import com.wxingyl.es.index.IndexTypeDesc;
import org.junit.Assert;
import org.junit.Test;

/**
 * Created by xing on 15/8/11.
 * config test
 */
public class IndexDbTest extends AbstractIndexDbTest {

    @Test
    public void createIndex() {
        IndexTypeDesc typeDesc = new IndexTypeDesc("order_v1", "order_info");
        IndexTypeBean typeBean = configManager.getIndexTypeBean(typeDesc);
        Assert.assertNotNull("can't find " + typeDesc + " config", typeBean);
        int num = indexManager.indexTypeFill(typeBean);
        System.out.println("create document: " + num);
    }

}
