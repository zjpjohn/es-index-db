package com.wxingyl.es.index;

import com.wxingyl.es.conf.index.IndexTypeBean;
import org.elasticsearch.client.Client;

/**
 * Created by xing on 15/8/30.
 * index document filler
 */
public class IndexDocFiller implements IndexDocFill {

    private Client client;

    @Override
    public void fill(IndexTypeBean typeBean) {
        IndexTypeBean.TableQuery masterTable = typeBean.getMasterTable();

    }
}
