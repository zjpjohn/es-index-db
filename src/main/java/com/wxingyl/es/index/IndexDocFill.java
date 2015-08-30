package com.wxingyl.es.index;

import com.wxingyl.es.conf.index.IndexTypeBean;

/**
 * Created by xing on 15/8/28.
 * fill document from db
 */
public interface IndexDocFill {
    /**
     * fill data from db
     */
    void fill(IndexTypeBean typeBean);
}
