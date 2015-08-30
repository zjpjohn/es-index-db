package com.wxingyl.es.index;

import com.wxingyl.es.conf.index.IndexTypeBean;

/**
 * Created by xing on 15/8/28.
 * fill data from db
 */
public interface IndexFill {
    /**
     * fill data from db
     */
    void fill(IndexTypeBean typeBean);
}
