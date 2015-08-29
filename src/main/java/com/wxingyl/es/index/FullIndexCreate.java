package com.wxingyl.es.index;

import com.wxingyl.es.conf.index.IndexTypeBean;

/**
 * Created by xing on 15/8/28.
 * define create index interface
 */
public interface FullIndexCreate {
    /**
     * if index don't exist, create
     */
    void create(IndexSetting setting);

    /**
     * fill data from db
     */
    void fill(IndexTypeBean typeBean);

}
