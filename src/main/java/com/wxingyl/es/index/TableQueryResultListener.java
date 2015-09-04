package com.wxingyl.es.index;

import com.wxingyl.es.jdal.DbTableDesc;
import com.wxingyl.es.jdal.TableQueryResult;

import java.util.Set;

/**
 * Created by xing on 15/9/2.
 * table query result listener
 */
public interface TableQueryResultListener {

    void onHandle(IndexTypeDesc type, TableQueryResult tableQueryResult);

    Set<DbTableDesc> supportTable();

}
