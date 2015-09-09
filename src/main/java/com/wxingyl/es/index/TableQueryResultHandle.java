package com.wxingyl.es.index;

import com.wxingyl.es.dbquery.DbTableDesc;
import com.wxingyl.es.dbquery.TableQueryResult;

import java.util.Set;

/**
 * Created by xing on 15/9/2.
 * table query result handle
 */
public interface TableQueryResultHandle {

    void onHandle(IndexTypeDesc type, TableQueryResult tableQueryResult);

    Set<DbTableDesc> supportTable();

}
