package com.wxingyl.es.index.db;

import com.wxingyl.es.db.DbTableDesc;
import com.wxingyl.es.db.result.TableQueryResult;
import com.wxingyl.es.index.IndexTypeDesc;

import java.util.Set;

/**
 * Created by xing on 15/9/2.
 * table query result handle
 */
public interface TableQueryResultHandle {

    void onHandle(IndexTypeDesc type, TableQueryResult tableQueryResult);

    Set<DbTableDesc> supportTable();

}
