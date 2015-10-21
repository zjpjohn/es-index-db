package com.wxingyl.es.index;

import com.wxingyl.es.db.DbTableDesc;
import com.wxingyl.es.db.query.TableQueryBean;
import com.wxingyl.es.index.db.SqlQueryCommon;

import java.util.List;

/**
 * Created by xing on 15/9/13.
 * define indexTypeBean interface
 */
public interface IndexTypeBean extends Comparable<IndexTypeBean> {

    IndexTypeDesc getType();

    TableQueryBean getMasterTable();

    SqlQueryCommon getTableQueryInfo(DbTableDesc table);

    List<SqlQueryCommon> getAllTableQueryInfo();

    int getPriority();

    int hashCode();

    boolean equals(Object o);

}
