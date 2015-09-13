package com.wxingyl.es.index;

import com.wxingyl.es.db.TableBaseInfo;
import com.wxingyl.es.db.query.TableQueryInfo;

import java.util.List;

/**
 * Created by xing on 15/9/13.
 * define indexTypeBean interface
 */
public interface IndexTypeBean {

    IndexTypeDesc getType();

    TableQueryInfo getMasterTable();

    List<TableBaseInfo> getTableInfo(String tableName);

    List<TableBaseInfo> getAllTableInfo();

}
