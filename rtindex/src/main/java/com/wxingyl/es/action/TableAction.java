package com.wxingyl.es.action;

import com.wxingyl.es.canal.ChangeDataEntry;
import com.wxingyl.es.command.RtCommand;
import com.wxingyl.es.db.query.QueryCondition;
import com.wxingyl.es.index.IndexTypeDesc;

import java.util.List;
import java.util.Set;

/**
 * Created by xing on 15/9/30.
 * table deal change
 * every table should have an one, different also should have different obj
 */
public interface TableAction {

    List<RtCommand> createCommand(IndexTypeDesc type, List<ChangeDataEntry> data);

    void addQueryCondition(IndexTypeDesc type, Set<QueryCondition> conditions);
}
