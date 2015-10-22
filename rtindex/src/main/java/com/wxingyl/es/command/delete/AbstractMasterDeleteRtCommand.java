package com.wxingyl.es.command.delete;

import com.wxingyl.es.action.IndexTypeInfo;
import com.wxingyl.es.command.AbstractRtCommand;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.index.query.QueryBuilder;

/**
 * Created by xing on 15/10/22.
 * abstract class
 */
public abstract class AbstractMasterDeleteRtCommand extends AbstractRtCommand implements MasterDeleteRtCommand {

    public AbstractMasterDeleteRtCommand(IndexTypeInfo.TableInfo tableInfo) {
        super(tableInfo);
    }

    public abstract boolean isInvalid();

    @Override
    public void addPreFilter(FilterBuilder filterBuilder) {
        throw new UnsupportedOperationException("MasterDeleteRtCommand add filter is useless");
    }

    @Override
    public void addPreQuery(QueryBuilder queryBuilder) {
        throw new UnsupportedOperationException("MasterDeleteRtCommand can query is useless");
    }

    @Override
    public IndexTypeInfo.TableInfo getTableInfo() {
        return tableInfo;
    }
}
