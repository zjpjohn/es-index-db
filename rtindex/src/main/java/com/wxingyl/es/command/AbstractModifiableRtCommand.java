package com.wxingyl.es.command;

import com.wxingyl.es.action.adapter.IndexTypeInfo;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.index.query.QueryBuilder;

import java.util.LinkedList;
import java.util.List;

/**
 * Created by xing on 15/10/23.
 * abstract class
 */
public abstract class AbstractModifiableRtCommand extends AbstractRtCommand implements ModifiableRtCommand {

    private List<QueryBuilder> commonQueryCondition;

    private List<FilterBuilder> commonFilterCondition;

    public AbstractModifiableRtCommand(IndexTypeInfo.TableInfo tableInfo) {
        super(tableInfo);
    }

    @Override
    public void addPreQuery(QueryBuilder queryBuilder) {
        if (queryBuilder == null) return;
        commonQueryCondition = new LinkedList<>();
        commonQueryCondition.add(queryBuilder);
    }

    @Override
    public void addPreFilter(FilterBuilder filterBuilder) {
        if (filterBuilder == null) return;
        commonFilterCondition = new LinkedList<>();
        commonFilterCondition.add(filterBuilder);
    }

    @Override
    public boolean isInvalid() {
        return commonQueryCondition == null && commonFilterCondition == null;
    }

    public List<FilterBuilder> getCommonFilterCondition() {
        return commonFilterCondition;
    }

    public List<QueryBuilder> getCommonQueryCondition() {
        return commonQueryCondition;
    }

}
