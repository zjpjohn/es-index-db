package com.wxingyl.es.command;

import com.wxingyl.es.action.IndexTypeInfo;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.index.query.QueryBuilder;

import java.util.LinkedList;
import java.util.List;

/**
 * Created by xing on 15/10/21.
 * abstract RtCommand implement
 */
public abstract class AbstractRtCommand implements RtCommand {

    protected final IndexTypeInfo.TableInfo tableInfo;

    private List<QueryBuilder> commonQueryCondition;

    private List<FilterBuilder> commonFilterCondition;

    public AbstractRtCommand(IndexTypeInfo.TableInfo tableInfo) {
        this.tableInfo = tableInfo;
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

    protected Client getClient() {
        return tableInfo.getIndexManager().getClient();
    }
}
