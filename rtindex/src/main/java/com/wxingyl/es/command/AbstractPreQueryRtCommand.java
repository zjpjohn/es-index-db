package com.wxingyl.es.command;

import com.wxingyl.es.action.adapter.IndexTypeInfo;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.index.query.*;

import java.util.LinkedList;
import java.util.List;

/**
 * Created by xing on 15/10/23.
 * abstract PreQueryRtCommand
 */
public abstract class AbstractPreQueryRtCommand extends AbstractRtCommand implements PreQueryRtCommand {

    private SearchRequestBuilder searchRequestBuilder;

    private List<QueryBuilder> commonQueryCondition;

    private List<FilterBuilder> commonFilterCondition;

    protected BoolQueryBuilder boolQueryBuilder;

    protected AndFilterBuilder andFilterBuilder;

    /**
     * operator document max number in a page
     */
    private final int pageSize;

    public AbstractPreQueryRtCommand(IndexTypeInfo.TableInfo tableInfo, int pageSize) {
        super(tableInfo);
        this.pageSize = pageSize;
    }

    @Override
    public void addPreQuery(QueryBuilder queryBuilder) {
        if (queryBuilder == null) return;
        if (searchRequestBuilder != null) {
            throw new IllegalStateException(getTypeTableMsg() + ": searchRequestBuilder had created, can not add more query");
        }
        commonQueryCondition = new LinkedList<>();
        commonQueryCondition.add(queryBuilder);
    }

    @Override
    public void addPreFilter(FilterBuilder filterBuilder) {
        if (filterBuilder == null) return;
        if (searchRequestBuilder != null) {
            throw new IllegalStateException(getTypeTableMsg() + ": searchRequestBuilder had created, can not add more filter");
        }
        commonFilterCondition = new LinkedList<>();
        commonFilterCondition.add(filterBuilder);
    }

    @Override
    public boolean isInvalid() {
        return commonQueryCondition == null && commonFilterCondition == null;
    }

    protected SearchRequestBuilder getSrb() {
        return searchRequestBuilder;
    }

    protected int getPageSize() {
        return pageSize;
    }

    /**
     * @return if return null, searchRequestBuilder had init, if not null, searchRequestBuilder is new created
     */
    protected SearchRequestBuilder initSearchRequestBuilder() {
        if (searchRequestBuilder != null) return null;
        searchRequestBuilder = getClient().prepareSearch(tableInfo.getType().getIndex())
                .setTypes(tableInfo.getType().getType());
        if (pageSize > 0) searchRequestBuilder.setSize(pageSize);
        if (commonQueryCondition != null) {
            boolQueryBuilder = QueryBuilders.boolQuery();
            for (QueryBuilder qb : commonQueryCondition) {
                boolQueryBuilder.must(qb);
            }
            searchRequestBuilder.setQuery(boolQueryBuilder);
        }
        if (commonFilterCondition != null) {
            andFilterBuilder = FilterBuilders.andFilter();
            for (FilterBuilder fb : commonFilterCondition) {
                andFilterBuilder.add(fb);
            }
            searchRequestBuilder.setPostFilter(andFilterBuilder);
        }
        return searchRequestBuilder;
    }

}
