package com.wxingyl.es.command;

import com.wxingyl.es.action.adapter.IndexTypeInfo;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.*;

import java.util.LinkedList;
import java.util.List;

/**
 * Created by xing on 15/10/23.
 * abstract PreQueryRtCommand
 */
public abstract class AbstractPreQueryRtAction extends AbstractRtAction implements PreQueryRtCommand {

    private List<QueryBuilder> commonQueryCondition;

    private List<FilterBuilder> commonFilterCondition;

    private boolean needContinue = true;
    /**
     * operator document max number in a page
     */
    private final int pageSize;

    private SearchRequestBuilder searchRequestBuilder;

    protected BoolQueryBuilder boolQueryBuilder;

    protected AndFilterBuilder andFilterBuilder;

    public AbstractPreQueryRtAction(IndexTypeInfo.TableInfo tableInfo, int pageSize) {
        super(tableInfo);
        this.pageSize = pageSize;
    }

    /**
     * child class self define, default do nothing
     */
    protected void initSearchRequestBuilder(SearchRequestBuilder srb) {
    }

    protected final SearchRequestBuilder getSrb() {
        return searchRequestBuilder;
    }

    protected final SearchResponse query() {
        if (searchRequestBuilder == null) {
            //init search request builder
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
                searchRequestBuilder.setPostFilter(andFilterBuilder);
                for (FilterBuilder fb : commonFilterCondition) {
                    andFilterBuilder.add(fb);
                }
            }
            initSearchRequestBuilder(searchRequestBuilder);
        }
        SearchResponse queryResponse = searchRequestBuilder.get();
        needContinue = queryResponse.getHits().getTotalHits() > pageSize;
        return queryResponse;
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

    /**
     * @return no search condition return true, other return false
     */
    @Override
    public boolean isInvalid() {
        return commonQueryCondition == null && commonFilterCondition == null;
    }

    @Override
    public boolean needContinue() {
        return needContinue;
    }

}
