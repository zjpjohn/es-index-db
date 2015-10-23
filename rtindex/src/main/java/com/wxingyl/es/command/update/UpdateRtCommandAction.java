package com.wxingyl.es.command.update;

import com.wxingyl.es.action.adapter.IndexTypeInfo;
import com.wxingyl.es.command.AbstractPreQueryRtCommand;
import com.wxingyl.es.exception.RtDocConvertJsonException;
import com.wxingyl.es.index.IndexTypeDesc;
import com.wxingyl.es.index.doc.DocFields;
import com.wxingyl.es.util.CommonUtils;
import com.wxingyl.es.util.EsUtils;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.*;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;

import java.io.IOException;
import java.util.*;

/**
 * Created by xing on 15/10/10.
 * update real time command action
 */
public class UpdateRtCommandAction extends AbstractPreQueryRtCommand implements UpdateRtCommand {

    private SearchRequestBuilder searchRequestBuilder;

    private final Map<String, ChangedFieldEntry> changeFieldMap = new HashMap<>();

    private int changeEntryQueryCount;

    private boolean needContinue = true;

    /**
     * operator document max number in a page
     */
    private final int pageSize;

    public UpdateRtCommandAction(IndexTypeInfo.TableInfo tableInfo) {
        this(tableInfo, 100);
    }

    public UpdateRtCommandAction(IndexTypeInfo.TableInfo tableInfo, int pageSize) {
        super(tableInfo);
        this.pageSize = pageSize;
    }

    private void initSearchRequestBuilder() {
        if (searchRequestBuilder != null) return;
        searchRequestBuilder = getClient().prepareSearch(tableInfo.getType().getIndex())
                .setTypes(tableInfo.getType().getType()).setSize(pageSize);
        List<QueryBuilder> commonQueryCondition = getCommonQueryCondition();
        if (commonQueryCondition != null) {
            if (commonQueryCondition.size() == 1) {
                searchRequestBuilder.setQuery(commonQueryCondition.get(0));
            } else {
                BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
                for (QueryBuilder qb : commonQueryCondition) {
                    boolQueryBuilder.must(qb);
                }
                searchRequestBuilder.setQuery(boolQueryBuilder);
            }
        }
        FilterBuilder[] filters = new FilterBuilder[changeEntryQueryCount];
        int index = 0;
        for (ChangedFieldEntry entry : changeFieldMap.values()) {
            if (entry.isOnlyReplaceVal()) continue;
            filters[index++] = builderFilter(entry.getDocFieldName(), entry.getBeforeValue());
        }
        FilterBuilder filterBuilder = changeEntryQueryCount == 1 ? filters[0] : FilterBuilders.orFilter(filters);
        if (getCommonFilterCondition() == null) {
            searchRequestBuilder.setPostFilter(filterBuilder);
        } else {
            AndFilterBuilder andFilterBuilder = FilterBuilders.andFilter(filterBuilder);
            for (FilterBuilder fb : getCommonFilterCondition()) {
                andFilterBuilder.add(fb);
            }
            searchRequestBuilder.setPostFilter(andFilterBuilder);
        }
    }

    private FilterBuilder builderFilter(String field, Object value) {
        return value == null ? FilterBuilders.missingFilter(field) : FilterBuilders.termFilter(field, value);
    }

    private void replaceDoc(Map<String, Object> doc, ChangedFieldEntry entry) {
        String docFieldName = entry.getFieldSplit() == null ? entry.getDocFieldName() :
                entry.getFieldSplit()[entry.getFieldSplit().length - 1];
        if (!Objects.equals(entry.getBeforeValue(), doc.get(docFieldName))) return;
        if (entry.getAfterValue() == null) {
            doc.remove(docFieldName);
        } else {
            doc.put(docFieldName, entry.getAfterValue());
        }
    }

    private List<DocFields> getDocFields(SearchResponse queryResponse) {
        SearchHits searchHits = queryResponse.getHits();
        List<DocFields> docs = new ArrayList<>(searchHits.hits().length);
        //this is only a local variate
        List<Map<String, Object>> child = new LinkedList<>();
        for (SearchHit e : searchHits) {
            Map<String, Object> map = e.getSource();
            for (String fieldName : changeFieldMap.keySet()) {
                ChangedFieldEntry entry = changeFieldMap.get(fieldName);
                if (entry.getFieldSplit() != null) {
                    EsUtils.findChildSource(map, entry.getFieldSplit(), child);
                    if (child.isEmpty()) {
                        continue;
                    }
                    for (Map<String, Object> m : child) {
                        replaceDoc(m, entry);
                    }
                    child.clear();
                } else {
                    replaceDoc(map, entry);
                }
            }
            DocFields docFields = new DocFields(map);
            docs.add(docFields);
        }
        return docs;
    }

    @Override
    public void addChangeField(ChangedFieldEntry entry) {
        if (entry == null) return;
        if (!entry.isOnlyReplaceVal() && searchRequestBuilder != null) {
            throw new IllegalStateException(getTypeTableMsg() + ": searchRequestBuilder had created, can not add more queryChange");
        }
        if (changeFieldMap.put(entry.getDocFieldName(), entry) == null && !entry.isOnlyReplaceVal()) {
            changeEntryQueryCount++;
        }
    }

    @Override
    public void addPreFilter(FilterBuilder filterBuilder) {
        if (searchRequestBuilder != null) {
            throw new IllegalStateException(getTypeTableMsg() + ": searchRequestBuilder had created, can not add more filter");
        }
        super.addPreFilter(filterBuilder);
    }

    @Override
    public void addPreQuery(QueryBuilder queryBuilder) {
        if (searchRequestBuilder != null) {
            throw new IllegalStateException(getTypeTableMsg() + ": searchRequestBuilder had created, can not add more query");
        }
        super.addPreQuery(queryBuilder);
    }

    @Override
    public boolean needContinue() {
        return needContinue;
    }

    @Override
    public boolean isInvalid() {
        return changeFieldMap.isEmpty() || (changeEntryQueryCount == 0
                && super.isInvalid());
    }

    @Override
    public ActionRequestBuilder makeRequest() {
        initSearchRequestBuilder();
        SearchResponse queryResponse = searchRequestBuilder.execute().actionGet();
        needContinue = queryResponse.getHits().getTotalHits() > pageSize;
        List<DocFields> docs = getDocFields(queryResponse);
        if (CommonUtils.isEmpty(docs)) return null;
        BulkRequestBuilder bulkRequestBuilder = getClient().prepareBulk();
        IndexTypeDesc typeDesc = tableInfo.getType();
        for (DocFields f : docs) {
            try {
                bulkRequestBuilder.add(getClient().prepareUpdate(typeDesc.getIndex(), typeDesc.getType(),
                        f.get(tableInfo.getIdField()).toString())
                        .setDoc(f.buildXContent(null)));
            } catch (IOException e) {
                throw new RtDocConvertJsonException(getTypeTableMsg(), e);
            }
        }
        return bulkRequestBuilder;
    }

}
