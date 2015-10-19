package com.wxingyl.es.command;

import com.wxingyl.es.index.IndexTypeDesc;
import com.wxingyl.es.index.doc.DocFields;
import com.wxingyl.es.util.CommonUtils;
import com.wxingyl.es.util.EsUtils;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.*;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;

import java.io.IOException;
import java.util.*;

/**
 * Created by xing on 15/10/10.
 * update real time command action
 */
public class UpdateRtCommandAction implements UpdateRtCommand {

    private Client client;

    private IndexTypeDesc type;

    private String idField;

    private SearchRequestBuilder searchRequestBuilder;

    private Map<String, ChangedFieldEntry> changeFieldMap = new HashMap<>();

    private int changeEntryQueryCount;

    private boolean needContinue = true;

    private List<QueryBuilder> commonQueryCondition;

    private List<FilterBuilder> commonFilterCondition;

    public UpdateRtCommandAction(Client client, IndexTypeDesc type, String idField) {
        this.client = client;
        this.type = type;
        this.idField = idField;
    }

    @Override
    public void addChangeField(ChangedFieldEntry entry) {
        if (entry == null) return;
        if (changeFieldMap.put(entry.getDocFieldName(), entry) == null && !entry.isOnlyReplaceVal()) {
            changeEntryQueryCount++;
        }
    }

    protected SearchRequestBuilder createSearchRequestBuilder() {
        searchRequestBuilder = client.prepareSearch(type.getIndex())
                .setTypes(type.getType());
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
        if (commonFilterCondition == null) {
            searchRequestBuilder.setPostFilter(filterBuilder);
        } else {
            AndFilterBuilder andFilterBuilder = FilterBuilders.andFilter(filterBuilder);
            for (FilterBuilder fb : commonFilterCondition) {
                andFilterBuilder.add(fb);
            }
            searchRequestBuilder.setPostFilter(andFilterBuilder);
        }
        return searchRequestBuilder;
    }


    @Override
    public SearchResponse query(int pageSize) {
        if (isInvalid()) return null;
        if (searchRequestBuilder == null) {
            createSearchRequestBuilder();
        }
        searchRequestBuilder.setSize(pageSize);
        SearchResponse response = searchRequestBuilder.execute().actionGet();
        needContinue = response.getHits().getTotalHits() > pageSize;
        return response;
    }

    private FilterBuilder builderFilter(String field, Object value) {
        return value == null ? FilterBuilders.missingFilter(field) : FilterBuilders.termFilter(field, value);
    }

    @Override
    public List<DocFields> replaceChange(SearchResponse queryResponse) {
        SearchHits searchHits = queryResponse.getHits();
        List<DocFields> retDocs = new ArrayList<>(searchHits.hits().length);
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
            retDocs.add(docFields);
        }
        return retDocs;
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

    @Override
    public BulkResponse updateDoc(List<DocFields> docs) throws IOException {
        if (CommonUtils.isEmpty(docs)) return null;
        BulkRequestBuilder bulkRequestBuilder = client.prepareBulk();
        for (DocFields f : docs) {
            bulkRequestBuilder.add(client.prepareUpdate(type.getIndex(), type.getType(), idField)
                    .setDoc(f.buildXContent(null)));
        }
        return bulkRequestBuilder.execute().actionGet();
    }

    @Override
    public boolean needContinue() {
        return needContinue;
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
        return changeFieldMap.isEmpty() || (changeEntryQueryCount == 0
                && commonQueryCondition == null && commonFilterCondition == null);
    }

}
