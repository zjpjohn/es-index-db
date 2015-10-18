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
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.QueryBuilder;
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

    private Map<String, Object> orgValChangeMap = new HashMap<>();

    private Map<String, Object> newValChangeMap = new HashMap<>();

    private Map<String, String[]> fieldSplitChangeMap = new HashMap<>();

    private boolean needContinue = true;

    private QueryBuilder commonQueryCondition;

    private FilterBuilder commonFilterCondition;

    public UpdateRtCommandAction(Client client, IndexTypeDesc type, String idField) {
        this.client = client;
        this.type = type;
        this.idField = idField;
    }

    @Override
    public void addChangeField(String fieldName, Object orgVal, Object newVal) {
        if (Objects.equals(orgVal, newVal)) return;
        String[] fieldSplit = CommonUtils.split(fieldName, '.');
        if (fieldSplit.length > 1) {
            fieldSplitChangeMap.put(fieldName, fieldSplit);
        }
        orgValChangeMap.put(fieldName, orgVal);
        newValChangeMap.put(fieldName, newVal);
    }

    protected SearchRequestBuilder createSearchRequestBuilder() {
        searchRequestBuilder = client.prepareSearch(type.getIndex())
                .setTypes(type.getType());
        if (commonQueryCondition != null) {
            searchRequestBuilder.setQuery(commonQueryCondition);
        }
        String[] keys = orgValChangeMap.keySet().toArray(new String[orgValChangeMap.size()]);
        FilterBuilder filterBuilder;
        if (keys.length == 1) {
            filterBuilder = builderFilter(keys[0], orgValChangeMap.get(keys[0]));
        } else {
            FilterBuilder[] filters = new FilterBuilder[orgValChangeMap.size()];
            for (int i = 0; i < keys.length; i++) {
                filters[i] = builderFilter(keys[i], orgValChangeMap.get(keys[i]));
            }
            filterBuilder = FilterBuilders.orFilter(filters);
        }
        searchRequestBuilder.setPostFilter(commonFilterCondition == null ? filterBuilder
                : FilterBuilders.andFilter(commonFilterCondition, filterBuilder));
        return searchRequestBuilder;
    }


    @Override
    public SearchResponse query(int pageSize) {
        if (orgValChangeMap.isEmpty()) return null;
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
            for (String fieldName : orgValChangeMap.keySet()) {
                if (fieldSplitChangeMap.containsKey(fieldName)) {
                    String[] filedArray = fieldSplitChangeMap.get(fieldName);
                    EsUtils.findChildSource(map, filedArray, child);
                    if (child.isEmpty()) {
                        continue;
                    }
                    for (Map<String, Object> m : child) {
                        replaceDoc(m, filedArray[filedArray.length - 1], fieldName);
                    }
                    child.clear();
                } else {
                    replaceDoc(map, fieldName, fieldName);
                }
            }
            DocFields docFields = new DocFields(map);
            retDocs.add(docFields);
        }
        return retDocs;
    }

    private void replaceDoc(Map<String, Object> doc, String docFieldName, String fieldName) {
        Object orgVal = orgValChangeMap.get(fieldName);
        Object docOrgVal = doc.get(docFieldName);
        if (!Objects.equals(orgVal, docOrgVal)) return;
        Object newVal = newValChangeMap.get(fieldName);
        if (newVal == null) {
            doc.remove(docFieldName);
        } else {
            doc.put(docFieldName, newVal);
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
    public void preQueryDocCondition(QueryBuilder queryBuilder, FilterBuilder filterBuilder) {
        commonQueryCondition = queryBuilder;
        commonFilterCondition = filterBuilder;
    }
}
