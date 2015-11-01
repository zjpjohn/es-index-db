package com.wxingyl.es.command;

import com.wxingyl.es.action.adapter.IndexTypeInfo;
import com.wxingyl.es.util.CommonUtils;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.*;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;

import java.util.*;

/**
 * Created by xing on 15/10/23.
 * abstract PreQueryRtCommand
 */
public abstract class AbstractEntryPreQueryRtAction<T extends FieldEntry> extends AbstractPreQueryRtAction implements EntryPreQueryRtCommand<T> {

    private int changeEntryQueryCount;

    private final RootNode rootNode = new RootNode();

    private final Map<RootNode.Node, T> nodeFieldMap = new HashMap<>();

    public AbstractEntryPreQueryRtAction(IndexTypeInfo.TableInfo tableInfo, int pageSize) {
        super(tableInfo, pageSize);
    }

    /**
     * child class self define, default do nothing
     */
    protected void initSearchRequestBuilder(SearchRequestBuilder srb) {
        if (changeEntryQueryCount > 0) {
            if (andFilterBuilder == null) {
                andFilterBuilder = FilterBuilders.andFilter();
                srb.setPostFilter(andFilterBuilder);
            }
            FilterBuilder filterBuilder;
            for (RootNode.Node node : nodeFieldMap.keySet()) {
                T entry = nodeFieldMap.get(node);
                if (entry.isQueryCondition() && (filterBuilder = entry.filter(node.fullName())) != null) {
                    andFilterBuilder.add(filterBuilder);
                }
            }
        }
    }

    protected abstract void replaceDocField(Object parentMap, RootNode.Node node, T entry);

    /**
     * @return no search condition return true, other return false
     */
    @Override
    public boolean isInvalid() {
        return nodeFieldMap.isEmpty() ||
                (changeEntryQueryCount == 0 && super.isInvalid());
    }

    @Override
    public void addFieldEntry(String docFieldName, T entry) {
        Objects.requireNonNull(docFieldName);
        Objects.requireNonNull(entry);
        if (entry.isQueryCondition() && getSrb() != null) {
            throw new IllegalStateException(getTypeTableMsg() + ": searchRequestBuilder had created, can not add more queryChange");
        }
        RootNode.Node node = rootNode.addNode(docFieldName);
        if (node == null) {
            throw new IllegalArgumentException("docFieldName: " + docFieldName + " is illegal");
        }
        if (!nodeFieldMap.containsKey(node) && entry.isQueryCondition()) {
            changeEntryQueryCount++;
        }
        nodeFieldMap.put(node, entry);
    }

    @Override
    public ActionRequestBuilder makeRequest() {
        SearchResponse queryResponse = query();
        SearchHits searchHits = queryResponse.getHits();
        List<Map<String, Object>> docs = new ArrayList<>(searchHits.hits().length);
        for (SearchHit e : searchHits) {
            final Map<String, Object> source = e.getSource();
            for (RootNode.Node node : nodeFieldMap.keySet()) {
                Object obj = node.getSourceMap(source);
                if (obj != null) {
                    T entry = nodeFieldMap.get(node);
                    if (entry.getDocConsumer() == null) {
                        replaceDocField(obj, node, nodeFieldMap.get(node));
                    } else {
                        entry.getDocConsumer().accept(obj, node);
                    }
                }
            }
            docs.add(source);
        }
        if (CommonUtils.isEmpty(docs)) return null;
        return RtEsUtils.bulkUpdateRequest(tableInfo, docs);
    }
}
