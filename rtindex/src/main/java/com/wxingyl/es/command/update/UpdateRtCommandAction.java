package com.wxingyl.es.command.update;

import com.wxingyl.es.action.adapter.IndexTypeInfo;
import com.wxingyl.es.command.AbstractPreQueryRtCommand;
import com.wxingyl.es.command.RootNode;
import com.wxingyl.es.index.IndexTypeDesc;
import com.wxingyl.es.util.CommonUtils;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;

import java.util.*;

/**
 * Created by xing on 15/10/10.
 * update real time command action
 */
public class UpdateRtCommandAction extends AbstractPreQueryRtCommand implements UpdateRtCommand {

//    private final Map<String, ChangedFieldEntry> changeFieldMap = new HashMap<>();

    private int changeEntryQueryCount;

    private boolean needContinue = true;

    private final RootNode rootNode = new RootNode();

    private final Map<RootNode.Node, ChangedFieldEntry> nodeChangeFieldMap = new HashMap<>();

    public UpdateRtCommandAction(IndexTypeInfo.TableInfo tableInfo) {
        this(tableInfo, 100);
    }

    public UpdateRtCommandAction(IndexTypeInfo.TableInfo tableInfo, int pageSize) {
        super(tableInfo, pageSize);
    }

    @Override
    protected SearchRequestBuilder initSearchRequestBuilder() {
        SearchRequestBuilder srb = super.initSearchRequestBuilder();
        if (srb == null) return null;
        if (changeEntryQueryCount > 0) {
            if (andFilterBuilder == null) {
                andFilterBuilder = FilterBuilders.andFilter();
                srb.setPostFilter(andFilterBuilder);
            }
            for (RootNode.Node node : nodeChangeFieldMap.keySet()) {
                ChangedFieldEntry entry = nodeChangeFieldMap.get(node);
                if (entry.isOnlyReplaceVal()) continue;
                andFilterBuilder.add(builderFilter(node.fullName(), entry.getBeforeValue()));
            }
        }
        return srb;
    }

    private FilterBuilder builderFilter(String field, Object value) {
        return value == null ? FilterBuilders.missingFilter(field) : FilterBuilders.termFilter(field, value);
    }

    private void replaceDoc(Map<String, Object> doc, RootNode.Node node, ChangedFieldEntry entry) {
        String docFieldName = node.field();
        if (!Objects.equals(entry.getBeforeValue(), doc.get(docFieldName))) return;
        if (entry.getAfterValue() == null) {
            doc.remove(docFieldName);
        } else {
            doc.put(docFieldName, entry.getAfterValue());
        }
    }

    @SuppressWarnings("unchecked")
    private List<Map<String, Object>> getDocFields(SearchResponse queryResponse) {
        SearchHits searchHits = queryResponse.getHits();
        List<Map<String, Object>> docs = new ArrayList<>(searchHits.hits().length);
        for (SearchHit e : searchHits) {
            final Map<String, Object> map = e.getSource();
            for (RootNode.Node node : nodeChangeFieldMap.keySet()) {
                Object obj = node.getSourceMap(map);
                if (obj == null) continue;
                ChangedFieldEntry entry = nodeChangeFieldMap.get(node);
                if (obj instanceof List) {
                    for (Map<String, Object> m : (List<Map<String, Object>>) obj) {
                        replaceDoc(m, node, entry);
                    }
                } else {
                    replaceDoc((Map<String, Object>) obj, node, entry);
                }
            }
            docs.add(map);
        }
        return docs;
    }

    @Override
    public void addChangeField(String docFieldName, ChangedFieldEntry entry) {
        if (entry == null) return;
        if (!entry.isOnlyReplaceVal() && getSrb() != null) {
            throw new IllegalStateException(getTypeTableMsg() + ": searchRequestBuilder had created, can not add more queryChange");
        }
        RootNode.Node node = rootNode.addNode(docFieldName);
        if (!nodeChangeFieldMap.containsKey(node) && !entry.isOnlyReplaceVal()) {
            changeEntryQueryCount++;
        }
        nodeChangeFieldMap.put(node, entry);
    }

    @Override
    public boolean needContinue() {
        return needContinue;
    }

    @Override
    public boolean isInvalid() {
        return nodeChangeFieldMap.isEmpty() ||
                (changeEntryQueryCount == 0 && super.isInvalid());
    }

    @Override
    public ActionRequestBuilder makeRequest() {
        initSearchRequestBuilder();
        SearchResponse queryResponse = getSrb().get();
        needContinue = queryResponse.getHits().getTotalHits() > getPageSize();
        List<Map<String, Object>> docs = getDocFields(queryResponse);
        if (CommonUtils.isEmpty(docs)) return null;
        BulkRequestBuilder bulkRequestBuilder = getClient().prepareBulk();
        IndexTypeDesc typeDesc = tableInfo.getType();
        for (Map<String, Object> f : docs) {
            bulkRequestBuilder.add(getClient().prepareUpdate(typeDesc.getIndex(), typeDesc.getType(),
                    f.get(tableInfo.getDocIdField()).toString())
                    .setDoc(f));
        }
        return bulkRequestBuilder;
    }

}
