package com.wxingyl.es.command;

import com.wxingyl.es.index.IndexTypeDesc;
import com.wxingyl.es.index.doc.DocFields;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by xing on 15/10/10.
 * update real time command action
 */
public class UpdateRtCommandAction implements UpdateRtCommand {

    private Client client;

    private IndexTypeDesc type;

    private String idField;

    private Map<String, String> orgValMap = new HashMap<>();

    private Map<String, String> newValMap = new HashMap<>();

    private boolean needContinue = true;

    public UpdateRtCommandAction(Client client, IndexTypeDesc type, String idField) {
        this.client = client;
        this.type = type;
        this.idField = idField;
    }

    @Override
    public void addField(String fieldName, String orgVal, String newVal) {
        orgValMap.put(fieldName, orgVal);
        newValMap.put(fieldName, newVal);
    }

    @Override
    public SearchResponse query(int pageSize) {
//        client.prepareSearch()
        return null;
    }

    @Override
    public List<DocFields> replaceChange() {
        return null;
    }

    @Override
    public void updateDoc(List<DocFields> docs) {

    }

    @Override
    public boolean needContinue() {
        return needContinue;
    }
}
