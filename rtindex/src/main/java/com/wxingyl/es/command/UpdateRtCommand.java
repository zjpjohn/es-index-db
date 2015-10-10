package com.wxingyl.es.command;

import com.wxingyl.es.index.doc.DocFields;
import org.elasticsearch.action.search.SearchResponse;

import java.util.List;

/**
 * Created by xing on 15/10/8.
 * real-time index update/insert/delete command
 */
public interface UpdateRtCommand {

    void addField(String fieldName, String orgVal, String newVal);

    SearchResponse query(int pageSize);

    List<DocFields> replaceChange();

    void updateDoc(List<DocFields> docs);

    boolean needContinue();

}
