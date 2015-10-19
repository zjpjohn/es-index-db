package com.wxingyl.es.command;

import com.wxingyl.es.index.doc.DocFields;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;

import java.io.IOException;
import java.util.List;

/**
 * Created by xing on 15/10/8.
 * update RtCommand
 */
public interface UpdateRtCommand extends RtCommand {

    void addChangeField(ChangedFieldEntry entry);

    SearchResponse query(int pageSize);

    List<DocFields> replaceChange(SearchResponse queryResponse);

    BulkResponse updateDoc(List<DocFields> docs) throws IOException;

    boolean needContinue();

}
