package com.wxingyl.es.command;

import com.wxingyl.es.action.adapter.IndexTypeInfo;
import com.wxingyl.es.index.IndexTypeDesc;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.client.Client;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Created by xing on 15/10/28.
 * es utils about real time
 */
public abstract class RtEsUtils {

    public static BulkRequestBuilder bulkUpdateRequest(IndexTypeInfo.TableInfo tableInfo, List<Map<String, Object>> docs) {
        final IndexTypeDesc type = tableInfo.getType();
        final String idField = tableInfo.getDocIdField();
        final Client client = tableInfo.getIndexManager().getClient();
        BulkRequestBuilder bulkRequestBuilder = client.prepareBulk();
        for (Map<String, Object> f : docs) {
            bulkRequestBuilder.add(client.prepareUpdate(type.getIndex(), type.getType(),
                    f.get(idField).toString())
                    .setDoc(f));
        }
        return bulkRequestBuilder;
    }

    private static void replaceDoc(Map<String, Object> parentDocMap, String field, Object destObj, Object newObj) {
        if (destObj == null || Objects.equals(destObj, parentDocMap.get(field))) {
            if (newObj == null) {
                parentDocMap.remove(field);
            } else {
                parentDocMap.put(field, newObj);
            }
        }
    }

    /**
     * replace a field value in source document
     *
     * @param parentObj searchHit return document
     * @param field     child document field, not full-path
     * @param destObj   dest object which need to change value
     * @param newObj    new object value, if newObj == null, will remove [doc.remove(docFieldName)],
     *                  is not [doc.put(docFieldName, null)]
     */
    @SuppressWarnings("unchecked")
    public static void replaceDocField(final Object parentObj, final String field, Object destObj, Object newObj) {
        if (parentObj instanceof Collection) {
            for (Map<String, Object> m : (Collection<Map<String, Object>>) parentObj) {
                replaceDoc(m, field, destObj, newObj);
            }
        } else {
            replaceDoc((Map<String, Object>) parentObj, field, destObj, newObj);
        }
    }
}
