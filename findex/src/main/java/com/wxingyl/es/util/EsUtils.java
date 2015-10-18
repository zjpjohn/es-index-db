package com.wxingyl.es.util;

import com.wxingyl.es.index.doc.DocFields;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.search.SearchHit;

import java.util.List;
import java.util.Map;

/**
 * Created by xing on 15/9/14.
 * Es utils
 */
public abstract class EsUtils {

    public static void createNewIndex(IndicesAdminClient indicesAdminClient, String index,
                                      Settings settings, Map<String, String> typeMapping) {
        indicesAdminClient.create(new CreateIndexRequest(index, settings));
        if (CommonUtils.isEmpty(typeMapping)) return;
        PutMappingRequest mappingRequest = Requests.putMappingRequest(index);
        for (Map.Entry<String, String> e : typeMapping.entrySet()) {
            mappingRequest.type(e.getKey());
            mappingRequest.source(e.getValue());
            indicesAdminClient.putMapping(mappingRequest);
        }
    }

    public static void mergeSlaveResult(String prefixConflict, DocFields masterField, DocFields slaveField) {
        for(String key : slaveField.keySet()) {
            if (masterField.containsKey(key)) {
                masterField.put(prefixConflict + key, slaveField.get(key));
            } else {
                masterField.put(key, slaveField.get(key));
            }
        }
    }

    public static void mergeSlaveResult(String prefixConflict, DocFields masterField, Map<String, Object> slaveField) {
        for(String key : slaveField.keySet()) {
            if (masterField.containsKey(key)) {
                masterField.put(prefixConflict + key, slaveField.get(key));
            } else {
                masterField.put(key, slaveField.get(key));
            }
        }
    }

    /**
     * Search query result {@link SearchHit#getSource()}, field have child result,
     */
    public static void findChildSource(Map<String, Object> sourceMap, String[] fields, List<Map<String, Object>> child) {
        findChildSource(sourceMap, fields, 0, child);
    }

    public static void findChildSource(Map<String, Object> sourceMap, String[] fields, int startIndex, List<Map<String, Object>> child) {
        final int length = fields.length - 1;
        for (int i = startIndex; i < length; i++) {
            Object obj = sourceMap.get(fields[i]);
            if (obj == null) return;
            if (obj instanceof List) {
                for (Object e : (List) obj) {
                    findChildSource((Map<String, Object>) e, fields, i+1, child);
                }
                return;
            } else {
                sourceMap = (Map<String, Object>) obj;
            }
        }
        child.add(sourceMap);
    }

}
