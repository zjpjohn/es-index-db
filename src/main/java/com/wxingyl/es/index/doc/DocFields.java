package com.wxingyl.es.index.doc;

import com.wxingyl.es.util.DateConvert;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

import java.io.IOException;
import java.util.*;

/**
 * Created by xing on 15/9/6.
 * document
 */
public class DocFields {

    protected Map<String, Object> sourceMap;

    public DocFields() {
        sourceMap = new HashMap<>();
    }

    public DocFields(int initialCapacity) {
        sourceMap = new HashMap<>(initialCapacity);
    }

    public Object get(String key) {
        return sourceMap.get(key);
    }

    public Object put(String key, Object value) {
        return sourceMap.put(key, value);
    }

    public void putAll(Map<? extends String, ?> m) {
        sourceMap.putAll(m);
    }

    public Object remove(String key) {
        return sourceMap.remove(key);
    }

    public boolean containsKey(String key) {
        return sourceMap.containsKey(key);
    }

    public XContentBuilder buildXContent(DateConvert dateConvert) throws IOException {
        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder().startObject();
        fillXContentBuilder(xContentBuilder, sourceMap, dateConvert);
        return xContentBuilder;
    }

    @SuppressWarnings("unchecked")
    protected void fillXContentBuilder(XContentBuilder builder, Map<String, ?> map, DateConvert dateConvert) throws IOException {
        builder.startObject();
        for (Map.Entry<String, ?> e : map.entrySet()) {
            Object v = e.getValue();
            if (v instanceof Date) {
                builder.field(e.getKey(), dateConvert.format((Date) v));
            } else if (v instanceof Map) {
                fillXContentBuilder(builder, (Map) v, dateConvert);
            } else {
                builder.field(e.getKey(), v);
            }
        }
        builder.endObject();
    }

    public static List<DocFields> build(List<Map<String, Object>> data) {
        List<DocFields> list = new ArrayList<>(data.size());
        data.forEach(map -> {
            DocFields fields = new DocFields(map.size());
            fields.putAll(map);
            list.add(fields);
        });
        return list;
    }
}
