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

    private Map<String, Object> sourceMap;

    private ThreadLocal<DateConvert> dateConvertLocal = new ThreadLocal<>();

    public DocFields(Map<String, Object> source) {
        sourceMap = source;
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

    public Set<String> keySet() {
        return sourceMap.keySet();
    }

    public XContentBuilder buildXContent(DateConvert dateConvert) throws IOException {
        if (dateConvert != null) dateConvertLocal.set(dateConvert);
        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder();
        fillXContentBuilder(xContentBuilder, sourceMap);
        if (dateConvert != null) dateConvertLocal.remove();
        return xContentBuilder;
    }

    private void fillXContentBuilder(XContentBuilder builder, Map<String, Object> map) throws IOException {
        builder.startObject();
        for (Map.Entry<String, Object> e : map.entrySet()) {
            builder.field(e.getKey());
            Object v = e.getValue();
            if (v instanceof Iterable) {
                builder.startArray();
                for (Object obj : (Iterable)v) {
                    fillXContentValue(builder, obj);
                }
                builder.endArray();
            } else if (v instanceof DocFields[]) {
                builder.startArray();
                for (Object obj : (DocFields[]) v) {
                    fillXContentValue(builder, obj);
                }
                builder.endArray();
            } else {
                fillXContentValue(builder, v);
            }
        }
        builder.endObject();
    }

    @SuppressWarnings("unchecked")
    private void fillXContentValue(XContentBuilder builder, Object val) throws IOException {
        if (val instanceof DocFields) {
            fillXContentBuilder(builder, ((DocFields)val).sourceMap);
        } else if (val instanceof Map) {
            fillXContentBuilder(builder, (Map) val);
        } else if (val instanceof Date && dateConvertLocal.get() != null) {
            builder.value(dateConvertLocal.get().format((Date) val));
        } else {
            builder.value(val);
        }
    }

    public static List<DocFields> build(List<Map<String, Object>> data) {
        List<DocFields> list = new ArrayList<>(data.size());
        for (Map<String, Object> map : data) {
            DocFields fields = new DocFields(map.size());
            fields.putAll(map);
            list.add(fields);
        }
        return list;
    }
}
