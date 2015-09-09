package com.wxingyl.es.index.post;

import com.wxingyl.es.db.result.NumberFieldValueProcessor;
import com.wxingyl.es.index.TypeBaseInfo;

import java.util.*;
import java.util.function.Consumer;

/**
 * Created by xing on 15/9/6.
 * document, have page
 */
public class PageDocument implements Iterable<DocFields> {

    private TypeBaseInfo baseInfo;

    private List<DocFields> docs;

    public PageDocument(TypeBaseInfo baseInfo) {
        super();
        this.baseInfo = baseInfo;
        docs = new LinkedList<>();
    }

    public void add(DocFields doc) {
        docs.add(doc);
    }

    public void addAll(Collection<? extends DocFields> collection) {
        docs.addAll(collection);
    }

    public TypeBaseInfo getBaseInfo() {
        return baseInfo;
    }

    public Map<Object, List<DocFields>> groupByKeyField(boolean removeKeyField) {
        Map<Object, List<DocFields>> group = new HashMap<>();
        docs.forEach(doc -> {
            Object val = doc.get(baseInfo.getKeyField());
            List<DocFields> list = group.get(val);
            if (list == null) {
                group.put(val, list = new LinkedList<>());
            }
            if (removeKeyField) doc.remove(baseInfo.getKeyField());
            list.add(doc);
        });
        return group;
    }

    public DocFields getDocFieldsByKeyVal(Object keyVal) {
        String keyField = baseInfo.getKeyField();
        keyVal = NumberFieldValueProcessor.INSTANCE.handle(keyField, keyVal);
        for(DocFields doc : docs) {
            if (keyVal.equals(doc.get(keyField))) return doc;
        }
        return null;
    }

    @Override
    public Iterator<DocFields> iterator() {
        return docs.iterator();
    }

    @Override
    public void forEach(Consumer<? super DocFields> action) {
        Objects.requireNonNull(action);
        for (DocFields doc : docs) {
            action.accept(doc);
        }
    }
}
