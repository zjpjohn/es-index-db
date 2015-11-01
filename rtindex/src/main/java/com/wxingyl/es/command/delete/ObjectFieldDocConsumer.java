package com.wxingyl.es.command.delete;

import com.wxingyl.es.command.DocConsumer;
import com.wxingyl.es.command.RootNode;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;

/**
 * Created by xing on 15/10/31.
 * delete child object field where objKeyFieldName value is equals keyValue
 */
public class ObjectFieldDocConsumer implements DocConsumer {

    private final String objKeyFieldName;

    private final Object keyValue;

    private final boolean nullValueDelete;

    public ObjectFieldDocConsumer(String objKeyFieldName, Object keyValue) {
        this(objKeyFieldName, keyValue, true);
    }

    /**
     * @param objKeyFieldName keyFieldName of object field, can not null
     * @param keyValue        keyField value, can not null
     * @param nullValueDelete If get objKeyFieldName value is null of document, default delete
     */
    public ObjectFieldDocConsumer(String objKeyFieldName, Object keyValue, boolean nullValueDelete) {
        this.objKeyFieldName = objKeyFieldName;
        this.keyValue = keyValue;
        this.nullValueDelete = nullValueDelete;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void accept(Object parentDoc, RootNode.Node field) {
        if (parentDoc instanceof Collection) {
            for (Map<String, Object> m : (Collection<Map<String, Object>>) parentDoc) {
                objectFieldReplace(m, field.field());
            }
        } else {
            objectFieldReplace((Map<String, Object>) parentDoc, field.field());
        }
    }

    @SuppressWarnings("unchecked")
    private void objectFieldReplace(Map<String, Object> parentMap, final String childField) {
        Object obj = parentMap.get(childField);
        if (obj == null) return;
        if (obj instanceof Collection) {
            Collection<Map<String, Object>> collection = (Collection<Map<String, Object>>) obj;
            Iterator<Map<String, Object>> it = collection.iterator();
            while (it.hasNext()) {
                if (canDelete(it.next())) {
                    it.remove();
                }
            }
            if (collection.isEmpty()) parentMap.remove(childField);
        } else if (canDelete((Map<String, Object>) obj)) {
            parentMap.remove(childField);
        }
    }

    private boolean canDelete(Map<String, Object> map) {
        Object obj = map.get(objKeyFieldName);
        if (obj == null) return nullValueDelete;
        else return obj.equals(keyValue);
    }

}
