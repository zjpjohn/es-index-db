package com.wxingyl.es.command.delete;

import com.wxingyl.es.command.DocConsumer;
import com.wxingyl.es.command.RootNode;

/**
 * Created by xing on 15/10/31.
 * delete child object field where objKeyFieldName value is equals keyValue
 */
public class ObjectFieldDocConsumer implements DocConsumer {

    private String objKeyFieldName;

    private Object keyValue;

    /**
     * @param objKeyFieldName if it is null, we will remove all child object
     * @param keyValue keyField value
     */
    public ObjectFieldDocConsumer(String objKeyFieldName, Object keyValue) {
        this.keyValue = keyValue;
        this.objKeyFieldName = objKeyFieldName;
    }

    @Override
    public void accept(Object parentDoc, RootNode.Node field) {

    }

}
