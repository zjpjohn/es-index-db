package com.wxingyl.es.command.update;

import com.wxingyl.es.util.CommonUtils;

/**
 * Created by xing on 15/10/19.
 * changed field entry
 */
public class ChangedFieldEntry {

    private String docFieldName;

    private Object beforeValue;

    private Object afterValue;

    private String[] fieldSplit;

    private boolean isOnlyReplaceVal;

    public ChangedFieldEntry(String docFieldName, Object beforeValue, Object afterValue) {
        this.docFieldName = docFieldName;
        this.beforeValue = beforeValue;
        this.afterValue = afterValue;
        String[] fieldSplit = CommonUtils.split(docFieldName, '.');
        if (fieldSplit.length > 1) {
            this.fieldSplit = fieldSplit;
        }
    }

    public void setIsOnlyReplaceVal(boolean isOnlyReplaceVal) {
        this.isOnlyReplaceVal = isOnlyReplaceVal;
    }

    public boolean isOnlyReplaceVal() {
        return isOnlyReplaceVal;
    }

    public Object getAfterValue() {
        return afterValue;
    }

    public Object getBeforeValue() {
        return beforeValue;
    }

    public String getDocFieldName() {
        return docFieldName;
    }

    public String[] getFieldSplit() {
        return fieldSplit;
    }
}
