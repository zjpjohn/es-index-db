package com.wxingyl.es.command.update;

/**
 * Created by xing on 15/10/19.
 * changed field entry
 */
public class ChangedFieldEntry {

    private Object beforeValue;

    private Object afterValue;

    private boolean isOnlyReplaceVal;

    public ChangedFieldEntry(Object beforeValue, Object afterValue) {
        this.beforeValue = beforeValue;
        this.afterValue = afterValue;
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

}
