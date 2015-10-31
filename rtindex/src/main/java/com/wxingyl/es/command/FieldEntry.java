package com.wxingyl.es.command;

import org.elasticsearch.index.query.FilterBuilder;

/**
 * Created by xing on 15/10/31.
 * FieldEntry
 */
public abstract class FieldEntry {

    protected boolean isQueryCondition;

    protected DocConsumer docConsumer;

    public DocConsumer getDocConsumer() {
        return docConsumer;
    }

    public boolean isQueryCondition() {
        return isQueryCondition;
    }

    public abstract FilterBuilder filter(String docField);

    public static abstract class Builder<T extends FieldEntry> {

        protected boolean isQueryCondition;

        protected DocConsumer docConsumer;

        protected abstract T get();

        public T build() {
            T t = get();
            t.docConsumer = this.docConsumer;
            t.isQueryCondition = this.isQueryCondition;
            return t;
        }
    }
}
