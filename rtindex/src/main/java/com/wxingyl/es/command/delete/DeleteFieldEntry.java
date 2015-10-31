package com.wxingyl.es.command.delete;

import com.wxingyl.es.command.FieldEntry;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.index.query.FilterBuilders;

/**
 * Created by xing on 15/10/31.
 * delete field entry
 */
public class DeleteFieldEntry extends FieldEntry {

    private Object srcValue;

    public Object getSrcValue() {
        return srcValue;
    }

    public static Builder build() {
        return new Builder();
    }

    @Override
    public FilterBuilder filter(String docField) {
        return srcValue == null ? null : FilterBuilders.termFilter(docField, srcValue);
    }

    public static class Builder extends FieldEntry.Builder<DeleteFieldEntry> {

        private Object srcValue;

        public Builder srcValue(Object srcValue) {
            this.srcValue = srcValue;
            return this;
        }

        public Builder objectFieldDocConsumer(String objKeyFieldName, Object keyValue) {
            this.docConsumer = new ObjectFieldDocConsumer(objKeyFieldName, keyValue);
            return this;
        }

        @Override
        protected DeleteFieldEntry get() {
            DeleteFieldEntry entry = new DeleteFieldEntry();
            entry.srcValue = srcValue;
            return entry;
        }
    }
}
