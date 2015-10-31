package com.wxingyl.es.command.update;


import com.wxingyl.es.command.DocConsumer;
import com.wxingyl.es.command.FieldEntry;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.index.query.FilterBuilders;

/**
 * Created by xing on 15/10/19.
 * changed field entry
 */
public class UpdateFieldEntry extends FieldEntry {

    private Object beforeValue;

    private Object afterValue;

    public Object getAfterValue() {
        return afterValue;
    }

    public Object getBeforeValue() {
        return beforeValue;
    }

    @Override
    public FilterBuilder filter(String docField) {
        return beforeValue == null ? FilterBuilders.missingFilter(docField)
                : FilterBuilders.termFilter(docField, beforeValue);
    }

    public static Builder build() {
        return new Builder();
    }

    public static class Builder extends FieldEntry.Builder<UpdateFieldEntry> {

        private Object beforeValue;

        private Object afterValue;

        public Builder isQueryCondition(boolean isQueryCondition) {
            this.isQueryCondition = isQueryCondition;
            return this;
        }

        public Builder docConsumer(DocConsumer docConsumer) {
            this.docConsumer = docConsumer;
            return this;
        }

        public Builder beforeValue(Object beforeValue) {
            this.beforeValue = beforeValue;
            return this;
        }

        public Builder afterValue(Object afterValue) {
            this.afterValue = afterValue;
            return this;
        }

        @Override
        protected UpdateFieldEntry get() {
            UpdateFieldEntry entry = new UpdateFieldEntry();
            entry.afterValue = afterValue;
            entry.beforeValue = beforeValue;
            return entry;
        }

    }

}
