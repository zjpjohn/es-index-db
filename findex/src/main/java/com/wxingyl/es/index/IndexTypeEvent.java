package com.wxingyl.es.index;

/**
 * Created by xing on 15/9/30.
 * index/type create event listener
 */
public class IndexTypeEvent {

    private IndexTypeDesc type;

    private IndexTypeEventTypeEnum eventType;

    public IndexTypeEvent(IndexTypeDesc type, IndexTypeEventTypeEnum eventType) {
        this.eventType = eventType;
        this.type = type;
    }

    public IndexTypeEventTypeEnum getEventType() {
        return eventType;
    }

    public IndexTypeDesc getType() {
        return type;
    }
}
