package com.wxingyl.es.command;

/**
 * Created by xing on 15/10/31.
 * support {@link FieldEntry} rtCommand
 */
public interface EntryPreQueryRtCommand<T extends FieldEntry> extends PreQueryRtCommand {

    void addFieldEntry(String docFieldName, T entry);

}
