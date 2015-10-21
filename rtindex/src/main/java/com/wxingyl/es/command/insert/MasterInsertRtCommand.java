package com.wxingyl.es.command.insert;

import com.wxingyl.es.action.IndexTypeInfo;
import com.wxingyl.es.index.doc.PageDocument;

/**
 * Created by xing on 15/10/21.
 * MasterInsertRtCommand
 */
public interface MasterInsertRtCommand extends InsertRtCommand {

    PageDocument docCreate();

    IndexTypeInfo.TableInfo getTableInfo();
}
