package com.wxingyl.es.action;

import com.wxingyl.es.index.IndexTypeDesc;

/**
 * Created by xing on 15/10/20.
 * abstract implement
 */
public abstract class AbstractTypeTableActionAdapter implements TypeTableActionAdapt {

    protected IndexTypeInfo.TableInfo tableInfo;

    protected TableColumnIndex tableColumnIndex;

    protected final IndexTypeDesc type;

    public AbstractTypeTableActionAdapter(IndexTypeDesc type) {
        this.type = type;
    }

    @Override
    public void initTableAction(TableAction action) {
        this.tableColumnIndex = action.tableColumnIndex();
        this.tableInfo = action.getTableAction(type);
    }
}
