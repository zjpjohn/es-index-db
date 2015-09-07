package com.wxingyl.es.index;

import com.wxingyl.es.jdal.TableQueryResult;
import com.wxingyl.es.util.CommonUtils;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Created by xing on 15/9/6.
 * default DocPostProcessor
 */
public class DefaultDocPostProcessor extends AbstractDocPostProcessor {

    @Override
    public PageDocument postProcessor(DbQueryDependResult masterResult) {
        return null;
    }

    @Override
    public PageDocument initMasterPageDoc(TableQueryResult masterResult) {
        PageDocument masterPageDoc = new PageDocument(DocumentBaseInfo.build(masterResult, getPostEvn()));
        masterPageDoc.addAll(DocFields.build(masterResult.getDbData()));
        return masterPageDoc;
    }

    @Override
    public PageDocument applyTableQueryResult(PageDocument masterPageDoc, String masterField, TableQueryResult slaveResult) {
        Map<Object, List<Map<String, Object>>> group = CommonUtils.groupListMap(slaveResult.getDbData(),
                slaveResult.getKeyField(), true);
        String keyAlias = slaveResult.getMasterAlias();
        masterPageDoc.forEach(doc -> {
            Object obj = doc.get(masterField);
            if (obj != null && group.get(obj) != null) {
                doc.put(keyAlias, group.get(obj));
            }
        });
        return masterPageDoc;
    }

    @Override
    public PageDocument mergeChildPageDoc(PageDocument masterPageDoc, String masterField, PageDocument childPageDoc) {
        Map<Object, List<DocFields>> group = childPageDoc.groupByKeyField(true);
        String masterAlias = childPageDoc.getBaseInfo().getMasterAlias();
        masterPageDoc.forEach(doc -> {
            Object val = doc.get(masterField);
            if (val != null && group.get(val) != null) {
                doc.put(masterAlias, group.get(val));
            }
        });
        return masterPageDoc;
    }

    @Override
    public Set<IndexTypeDesc> supportType() {
        return null;
    }
}
