package com.wxingyl.es.index;

import com.wxingyl.es.jdal.TableQueryResult;
import com.wxingyl.es.util.CommonUtils;

import java.util.List;
import java.util.Map;

/**
 * Created by xing on 15/9/6.
 * default DocPostProcessor
 */
public class DefaultDocPostProcessor implements DocPostProcessor<DocFields> {

    /**
     * default postProcessor in {@link IndexDocFiller#document}
     */
    @Override
    public PageDocument<DocFields> postProcessor(DbQueryDependResult masterResult) {
        return null;
    }

    @Override
    public PageDocument<DocFields> initMasterPageDoc(TableQueryResult masterResult) {
        PageDocument<DocFields> masterPageDoc = new PageDocument<>(masterResult.getKeyField(), masterResult.getMasterAlias());
        masterPageDoc.addAll(DocFields.build(masterResult.getDbData()));
        return masterPageDoc;
    }

    @Override
    public <R extends DocFields> PageDocument<R> applyTableQueryResult(PageDocument<R> masterPageDoc, String masterField, TableQueryResult slaveResult) {
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
    public <R extends DocFields> PageDocument<R> mergeChildPageDoc(PageDocument<R> masterPageDoc, String masterField, PageDocument<R> childPageDoc) {
        Map<Object, List<R>> group = childPageDoc.groupByKeyField(true);
        String masterAlias = childPageDoc.getMasterAlias();
        masterPageDoc.forEach(doc -> {
            Object val = doc.get(masterField);
            if (val != null && group.get(val) != null) {
                doc.put(masterAlias, group.get(val));
            }
        });
        return masterPageDoc;
    }

    @Override
    public IndexTypeDesc supportType() {
        return null;
    }
}
