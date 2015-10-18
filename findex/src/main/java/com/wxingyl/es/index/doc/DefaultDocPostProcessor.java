package com.wxingyl.es.index.doc;

import com.wxingyl.es.index.IndexSlaveResultMergeEnum;
import com.wxingyl.es.index.IndexTypeDesc;
import com.wxingyl.es.db.TableBaseInfo;
import com.wxingyl.es.db.result.TableQueryResult;
import com.wxingyl.es.util.CommonUtils;
import com.wxingyl.es.util.EsUtils;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Created by xing on 15/9/6.
 * default DocPostProcessor
 */
public class DefaultDocPostProcessor extends AbstractDocPostProcessor {

    @SuppressWarnings("unchecked")
    @Override
    public PageDocument applyTableQueryResult(PageDocument masterPageDoc, String masterField, TableQueryResult slaveResult) {
        TableBaseInfo slaveBaseInfo = slaveResult.getBaseInfo();
        Map<Object, List<Map<String, Object>>> slaveGroup = CommonUtils.groupListMap(slaveResult.getDbData(),
                slaveBaseInfo.getKeyField(), true);
        String keyAlias = slaveBaseInfo.getMasterAlias();
        IndexSlaveResultMergeEnum mergeType = slaveBaseInfo.getMergeType();
        if (mergeType == IndexSlaveResultMergeEnum.MERGE && !keyAlias.endsWith("_")) {
            keyAlias += '_';
        }
        for (DocFields doc : masterPageDoc) {
            Object obj = doc.get(masterField);
            if (obj == null) continue;
            Object valObj = mergeType.function(slaveGroup.get(obj));
            if (valObj != null) {
                if (mergeType == IndexSlaveResultMergeEnum.MERGE) {
                    EsUtils.mergeSlaveResult(keyAlias, doc, (Map) valObj);
                } else {
                    doc.put(keyAlias, valObj);
                }
            }
        }
        return masterPageDoc;
    }

    @Override
    public PageDocument mergeChildPageDoc(PageDocument masterPageDoc, String masterField, PageDocument childPageDoc) {
        Map<Object, List<DocFields>> group = childPageDoc.groupByKeyField(true);
        String masterAlias = childPageDoc.getBaseInfo().getMasterAlias();
        IndexSlaveResultMergeEnum mergeType = childPageDoc.getBaseInfo().getMergeType();
        if (mergeType == IndexSlaveResultMergeEnum.MERGE && !masterAlias.endsWith("_")) {
            masterAlias += '_';
        }
        for (DocFields doc : masterPageDoc) {
            Object val = doc.get(masterField);
            if (val != null) {
                Object obj = mergeType.function(group.get(val));
                if (obj == null) continue;
                if (mergeType == IndexSlaveResultMergeEnum.MERGE) {
                    EsUtils.mergeSlaveResult(masterAlias, doc, (DocFields) obj);
                } else {
                    doc.put(masterAlias, obj);
                }
            }
        }
        return masterPageDoc;
    }

    @Override
    public Set<IndexTypeDesc> supportType() {
        return null;
    }
}
