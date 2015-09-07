package com.wxingyl.es.index.doc;

import com.wxingyl.es.index.IndexTypeDesc;
import com.wxingyl.es.jdal.TableQueryBaseInfo;

/**
 * Created by xing on 15/9/7.
 * document base info, such as index, type, table-name and so on
 */
public class DocumentBaseInfo extends TableQueryBaseInfo {

    private IndexTypeDesc type;

    public IndexTypeDesc getType() {
        return type;
    }

    public static DocumentBaseInfo build(TableQueryBaseInfo oldBaseInfo, IndexTypeDesc type) {
        if (oldBaseInfo instanceof DocumentBaseInfo) return (DocumentBaseInfo) oldBaseInfo;
        DocumentBaseInfo baseInfo = new DocumentBaseInfo();
        baseInfo.init(oldBaseInfo);
        baseInfo.type = type;
        return baseInfo;
    }
}
