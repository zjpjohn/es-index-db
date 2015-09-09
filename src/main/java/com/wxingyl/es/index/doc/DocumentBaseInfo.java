package com.wxingyl.es.index.doc;

import com.wxingyl.es.index.IndexTypeDesc;
import com.wxingyl.es.dbquery.TableQueryBaseInfo;

/**
 * Created by xing on 15/9/7.
 * document base info, such as index, type, table-name and so on
 */
public class DocumentBaseInfo extends TableQueryBaseInfo {

    private IndexTypeDesc type;

    public IndexTypeDesc getType() {
        return type;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof DocumentBaseInfo)) return false;
        if (!super.equals(o)) return false;

        DocumentBaseInfo that = (DocumentBaseInfo) o;

        return type.equals(that.type);

    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + type.hashCode();
        return result;
    }

    public static DocumentBaseInfo build(TableQueryBaseInfo oldBaseInfo, IndexTypeDesc type) {
        if (oldBaseInfo instanceof DocumentBaseInfo) return (DocumentBaseInfo) oldBaseInfo;
        DocumentBaseInfo baseInfo = new DocumentBaseInfo();
        baseInfo.init(oldBaseInfo);
        baseInfo.type = type;
        return baseInfo;
    }
}
