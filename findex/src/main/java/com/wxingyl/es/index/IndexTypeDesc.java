package com.wxingyl.es.index;

/**
 * Created by xing on 15/9/2.
 * index type desc
 */
public class IndexTypeDesc {

    private String index;

    private String type;

    public IndexTypeDesc(String index, String type) {
        this.index = index;
        this.type = type;
    }

    public String getIndex() {
        return index;
    }

    public String getType() {
        return type;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof IndexTypeDesc)) return false;

        IndexTypeDesc that = (IndexTypeDesc) o;

        if (!index.equals(that.index)) return false;
        return type.equals(that.type);

    }

    @Override
    public int hashCode() {
        int result = index.hashCode();
        result = 31 * result + type.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "IndexTypeDesc{" +
                "index='" + index + '\'' +
                ", type='" + type + '\'' +
                '}';
    }
}
