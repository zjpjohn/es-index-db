package com.wxingyl.es.conf.ds;

import com.wxingyl.es.jdal.handle.SqlQueryHandle;

/**
 * Created by xing on 15/8/17.
 * urlAddress can't null
 * 数据源配置解析后的信息
 */
public class DataSourceBean {
    /**
     * db服务器地址, urlAddress + schema才能判断两个数据库实例是否相同
     */
    private String urlAddress;
    /**
     * 数据库名
     */
    private String schema;

    private SqlQueryHandle queryHandle;

    private DataSourceBean() {
    }

    public String getUrlAddress() {
        return urlAddress;
    }

    public String getSchema() {
        return schema;
    }

    public SqlQueryHandle getQueryHandle() {
        return queryHandle;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof DataSourceBean)) return false;

        DataSourceBean that = (DataSourceBean) o;

        if (urlAddress != null ? !urlAddress.equals(that.urlAddress) : that.urlAddress != null) return false;
        return !(schema != null ? !schema.equals(that.schema) : that.schema != null);

    }

    @Override
    public int hashCode() {
        int result = urlAddress != null ? urlAddress.hashCode() : 0;
        result = 31 * result + (schema != null ? schema.hashCode() : 0);
        return result;
    }

    public static Build build(String urlAddress, String schema) {
        return new Build(urlAddress, schema);
    }

    public static class Build {

        private String urlAddress, schema;

        private SqlQueryHandle queryHandle;

        public Build(String urlAddress, String schema) {
            this.urlAddress = urlAddress;
            this.schema = schema;
        }

        public Build queryHandle(SqlQueryHandle queryHandle) {
            this.queryHandle = queryHandle;
            return this;
        }

        public DataSourceBean build() {
            DataSourceBean ret = new DataSourceBean();
            ret.urlAddress = urlAddress;
            ret.schema = schema;
            ret.queryHandle = queryHandle;
            return ret;
        }
    }

}
