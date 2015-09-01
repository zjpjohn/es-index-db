package com.wxingyl.es.jdal;

import com.wxingyl.es.util.CommonUtils;
import org.apache.commons.dbutils.ResultSetHandler;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.*;

/**
 * Created by xing on 15/8/26.
 * filter some field
 * In this class, most code copy from {@link org.apache.commons.dbutils.BasicRowProcessor#toMap(ResultSet)} and
 * {@link org.apache.commons.dbutils.BasicRowProcessor.CaseInsensitiveHashMap}
 */
public class FilterMapListHandler implements ResultSetHandler<List<Map<String, Object>>> {

    private Set<String> filterFields;

    public FilterMapListHandler(Collection<String> fields) {
        if (!CommonUtils.isEmpty(fields)) {
            filterFields = new HashSet<>(fields);
        }
    }

    protected Map<String, Object> handleRow(ResultSet rs) throws SQLException {
        Map<String, Object> result = new HashMap<>();
        ResultSetMetaData rsmd = rs.getMetaData();
        int cols = rsmd.getColumnCount();

        for (int i = 1; i <= cols; i++) {
            String columnName = rsmd.getColumnLabel(i);
            if (null == columnName || 0 == columnName.length()) {
                columnName = rsmd.getColumnName(i);
            }
            columnName = columnName.toLowerCase();
            if (filterFields != null && filterFields.contains(columnName)) continue;
            result.put(columnName, rs.getObject(i));
        }

        return result;
    }

    @Override
    public List<Map<String, Object>> handle(ResultSet rs) throws SQLException {
        List<Map<String, Object>> rows = new LinkedList<>();
        while (rs.next()) {
            rows.add(handleRow(rs));
        }
        return rows;
    }
}
