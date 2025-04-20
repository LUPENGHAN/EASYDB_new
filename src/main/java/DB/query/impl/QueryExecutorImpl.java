package DB.query.impl;

import DB.index.interfaces.IndexManager;
import DB.page.interfaces.PageManager;
import DB.query.interfaces.QueryExecutor;
import DB.query.interfaces.QueryOptimizer;
import DB.query.interfaces.PreparedQuery;
import DB.record.models.Record;
import DB.table.interfaces.TableManager;
import DB.transaction.interfaces.TransactionManager;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 查询执行器实现
 */
@Slf4j
@RequiredArgsConstructor
public class QueryExecutorImpl implements QueryExecutor {
    private final TableManager tableManager;
    private final IndexManager indexManager;
    private final PageManager pageManager;
    private final TransactionManager transactionManager;
    private final QueryOptimizer queryOptimizer;
    private final Map<String, PreparedQueryImpl> preparedQueries = new ConcurrentHashMap<>();

    @Override
    public List<Record> executeQuery(String sql) {
        // 获取查询计划
        QueryOptimizer.QueryPlan plan = queryOptimizer.optimizeQuery(sql, indexManager, transactionManager);
        
        // 执行查询计划
        return queryOptimizer.executeQuery(plan);
    }

    @Override
    public List<Record> executeSelect(String tableName, String condition, List<String> selectedColumns) {
        // 构建SQL
        StringBuilder sql = new StringBuilder("SELECT ");
        if (selectedColumns == null || selectedColumns.isEmpty()) {
            sql.append("*");
        } else {
            sql.append(String.join(", ", selectedColumns));
        }
        sql.append(" FROM ").append(tableName);
        
        if (condition != null && !condition.isEmpty()) {
            sql.append(" WHERE ").append(condition);
        }
        
        // 执行查询
        return executeQuery(sql.toString());
    }

    @Override
    public int executeInsert(String tableName, List<String> columns, List<Object> values) {
        // 验证列和值的数量是否匹配
        if (columns.size() != values.size()) {
            throw new IllegalArgumentException("Columns and values count mismatch");
        }
        
        // 构建SQL
        StringBuilder sql = new StringBuilder("INSERT INTO ");
        sql.append(tableName).append(" (");
        sql.append(String.join(", ", columns));
        sql.append(") VALUES (");
        
        List<String> formattedValues = new ArrayList<>();
        for (Object value : values) {
            if (value == null) {
                formattedValues.add("NULL");
            } else if (value instanceof String) {
                formattedValues.add("'" + value + "'");
            } else {
                formattedValues.add(value.toString());
            }
        }
        
        sql.append(String.join(", ", formattedValues));
        sql.append(")");
        
        // 执行查询（INSERT返回空记录列表）
        executeQuery(sql.toString());
        
        // 这里应该统计实际影响的行数，但现在简化处理
        return 1;
    }

    @Override
    public int executeUpdate(String tableName, Map<String, Object> setValues, String condition) {
        if (setValues == null || setValues.isEmpty()) {
            throw new IllegalArgumentException("No values to update");
        }
        
        // 构建SQL
        StringBuilder sql = new StringBuilder("UPDATE ");
        sql.append(tableName).append(" SET ");
        
        List<String> setParts = new ArrayList<>();
        for (Map.Entry<String, Object> entry : setValues.entrySet()) {
            String key = entry.getKey();
            Object value = entry.getValue();
            
            if (value == null) {
                setParts.add(key + " = NULL");
            } else if (value instanceof String) {
                setParts.add(key + " = '" + value + "'");
            } else {
                setParts.add(key + " = " + value);
            }
        }
        
        sql.append(String.join(", ", setParts));
        
        if (condition != null && !condition.isEmpty()) {
            sql.append(" WHERE ").append(condition);
        }
        
        // 执行查询（UPDATE返回空记录列表）
        executeQuery(sql.toString());
        
        // 这里应该统计实际影响的行数，但现在简化处理
        return 1;
    }

    @Override
    public int executeDelete(String tableName, String condition) {
        // 构建SQL
        StringBuilder sql = new StringBuilder("DELETE FROM ");
        sql.append(tableName);
        
        if (condition != null && !condition.isEmpty()) {
            sql.append(" WHERE ").append(condition);
        }
        
        // 执行查询（DELETE返回空记录列表）
        executeQuery(sql.toString());
        
        // 这里应该统计实际影响的行数，但现在简化处理
        return 1;
    }

    @Override
    public PreparedQuery prepare(String sql) {
        // 如果已经有预编译查询，直接返回
        if (preparedQueries.containsKey(sql)) {
            return preparedQueries.get(sql);
        }
        
        // 创建新的预编译查询
        PreparedQueryImpl query = new PreparedQueryImpl(sql, queryOptimizer, indexManager);
        preparedQueries.put(sql, query);
        return query;
    }

    private String generateQueryId(String query) {
        return String.valueOf(query.hashCode());
    }

    /**
     * 预编译查询实现
     */
    private static class PreparedQueryImpl implements PreparedQuery {
        private final String query;
        private final QueryOptimizer queryOptimizer;
        private final IndexManager indexManager;
        private final List<Object> parameters = new ArrayList<>();
        private boolean closed = false;

        public PreparedQueryImpl(String query, QueryOptimizer queryOptimizer, IndexManager indexManager) {
            this.query = query;
            this.queryOptimizer = queryOptimizer;
            this.indexManager = indexManager;
        }

        @Override
        public List<Record> execute(Object[] params, TransactionManager transactionManager) {
            if (closed) {
                throw new IllegalStateException("PreparedQuery is closed");
            }
            
            // 根据参数替换占位符
            String finalQuery = replaceParametersArray(query, params);
            return queryOptimizer.executeQuery(
                queryOptimizer.optimizeQuery(finalQuery, indexManager, transactionManager)
            );
        }

        @Override
        public int executeUpdate(Object[] params, TransactionManager transactionManager) {
            if (closed) {
                throw new IllegalStateException("PreparedQuery is closed");
            }
            
            // 根据参数替换占位符
            String finalQuery = replaceParametersArray(query, params);
            List<Record> records = queryOptimizer.executeQuery(
                queryOptimizer.optimizeQuery(finalQuery, indexManager, transactionManager)
            );
            return records.size(); // 简化处理，返回影响的记录数
        }
        
        @Override
        public void close() {
            closed = true;
            parameters.clear();
        }

        // 旧方法，保留以兼容现有代码
        public void setParameter(int index, Object value) {
            while (parameters.size() <= index) {
                parameters.add(null);
            }
            parameters.set(index, value);
        }

        // 旧方法，保留以兼容现有代码
        public List<Record> execute(TransactionManager transactionManager) {
            if (closed) {
                throw new IllegalStateException("PreparedQuery is closed");
            }
            
            String finalQuery = replaceParameters(query);
            return queryOptimizer.executeQuery(
                queryOptimizer.optimizeQuery(finalQuery, indexManager, transactionManager)
            );
        }

        // 旧方法，保留以兼容现有代码
        public int executeUpdate(TransactionManager transactionManager) {
            if (closed) {
                throw new IllegalStateException("PreparedQuery is closed");
            }
            
            String finalQuery = replaceParameters(query);
            List<Record> records = queryOptimizer.executeQuery(
                queryOptimizer.optimizeQuery(finalQuery, indexManager, transactionManager)
            );
            return records.size();
        }

        private String replaceParameters(String query) {
            String result = query;
            for (int i = 0; i < parameters.size(); i++) {
                Object value = parameters.get(i);
                String placeholder = "?";
                String replacement = formatValue(value);
                result = result.replaceFirst("\\?", replacement);
            }
            return result;
        }
        
        private String replaceParametersArray(String query, Object[] params) {
            if (params == null || params.length == 0) {
                return query;
            }
            
            String result = query;
            for (Object param : params) {
                String replacement = formatValue(param);
                result = result.replaceFirst("\\?", replacement);
            }
            return result;
        }
        
        private String formatValue(Object value) {
            if (value == null) {
                return "NULL";
            } else if (value instanceof String) {
                return "'" + ((String) value).replace("'", "''") + "'";
            } else if (value instanceof Number || value instanceof Boolean) {
                return value.toString();
            } else {
                return "'" + value.toString().replace("'", "''") + "'";
            }
        }
    }
} 