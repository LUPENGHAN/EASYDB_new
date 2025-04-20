package DB.query.impl;

import DB.index.interfaces.IndexManager;
import DB.query.interfaces.QueryOptimizer;
import DB.record.models.Record;
import DB.transaction.interfaces.TransactionManager;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 查询优化器实现
 */
@Slf4j
public class QueryOptimizerImpl implements QueryOptimizer {
    private static final Pattern SELECT_PATTERN = Pattern.compile(
            "SELECT\\s+(.*?)\\s+FROM\\s+(\\w+)(?:\\s+WHERE\\s+(.*?))?",
            Pattern.CASE_INSENSITIVE
    );

    private static final Pattern INSERT_PATTERN = Pattern.compile(
            "INSERT\\s+INTO\\s+(\\w+)\\s*\\((.*?)\\)\\s*VALUES\\s*\\((.*?)\\)",
            Pattern.CASE_INSENSITIVE
    );

    private static final Pattern UPDATE_PATTERN = Pattern.compile(
            "UPDATE\\s+(\\w+)\\s+SET\\s+(.*?)(?:\\s+WHERE\\s+(.*?))?",
            Pattern.CASE_INSENSITIVE
    );

    private static final Pattern DELETE_PATTERN = Pattern.compile(
            "DELETE\\s+FROM\\s+(\\w+)(?:\\s+WHERE\\s+(.*?))?",
            Pattern.CASE_INSENSITIVE
    );

    private final IndexManager indexManager;
    private final TransactionManager transactionManager;

    public QueryOptimizerImpl(IndexManager indexManager, TransactionManager transactionManager) {
        this.indexManager = indexManager;
        this.transactionManager = transactionManager;
    }

    @Override
    public QueryPlan optimizeQuery(String query, IndexManager indexManager, TransactionManager transactionManager) {
        // 解析查询类型
        QueryType queryType = determineQueryType(query);
        
        // 根据查询类型创建查询计划
        switch (queryType) {
            case SELECT:
                return optimizeSelectQuery(query, indexManager);
            case INSERT:
                return optimizeInsertQuery(query);
            case UPDATE:
                return optimizeUpdateQuery(query, indexManager);
            case DELETE:
                return optimizeDeleteQuery(query, indexManager);
            default:
                throw new IllegalArgumentException("Unsupported query type: " + queryType);
        }
    }

    @Override
    public List<Record> executeQuery(QueryPlan plan) {
        List<Record> result = new ArrayList<>();
        long xid = plan.getTransactionManager().beginTransaction();

        try {
            switch (plan.getQueryType()) {
                case SELECT:
                    result = executeSelectQuery(plan);
                    break;
                case INSERT:
                    executeInsertQuery(plan);
                    break;
                case UPDATE:
                    executeUpdateQuery(plan);
                    break;
                case DELETE:
                    executeDeleteQuery(plan);
                    break;
                default:
                    throw new IllegalArgumentException("Unsupported query type: " + plan.getQueryType());
            }

            plan.getTransactionManager().commit(xid);
            return result;
        } catch (Exception e) {
            plan.getTransactionManager().rollback(xid);
            throw new RuntimeException("Failed to execute query", e);
        }
    }

    private List<Record> executeSelectQuery(QueryPlan plan) {
        List<Record> result = new ArrayList<>();
        String indexName = plan.getIndexName();
        String condition = plan.getCondition();

        if (indexName != null && !indexName.isEmpty()) {
            // 使用索引查询
            Object key = parseKeyFromCondition(condition);
            result = plan.getIndexManager().exactQuery(indexName, key, plan.getTransactionManager());
        } else {
            // 全表扫描
            // TODO: 实现全表扫描逻辑
        }

        // 过滤选择的列
        return filterColumns(result, plan.getSelectedColumns());
    }

    private void executeInsertQuery(QueryPlan plan) {
        // TODO: 实现插入逻辑
    }

    private void executeUpdateQuery(QueryPlan plan) {
        // TODO: 实现更新逻辑
    }

    private void executeDeleteQuery(QueryPlan plan) {
        // TODO: 实现删除逻辑
    }

    private List<Record> filterColumns(List<Record> records, List<String> selectedColumns) {
        if (selectedColumns.isEmpty() || selectedColumns.contains("*")) {
            return records;
        }

        List<Record> filtered = new ArrayList<>();
        for (Record record : records) {
            Record filteredRecord = new Record();
            for (String column : selectedColumns) {
                Object value = record.getField(column);
                filteredRecord.setField(column, value);
            }
            filtered.add(filteredRecord);
        }
        return filtered;
    }

    private Object parseKeyFromCondition(String condition) {
        if (condition == null || condition.isEmpty()) {
            return null;
        }

        // 简单实现：假设条件是 "column = value" 的形式
        String[] parts = condition.split("=");
        if (parts.length != 2) {
            return null;
        }

        String value = parts[1].trim();
        // 移除可能的引号
        if (value.startsWith("'") && value.endsWith("'")) {
            value = value.substring(1, value.length() - 1);
        }

        // 尝试转换为适当的类型
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            try {
                return Double.parseDouble(value);
            } catch (NumberFormatException e2) {
                return value;
            }
        }
    }

    private String findApplicableIndex(String tableName, String condition, IndexManager indexManager) {
        if (condition == null || condition.isEmpty()) {
            return null;
        }

        // 简单实现：假设条件是 "column = value" 的形式
        String[] parts = condition.split("=");
        if (parts.length != 2) {
            return null;
        }

        String column = parts[0].trim();
        String indexName = tableName + "_" + column + "_idx";

        // 检查索引是否存在
        Page indexRoot = indexManager.getIndexRoot(indexName);
        if (indexRoot != null) {
            return indexName;
        }

        return null;
    }

    private QueryType determineQueryType(String query) {
        String normalizedQuery = query.trim().toUpperCase();
        if (normalizedQuery.startsWith("SELECT")) {
            return QueryType.SELECT;
        } else if (normalizedQuery.startsWith("INSERT")) {
            return QueryType.INSERT;
        } else if (normalizedQuery.startsWith("UPDATE")) {
            return QueryType.UPDATE;
        } else if (normalizedQuery.startsWith("DELETE")) {
            return QueryType.DELETE;
        } else {
            throw new IllegalArgumentException("Unsupported query: " + query);
        }
    }

    private QueryPlan optimizeSelectQuery(String query, IndexManager indexManager) {
        Matcher matcher = SELECT_PATTERN.matcher(query);
        if (!matcher.find()) {
            throw new IllegalArgumentException("Invalid SELECT query: " + query);
        }

        String columns = matcher.group(1).trim();
        String tableName = matcher.group(2).trim();
        String condition = matcher.group(3) != null ? matcher.group(3).trim() : null;

        // 解析选择的列
        List<String> selectedColumns = parseColumns(columns);

        // 检查是否有可用的索引
        String indexName = findApplicableIndex(tableName, condition, indexManager);

        return new QueryPlanImpl(QueryType.SELECT, tableName, condition, selectedColumns, indexName);
    }

    private QueryPlan optimizeInsertQuery(String query) {
        Matcher matcher = INSERT_PATTERN.matcher(query);
        if (!matcher.find()) {
            throw new IllegalArgumentException("Invalid INSERT query: " + query);
        }

        String tableName = matcher.group(1).trim();
        String columns = matcher.group(2).trim();
        String values = matcher.group(3).trim();

        return new QueryPlanImpl(QueryType.INSERT, tableName, values, parseColumns(columns), null);
    }

    private QueryPlan optimizeUpdateQuery(String query, IndexManager indexManager) {
        Matcher matcher = UPDATE_PATTERN.matcher(query);
        if (!matcher.find()) {
            throw new IllegalArgumentException("Invalid UPDATE query: " + query);
        }

        String tableName = matcher.group(1).trim();
        String setClause = matcher.group(2).trim();
        String condition = matcher.group(3) != null ? matcher.group(3).trim() : null;

        // 检查是否有可用的索引
        String indexName = findApplicableIndex(tableName, condition, indexManager);

        return new QueryPlanImpl(QueryType.UPDATE, tableName, condition, parseColumns(setClause), indexName);
    }

    private QueryPlan optimizeDeleteQuery(String query, IndexManager indexManager) {
        Matcher matcher = DELETE_PATTERN.matcher(query);
        if (!matcher.find()) {
            throw new IllegalArgumentException("Invalid DELETE query: " + query);
        }

        String tableName = matcher.group(1).trim();
        String condition = matcher.group(2) != null ? matcher.group(2).trim() : null;

        // 检查是否有可用的索引
        String indexName = findApplicableIndex(tableName, condition, indexManager);

        return new QueryPlanImpl(QueryType.DELETE, tableName, condition, new ArrayList<>(), indexName);
    }

    private List<String> parseColumns(String columns) {
        List<String> result = new ArrayList<>();
        String[] parts = columns.split(",");
        for (String part : parts) {
            result.add(part.trim());
        }
        return result;
    }

    /**
     * 查询计划实现
     */
    private static class QueryPlanImpl implements QueryPlan {
        private final QueryType queryType;
        private final String tableName;
        private final String condition;
        private final List<String> selectedColumns;
        private final String indexName;
        private final TransactionManager transactionManager;

        public QueryPlanImpl(QueryType queryType, String tableName, String condition,
                           List<String> selectedColumns, String indexName) {
            this.queryType = queryType;
            this.tableName = tableName;
            this.condition = condition;
            this.selectedColumns = selectedColumns;
            this.indexName = indexName;
            this.transactionManager = null; // Assuming transactionManager is not available in the constructor
        }

        @Override
        public QueryType getQueryType() {
            return queryType;
        }

        @Override
        public String getTableName() {
            return tableName;
        }

        @Override
        public String getCondition() {
            return condition;
        }

        @Override
        public List<String> getSelectedColumns() {
            return selectedColumns;
        }

        @Override
        public String getIndexName() {
            return indexName;
        }

        @Override
        public TransactionManager getTransactionManager() {
            return transactionManager;
        }
    }
} 