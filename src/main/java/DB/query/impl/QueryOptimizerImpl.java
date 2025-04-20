package DB.query.impl;

import DB.index.interfaces.IndexManager;
import DB.page.models.Page;
import DB.query.interfaces.QueryOptimizer;
import DB.record.models.Record;
import DB.table.interfaces.TableManager;
import DB.transaction.interfaces.TransactionManager;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
    private final TableManager tableManager;

    public QueryOptimizerImpl(IndexManager indexManager, TransactionManager transactionManager, TableManager tableManager) {
        this.indexManager = indexManager;
        this.transactionManager = transactionManager;
        this.tableManager = tableManager;
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

            plan.getTransactionManager().commitTransaction(xid);
            return result;
        } catch (Exception e) {
            plan.getTransactionManager().rollbackTransaction(xid);
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
            // 获取表的所有页面
            // TODO: 实现通过TableManager获取表的所有页面
            // 然后遍历所有记录，应用过滤条件
        }

        // 过滤选择的列
        return filterColumns(result, plan.getSelectedColumns());
    }

    private void executeInsertQuery(QueryPlan plan) {
        // 解析INSERT语句的值
        String[] columnNames = parseInsertColumns(plan.getCondition());
        Object[] values = parseInsertValues(plan.getSelectedColumns().get(0)); // 在INSERT中，condition存储了列名，selectedColumns[0]存储了值
        
        // 创建新记录
        Record record = new Record();
        for (int i = 0; i < columnNames.length; i++) {
            record.setFieldValue(columnNames[i], values[i]);
        }
        
        // 插入记录
        // TODO: 实现通过TableManager插入记录
    }

    private void executeUpdateQuery(QueryPlan plan) {
        // 解析UPDATE语句的SET子句
        Map<String, Object> updateValues = parseUpdateSet(plan.getSelectedColumns().get(0)); // 在UPDATE中，selectedColumns[0]存储了SET子句
        
        // 获取符合条件的记录
        List<Record> records = new ArrayList<>();
        if (plan.getIndexName() != null && !plan.getIndexName().isEmpty()) {
            // 使用索引
            Object key = parseKeyFromCondition(plan.getCondition());
            records = plan.getIndexManager().exactQuery(plan.getIndexName(), key, plan.getTransactionManager());
        } else {
            // 全表扫描
            // TODO: 实现通过TableManager获取表的所有记录，并应用过滤条件
        }
        
        // 更新记录
        for (Record record : records) {
            for (Map.Entry<String, Object> entry : updateValues.entrySet()) {
                record.setFieldValue(entry.getKey(), entry.getValue());
            }
            // TODO: 实现通过TableManager更新记录
        }
    }

    private void executeDeleteQuery(QueryPlan plan) {
        // 获取符合条件的记录
        List<Record> records = new ArrayList<>();
        if (plan.getIndexName() != null && !plan.getIndexName().isEmpty()) {
            // 使用索引
            Object key = parseKeyFromCondition(plan.getCondition());
            records = plan.getIndexManager().exactQuery(plan.getIndexName(), key, plan.getTransactionManager());
        } else {
            // 全表扫描
            // TODO: 实现通过TableManager获取表的所有记录，并应用过滤条件
        }
        
        // 删除记录
        for (Record record : records) {
            // TODO: 实现通过TableManager删除记录
        }
    }

    private List<Record> filterColumns(List<Record> records, List<String> selectedColumns) {
        if (selectedColumns.isEmpty() || selectedColumns.contains("*")) {
            return records;
        }

        List<Record> filtered = new ArrayList<>();
        for (Record record : records) {
            Record filteredRecord = new Record();
            for (String column : selectedColumns) {
                Object value = record.getFieldValue(column);
                filteredRecord.setFieldValue(column, value);
            }
            filtered.add(filteredRecord);
        }
        return filtered;
    }

    private String[] parseInsertColumns(String columnsString) {
        return columnsString.split(",\\s*");
    }

    private Object[] parseInsertValues(String valuesString) {
        String[] valueParts = valuesString.split(",\\s*");
        Object[] values = new Object[valueParts.length];
        
        for (int i = 0; i < valueParts.length; i++) {
            String value = valueParts[i].trim();
            if (value.startsWith("'") && value.endsWith("'")) {
                // 字符串值
                values[i] = value.substring(1, value.length() - 1);
            } else if (value.matches("-?\\d+")) {
                // 整数值
                values[i] = Integer.parseInt(value);
            } else if (value.matches("-?\\d+\\.\\d+")) {
                // 浮点值
                values[i] = Double.parseDouble(value);
            } else if (value.equalsIgnoreCase("true") || value.equalsIgnoreCase("false")) {
                // 布尔值
                values[i] = Boolean.parseBoolean(value);
            } else if (value.equalsIgnoreCase("null")) {
                // NULL值
                values[i] = null;
            } else {
                // 默认为字符串
                values[i] = value;
            }
        }
        
        return values;
    }

    private Map<String, Object> parseUpdateSet(String setClause) {
        Map<String, Object> updateValues = new HashMap<>();
        String[] setParts = setClause.split(",\\s*");
        
        for (String setPart : setParts) {
            String[] keyValue = setPart.split("=", 2);
            String key = keyValue[0].trim();
            String valueStr = keyValue[1].trim();
            
            Object value;
            if (valueStr.startsWith("'") && valueStr.endsWith("'")) {
                // 字符串值
                value = valueStr.substring(1, valueStr.length() - 1);
            } else if (valueStr.matches("-?\\d+")) {
                // 整数值
                value = Integer.parseInt(valueStr);
            } else if (valueStr.matches("-?\\d+\\.\\d+")) {
                // 浮点值
                value = Double.parseDouble(valueStr);
            } else if (valueStr.equalsIgnoreCase("true") || valueStr.equalsIgnoreCase("false")) {
                // 布尔值
                value = Boolean.parseBoolean(valueStr);
            } else if (valueStr.equalsIgnoreCase("null")) {
                // NULL值
                value = null;
            } else {
                // 默认为字符串
                value = valueStr;
            }
            
            updateValues.put(key, value);
        }
        
        return updateValues;
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

        return new QueryPlanImpl(QueryType.SELECT, tableName, condition, selectedColumns, indexName, indexManager, transactionManager);
    }

    private QueryPlan optimizeInsertQuery(String query) {
        Matcher matcher = INSERT_PATTERN.matcher(query);
        if (!matcher.find()) {
            throw new IllegalArgumentException("Invalid INSERT query: " + query);
        }

        String tableName = matcher.group(1).trim();
        String columns = matcher.group(2).trim();
        String values = matcher.group(3).trim();

        List<String> valuesList = new ArrayList<>();
        valuesList.add(values); // 在INSERT中，condition存储了列名，selectedColumns[0]存储了值

        return new QueryPlanImpl(QueryType.INSERT, tableName, columns, valuesList, null, indexManager, transactionManager);
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

        List<String> setClauseList = new ArrayList<>();
        setClauseList.add(setClause); // 在UPDATE中，selectedColumns[0]存储了SET子句

        return new QueryPlanImpl(QueryType.UPDATE, tableName, condition, setClauseList, indexName, indexManager, transactionManager);
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

        return new QueryPlanImpl(QueryType.DELETE, tableName, condition, new ArrayList<>(), indexName, indexManager, transactionManager);
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
        private final IndexManager indexManager;
        private final TransactionManager transactionManager;

        public QueryPlanImpl(QueryType queryType, String tableName, String condition,
                           List<String> selectedColumns, String indexName,
                           IndexManager indexManager, TransactionManager transactionManager) {
            this.queryType = queryType;
            this.tableName = tableName;
            this.condition = condition;
            this.selectedColumns = selectedColumns;
            this.indexName = indexName;
            this.indexManager = indexManager;
            this.transactionManager = transactionManager;
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
        public IndexManager getIndexManager() {
            return indexManager;
        }

        @Override
        public TransactionManager getTransactionManager() {
            return transactionManager;
        }
    }

    /**
     * 解析条件表达式
     * @param condition 条件表达式
     * @return 解析后的条件对象
     */
    private Condition parseCondition(String condition) {
        if (condition == null || condition.trim().isEmpty()) {
            return null;
        }
        
        // 解析AND条件
        if (condition.toUpperCase().contains(" AND ")) {
            String[] parts = condition.split("(?i)\\s+AND\\s+");
            List<Condition> subConditions = new ArrayList<>();
            for (String part : parts) {
                subConditions.add(parseCondition(part));
            }
            return new AndCondition(subConditions);
        }
        
        // 解析OR条件
        if (condition.toUpperCase().contains(" OR ")) {
            String[] parts = condition.split("(?i)\\s+OR\\s+");
            List<Condition> subConditions = new ArrayList<>();
            for (String part : parts) {
                subConditions.add(parseCondition(part));
            }
            return new OrCondition(subConditions);
        }
        
        // 解析基本条件 (column operator value)
        String[] operators = {"=", "<>", "!=", ">", "<", ">=", "<=", "LIKE", "IN", "IS NULL", "IS NOT NULL"};
        for (String op : operators) {
            if (condition.toUpperCase().contains(op)) {
                String[] parts = condition.split(op, 2);
                if (parts.length == 2) {
                    String column = parts[0].trim();
                    String value = parts[1].trim();
                    return new SimpleCondition(column, op, value);
                }
            }
        }
        
        // 无法解析的条件
        return new RawCondition(condition);
    }
    
    /**
     * 条件接口
     */
    private interface Condition {
        boolean evaluate(Record record);
        String getConditionString();
    }
    
    /**
     * 原始条件
     */
    private static class RawCondition implements Condition {
        private final String conditionString;
        
        public RawCondition(String conditionString) {
            this.conditionString = conditionString;
        }
        
        @Override
        public boolean evaluate(Record record) {
            // 原始条件无法直接评估，总是返回true
            return true;
        }
        
        @Override
        public String getConditionString() {
            return conditionString;
        }
    }
    
    /**
     * 简单条件 (列 操作符 值)
     */
    private static class SimpleCondition implements Condition {
        private final String column;
        private final String operator;
        private final String value;
        
        public SimpleCondition(String column, String operator, String value) {
            this.column = column;
            this.operator = operator;
            this.value = value;
        }
        
        @Override
        public boolean evaluate(Record record) {
            Object fieldValue = record.getFieldValue(column);
            
            // 处理NULL值
            if ("IS NULL".equalsIgnoreCase(operator)) {
                return fieldValue == null;
            }
            if ("IS NOT NULL".equalsIgnoreCase(operator)) {
                return fieldValue != null;
            }
            
            // 如果字段值为null，其他操作都返回false
            if (fieldValue == null) {
                return false;
            }
            
            // 根据操作符类型评估条件
            switch (operator.toUpperCase()) {
                case "=":
                    return String.valueOf(fieldValue).equals(value);
                case "<>":
                case "!=":
                    return !String.valueOf(fieldValue).equals(value);
                case ">":
                    if (fieldValue instanceof Number && isNumeric(value)) {
                        return ((Number) fieldValue).doubleValue() > Double.parseDouble(value);
                    }
                    return String.valueOf(fieldValue).compareTo(value) > 0;
                case "<":
                    if (fieldValue instanceof Number && isNumeric(value)) {
                        return ((Number) fieldValue).doubleValue() < Double.parseDouble(value);
                    }
                    return String.valueOf(fieldValue).compareTo(value) < 0;
                case ">=":
                    if (fieldValue instanceof Number && isNumeric(value)) {
                        return ((Number) fieldValue).doubleValue() >= Double.parseDouble(value);
                    }
                    return String.valueOf(fieldValue).compareTo(value) >= 0;
                case "<=":
                    if (fieldValue instanceof Number && isNumeric(value)) {
                        return ((Number) fieldValue).doubleValue() <= Double.parseDouble(value);
                    }
                    return String.valueOf(fieldValue).compareTo(value) <= 0;
                case "LIKE":
                    // 简单的LIKE实现，只支持%作为通配符
                    String pattern = value.replace("%", ".*");
                    return String.valueOf(fieldValue).matches(pattern);
                case "IN":
                    // 假设IN的值格式为 (value1, value2, ...)
                    String inValues = value.substring(1, value.length() - 1); // 去除括号
                    String[] values = inValues.split(",");
                    for (String val : values) {
                        if (String.valueOf(fieldValue).equals(val.trim())) {
                            return true;
                        }
                    }
                    return false;
                default:
                    return false;
            }
        }
        
        private boolean isNumeric(String str) {
            try {
                Double.parseDouble(str);
                return true;
            } catch (NumberFormatException e) {
                return false;
            }
        }
        
        @Override
        public String getConditionString() {
            return column + " " + operator + " " + value;
        }
    }
    
    /**
     * AND条件
     */
    private static class AndCondition implements Condition {
        private final List<Condition> conditions;
        
        public AndCondition(List<Condition> conditions) {
            this.conditions = conditions;
        }
        
        @Override
        public boolean evaluate(Record record) {
            for (Condition condition : conditions) {
                if (!condition.evaluate(record)) {
                    return false;
                }
            }
            return true;
        }
        
        @Override
        public String getConditionString() {
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < conditions.size(); i++) {
                if (i > 0) {
                    sb.append(" AND ");
                }
                sb.append(conditions.get(i).getConditionString());
            }
            return sb.toString();
        }
    }
    
    /**
     * OR条件
     */
    private static class OrCondition implements Condition {
        private final List<Condition> conditions;
        
        public OrCondition(List<Condition> conditions) {
            this.conditions = conditions;
        }
        
        @Override
        public boolean evaluate(Record record) {
            for (Condition condition : conditions) {
                if (condition.evaluate(record)) {
                    return true;
                }
            }
            return false;
        }
        
        @Override
        public String getConditionString() {
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < conditions.size(); i++) {
                if (i > 0) {
                    sb.append(" OR ");
                }
                sb.append(conditions.get(i).getConditionString());
            }
            return sb.toString();
        }
    }
} 