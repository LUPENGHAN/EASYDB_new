package DB.query.impl;

import DB.index.interfaces.IndexManager;
import DB.page.interfaces.PageManager;
import DB.page.models.Page;
import DB.query.interfaces.QueryComponents.*;
import DB.record.impl.RecordManagerImpl;
import DB.record.interfaces.RecordManager;
import DB.record.models.Record;
import DB.table.interfaces.TableManager;
import DB.table.models.Column;
import DB.table.models.Table;
import DB.transaction.interfaces.TransactionManager;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.lang.reflect.Field;

/**
 * 查询核心实现
 * 整合了查询优化器和查询执行器的功能
 */
@Slf4j
public class QueryCore implements ExtendedQueryExecutor, QueryOptimizer {
    private final TableManager tableManager;
    private final IndexManager indexManager;
    private final RecordManager recordManager;
    final QueryParser queryParser;
    final TransactionManager transactionManager;

    /**
     * 构造函数
     * @param tableManager 表管理器
     * @param indexManager 索引管理器
     * @param queryParser 查询解析器
     * @param transactionManager 事务管理器
     */
    public QueryCore(TableManager tableManager, IndexManager indexManager, 
                    QueryParser queryParser, TransactionManager transactionManager) {
        this.tableManager = tableManager;
        this.indexManager = indexManager;
        this.queryParser = queryParser;
        this.transactionManager = transactionManager;
        this.recordManager = new RecordManagerImpl(tableManager.getPageManager());
    }

    /**
     * 构造函数
     * @param tableManager 表管理器
     * @param indexManager 索引管理器
     * @param recordManager 记录管理器
     * @param queryParser 查询解析器
     * @param transactionManager 事务管理器
     */
    public QueryCore(TableManager tableManager, IndexManager indexManager, RecordManager recordManager,
                    QueryParser queryParser, TransactionManager transactionManager) {
        this.tableManager = tableManager;
        this.indexManager = indexManager;
        this.recordManager = recordManager;
        this.queryParser = queryParser;
        this.transactionManager = transactionManager;
    }

    //----------- 查询执行器接口实现 -----------

    @Override
    public List<Record> executeQuery(String sql) {
        // 获取查询计划
        QueryPlan plan = optimizeQuery(sql, indexManager, transactionManager);

        // 执行查询计划
        return executeQuery(plan);
    }

    @Override
    public int executeUpdate(String sql) {
        // 获取查询计划
        QueryPlan plan = optimizeQuery(sql, indexManager, transactionManager);

        // 执行查询计划
        List<Record> records = executeQuery(plan);

        // 根据查询类型返回影响的行数
        return records != null ? records.size() : 0;
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
                    throw new IllegalArgumentException("不支持的查询类型: " + plan.getQueryType());
            }

            plan.getTransactionManager().commitTransaction(xid);
            return result;
        } catch (Exception e) {
            plan.getTransactionManager().rollbackTransaction(xid);
            throw new RuntimeException("执行查询失败", e);
        }
    }

    @Override
    public List<Record> executeSelectQuery(QueryPlan plan) {
        List<Record> result = new ArrayList<>();
        String indexName = plan.getIndexName();
        String condition = plan.getCondition();
        String tableName = plan.getTableName();

        if (indexName != null && !indexName.isEmpty()) {
            // 使用索引查询
            QueryParser.ConditionData conditionData = queryParser.parseCondition(condition);
            Object key = parseValueFromCondition(conditionData);
            result = plan.getIndexManager().exactQuery(indexName, key, plan.getTransactionManager());
        } else {
            // 全表扫描
            if (tableManager instanceof DB.table.impl.TableManagerImpl) {
                // 使用TableManagerImpl的findRecords方法
                DB.table.impl.TableManagerImpl tableManagerImpl = (DB.table.impl.TableManagerImpl) tableManager;
                result = tableManagerImpl.findRecords(
                    tableName, 
                    condition, 
                    this.recordManager, 
                    plan.getTransactionManager()
                );
            } else {
                // 如果不是TableManagerImpl的实例，使用默认方式
                Table table = tableManager.getTable(tableName);
                if (table == null) {
                    throw new IllegalArgumentException("表不存在: " + tableName);
                }

                // 获取表的所有页面并扫描记录
                int pageCount = 0;
                try {
                    // 尝试获取表的页面数量
                    Field field = tableManager.getClass().getDeclaredField("tablePageCounts");
                    field.setAccessible(true);
                    Map<String, Integer> tablePageCounts = (Map<String, Integer>) field.get(tableManager);
                    pageCount = tablePageCounts.getOrDefault(tableName, 0);
                } catch (Exception e) {
                    log.error("获取表页面数量失败: " + tableName, e);
                    pageCount = 10; // 默认扫描10个页面
                }

                PageManager pageManager = tableManager.getPageManager();
                for (int i = 0; i < pageCount; i++) {
                    try {
                        Page page = pageManager.readPage(i);
                        if (page == null) continue;

                        List<Record> pageRecords = recordManager.getAllRecords(page);
                        
                        // 如果没有条件，返回所有记录
                        if (condition == null || condition.isEmpty()) {
                            result.addAll(pageRecords);
                            continue;
                        }
                        
                        // 应用条件过滤
                        for (Record record : pageRecords) {
                            if (matchCondition(record, condition)) {
                                result.add(record);
                            }
                        }
                    } catch (Exception e) {
                        log.error("扫描表页面时出错: " + tableName, e);
                    }
                }
            }
        }

        // 过滤选择的列
        return filterColumns(result, plan.getSelectedColumns());
    }

    /**
     * 简单条件匹配
     */
    private boolean matchCondition(Record record, String condition) {
        // 简单处理，只支持 "column = value" 形式
        String[] parts = condition.split("\\s*=\\s*");
        if (parts.length != 2) {
            return true; // 不支持的条件格式，默认返回true
        }
        
        String columnName = parts[0].trim();
        String valueStr = parts[1].trim();
        
        // 处理字符串值（去除引号）
        if (valueStr.startsWith("'") && valueStr.endsWith("'")) {
            valueStr = valueStr.substring(1, valueStr.length() - 1);
        }
        
        // 获取记录中的字段值
        Object recordValue = record.getFieldValue(columnName);
        
        // 转换条件值
        Object conditionValue = parseValue(valueStr);
        
        // 比较
        if (recordValue == null) {
            return conditionValue == null;
        }
        
        return recordValue.equals(conditionValue);
    }

    @Override
    public List<Record> executeInsertQuery(QueryPlan plan) {
        String tableName = plan.getTableName();

        // 解析INSERT查询数据
        QueryParser.InsertQueryData insertData = queryParser.parseInsertQuery(plan.getMetadata());
        List<String> columns = insertData.getColumns();
        List<String> rawValues = insertData.getValues();

        // 解析值
        Object[] values = new Object[rawValues.size()];
        for (int i = 0; i < rawValues.size(); i++) {
            values[i] = parseValue(rawValues.get(i));
        }

        // 创建新记录
        Record record = new Record();
        for (int i = 0; i < columns.size(); i++) {
            record.setFieldValue(columns.get(i), values[i]);
        }

        // 插入记录
        List<Record> insertedRecords = new ArrayList<>();
        
        // 获取表结构
        Table table = tableManager.getTable(tableName);
        if (table == null) {
            throw new IllegalArgumentException("表不存在: " + tableName);
        }
        
        // 创建事务
        long xid = plan.getTransactionManager().beginTransaction();
        
        try {
            // 获取页面管理器
            PageManager pageManager = tableManager.getPageManager();
            
            // 为简化处理，这里假设总是使用最后一个页面
            // 实际应该根据剩余空间选择合适的页面或创建新页面
            int pageCount = 0;
            try {
                // 尝试获取表的页面数量
                Field field = tableManager.getClass().getDeclaredField("tablePageCounts");
                field.setAccessible(true);
                Map<String, Integer> tablePageCounts = (Map<String, Integer>) field.get(tableManager);
                pageCount = tablePageCounts.getOrDefault(tableName, 0);
            } catch (Exception e) {
                log.error("获取表页面数量失败: " + tableName, e);
            }
            
            // 如果没有页面，创建一个新页面
            Page page;
            if (pageCount == 0) {
                page = pageManager.createPage();
                
                // 更新表的页面数量
                try {
                    Field field = tableManager.getClass().getDeclaredField("tablePageCounts");
                    field.setAccessible(true);
                    Map<String, Integer> tablePageCounts = (Map<String, Integer>) field.get(tableManager);
                    tablePageCounts.put(tableName, 1);
                } catch (Exception e) {
                    log.error("更新表页面数量失败: " + tableName, e);
                }
            } else {
                // 获取最后一个页面
                page = pageManager.readPage(pageCount - 1);
                
                // 如果页面已满，创建新页面
                if (pageManager.getFreeSpace(page) < 100) { // 假设一条记录至少需要100字节
                    page = pageManager.createPage();
                    
                    // 更新表的页面数量
                    try {
                        Field field = tableManager.getClass().getDeclaredField("tablePageCounts");
                        field.setAccessible(true);
                        Map<String, Integer> tablePageCounts = (Map<String, Integer>) field.get(tableManager);
                        tablePageCounts.put(tableName, pageCount + 1);
                    } catch (Exception e) {
                        log.error("更新表页面数量失败: " + tableName, e);
                    }
                }
            }
            
            // 序列化记录
            byte[] data = serializeRecord(record, table);
            
            // 使用记录管理器插入记录
            Record insertedRecord = recordManager.insertRecord(page, data, xid);
            
            // 设置记录的字段值
            for (int i = 0; i < columns.size(); i++) {
                insertedRecord.setFieldValue(columns.get(i), values[i]);
            }
            
            insertedRecords.add(insertedRecord);
            
            // 提交事务
            plan.getTransactionManager().commitTransaction(xid);
        } catch (Exception e) {
            // 回滚事务
            plan.getTransactionManager().rollbackTransaction(xid);
            log.error("插入记录失败", e);
            throw new RuntimeException("插入记录失败: " + e.getMessage(), e);
        }

        return insertedRecords;
    }

    /**
     * 序列化记录为字节数组
     */
    private byte[] serializeRecord(Record record, Table table) {
        // 实际实现应当根据表结构将记录序列化为字节数组
        // 这里简化实现，假设所有字段按字符串序列化并以逗号分隔
        
        StringBuilder sb = new StringBuilder();
        for (String columnName : record.getFields().keySet()) {
            Object value = record.getFieldValue(columnName);
            sb.append(value != null ? value.toString() : "NULL").append(",");
        }
        
        // 移除最后的逗号
        if (sb.length() > 0) {
            sb.setLength(sb.length() - 1);
        }
        
        return sb.toString().getBytes();
    }

    @Override
    public List<Record> executeUpdateQuery(QueryPlan plan) {
        String tableName = plan.getTableName();

        // 解析UPDATE查询数据
        QueryParser.UpdateQueryData updateData = queryParser.parseUpdateQuery(plan.getMetadata());
        Map<String, String> setValuesStr = updateData.getSetValues();
        String condition = updateData.getCondition();

        // 解析SET子句中的值
        Map<String, Object> updateValues = new HashMap<>();
        for (Map.Entry<String, String> entry : setValuesStr.entrySet()) {
            updateValues.put(entry.getKey(), parseValue(entry.getValue()));
        }

        // 获取符合条件的记录
        List<Record> records = new ArrayList<>();
        if (plan.getIndexName() != null && !plan.getIndexName().isEmpty()) {
            // 使用索引
            QueryParser.ConditionData conditionData = queryParser.parseCondition(condition);
            Object key = parseValueFromCondition(conditionData);
            records = plan.getIndexManager().exactQuery(plan.getIndexName(), key, plan.getTransactionManager());
        } else {
            // 全表扫描
            if (tableManager instanceof DB.table.impl.TableManagerImpl) {
                // 使用TableManagerImpl的findRecords方法
                DB.table.impl.TableManagerImpl tableManagerImpl = (DB.table.impl.TableManagerImpl) tableManager;
                records = tableManagerImpl.findRecords(
                    tableName, 
                    condition, 
                    this.recordManager, 
                    plan.getTransactionManager()
                );
            } else {
                // 如果不是TableManagerImpl的实例，使用默认方式
                log.info("全表扫描更新：{}", tableName);
                // TODO: 实现通过TableManager获取表的所有记录并筛选
            }
        }

        // 更新记录
        List<Record> updatedRecords = new ArrayList<>();
        for (Record record : records) {
            if (tableManager instanceof DB.table.impl.TableManagerImpl) {
                // 使用TableManagerImpl的updateRecord方法
                DB.table.impl.TableManagerImpl tableManagerImpl = (DB.table.impl.TableManagerImpl) tableManager;
                Record updatedRecord = tableManagerImpl.updateRecord(
                    tableName,
                    record,
                    updateValues,
                    this.recordManager,
                    plan.getTransactionManager()
                );
                updatedRecords.add(updatedRecord);
            } else {
                // 如果不是TableManagerImpl的实例，使用默认方式
                for (Map.Entry<String, Object> entry : updateValues.entrySet()) {
                    record.setFieldValue(entry.getKey(), entry.getValue());
                }
                
                try {
                    // 获取记录所在页面
                    int pageId = record.getPageId();
                    PageManager pageManager = tableManager.getPageManager();
                    Page page = pageManager.readPage(pageId);
                    if (page == null) {
                        throw new IllegalStateException("找不到记录所在页面: pageId=" + pageId);
                    }
                    
                    // 序列化记录
                    // 为了简化，我们假设每个表都有列的列表
                    Table table = tableManager.getTable(tableName);
                    byte[] newData = serializeRecord(record, table);
                    
                    // 更新记录
                    long xid = plan.getTransactionManager().beginTransaction();
                    try {
                        Record updatedRecord = recordManager.updateRecord(page, record, newData, xid);
                        
                        // 拷贝字段值到更新后的记录
                        for (Map.Entry<String, Object> entry : record.getFields().entrySet()) {
                            updatedRecord.setFieldValue(entry.getKey(), entry.getValue());
                        }
                        
                        updatedRecords.add(updatedRecord);
                        plan.getTransactionManager().commitTransaction(xid);
                    } catch (Exception e) {
                        plan.getTransactionManager().rollbackTransaction(xid);
                        throw new RuntimeException("更新记录失败: " + e.getMessage(), e);
                    }
                } catch (Exception e) {
                    log.error("更新记录失败", e);
                }
            }
        }

        return updatedRecords;
    }

    @Override
    public List<Record> executeDeleteQuery(QueryPlan plan) {
        String tableName = plan.getTableName();

        // 解析DELETE查询数据
        QueryParser.DeleteQueryData deleteData = queryParser.parseDeleteQuery(plan.getMetadata());
        String condition = deleteData.getCondition();

        // 获取符合条件的记录
        List<Record> records = new ArrayList<>();
        if (plan.getIndexName() != null && !plan.getIndexName().isEmpty()) {
            // 使用索引
            QueryParser.ConditionData conditionData = queryParser.parseCondition(condition);
            Object key = parseValueFromCondition(conditionData);
            records = plan.getIndexManager().exactQuery(plan.getIndexName(), key, plan.getTransactionManager());
        } else {
            // 全表扫描
            if (tableManager instanceof DB.table.impl.TableManagerImpl) {
                // 使用TableManagerImpl的findRecords方法
                DB.table.impl.TableManagerImpl tableManagerImpl = (DB.table.impl.TableManagerImpl) tableManager;
                records = tableManagerImpl.findRecords(
                    tableName, 
                    condition, 
                    this.recordManager, 
                    plan.getTransactionManager()
                );
            } else {
                // 如果不是TableManagerImpl的实例，使用默认方式
                log.info("全表扫描删除：{}", tableName);
                
                // 获取表结构
                Table table = tableManager.getTable(tableName);
                if (table == null) {
                    throw new IllegalArgumentException("表不存在: " + tableName);
                }
                
                // 获取表的所有页面并扫描记录
                int pageCount = 0;
                try {
                    // 尝试获取表的页面数量
                    Field field = tableManager.getClass().getDeclaredField("tablePageCounts");
                    field.setAccessible(true);
                    Map<String, Integer> tablePageCounts = (Map<String, Integer>) field.get(tableManager);
                    pageCount = tablePageCounts.getOrDefault(tableName, 0);
                } catch (Exception e) {
                    log.error("获取表页面数量失败: " + tableName, e);
                    pageCount = 10; // 默认扫描10个页面
                }
                
                PageManager pageManager = tableManager.getPageManager();
                for (int i = 0; i < pageCount; i++) {
                    try {
                        Page page = pageManager.readPage(i);
                        if (page == null) continue;
                        
                        List<Record> pageRecords = recordManager.getAllRecords(page);
                        
                        // 如果没有条件，删除所有记录
                        if (condition == null || condition.isEmpty()) {
                            records.addAll(pageRecords);
                            continue;
                        }
                        
                        // 应用条件过滤
                        for (Record record : pageRecords) {
                            if (matchCondition(record, condition)) {
                                records.add(record);
                            }
                        }
                    } catch (Exception e) {
                        log.error("扫描表页面时出错: " + tableName, e);
                    }
                }
            }
        }

        // 删除记录
        List<Record> deletedRecords = new ArrayList<>();
        long xid = plan.getTransactionManager().beginTransaction();
        
        for (Record record : records) {
            try {
                // 1. 获取记录所在页面
                int pageId = record.getPageId();
                PageManager pageManager = tableManager.getPageManager();
                Page page = pageManager.readPage(pageId);
                if (page == null) {
                    log.error("找不到记录所在页面: pageId={}", pageId);
                    continue;
                }
                
                // 2. 删除记录
                recordManager.deleteRecord(page, record, xid);
                
                // 3. 保存删除的记录
                deletedRecords.add(record);
                
                log.info("删除记录：{}", record);
            } catch (Exception e) {
                log.error("删除记录失败", e);
            }
        }

        plan.getTransactionManager().commitTransaction(xid);
        return deletedRecords;
    }

    //----------- 查询优化器接口实现 -----------

    @Override
    public QueryPlan optimizeQuery(String query, IndexManager indexManager, TransactionManager transactionManager) {
        // 解析查询类型
        QueryType queryType = queryParser.determineQueryType(query);

        TransactionManager txManager = transactionManager != null ? transactionManager : this.transactionManager;
        IndexManager idxManager = indexManager != null ? indexManager : this.indexManager;

        // 根据查询类型创建查询计划
        switch (queryType) {
            case SELECT:
                return optimizeSelectQuery(query, idxManager, txManager);
            case INSERT:
                return optimizeInsertQuery(query, idxManager, txManager);
            case UPDATE:
                return optimizeUpdateQuery(query, idxManager, txManager);
            case DELETE:
                return optimizeDeleteQuery(query, idxManager, txManager);
            default:
                throw new IllegalArgumentException("不支持的查询类型: " + queryType);
        }
    }

    //----------- 辅助方法 -----------

    /**
     * 便捷方法：执行SELECT查询
     * @param tableName 表名
     * @param condition 条件
     * @param columns 列名
     * @return 结果记录
     */
    public List<Record> executeSelect(String tableName, String condition, List<String> columns) {
        StringBuilder sql = new StringBuilder("SELECT ");

        if (columns == null || columns.isEmpty()) {
            sql.append("*");
        } else {
            sql.append(String.join(", ", columns));
        }

        sql.append(" FROM ").append(tableName);

        if (condition != null && !condition.isEmpty()) {
            sql.append(" WHERE ").append(condition);
        }

        return executeQuery(sql.toString());
    }

    /**
     * 便捷方法：执行INSERT语句
     * @param tableName 表名
     * @param columns 列名
     * @param values 值
     * @return 影响的行数
     */
    public int executeInsert(String tableName, List<String> columns, List<Object> values) {
        StringBuilder sql = new StringBuilder("INSERT INTO ");
        sql.append(tableName).append(" (");
        sql.append(String.join(", ", columns));
        sql.append(") VALUES (");

        List<String> formattedValues = new ArrayList<>();
        for (Object value : values) {
            formattedValues.add(SqlParameterUtils.formatParam(value));
        }

        sql.append(String.join(", ", formattedValues));
        sql.append(")");

        return executeUpdate(sql.toString());
    }

    /**
     * 优化SELECT查询
     */
    private QueryPlan optimizeSelectQuery(String query, IndexManager indexManager, TransactionManager transactionManager) {
        // 解析查询
        QueryParser.SelectQueryData data = queryParser.parseSelectQuery(query);
        String tableName = data.getTableName();
        List<String> columns = data.getColumns();
        String condition = data.getCondition();

        // 查找适用的索引
        String indexName = findApplicableIndex(tableName, condition, indexManager);

        // 创建查询计划
        return new QueryPlanImpl(
                QueryType.SELECT,
                tableName,
                condition,
                columns,
                indexName,
                indexManager,
                transactionManager,
                query);
    }

    /**
     * 优化INSERT查询
     */
    private QueryPlan optimizeInsertQuery(String query, IndexManager indexManager, TransactionManager transactionManager) {
        // 解析查询
        QueryParser.InsertQueryData data = queryParser.parseInsertQuery(query);
        String tableName = data.getTableName();

        // 创建查询计划
        return new QueryPlanImpl(
                QueryType.INSERT,
                tableName,
                null,
                new ArrayList<>(),
                null,
                indexManager,
                transactionManager,
                query);
    }

    /**
     * 优化UPDATE查询
     */
    private QueryPlan optimizeUpdateQuery(String query, IndexManager indexManager, TransactionManager transactionManager) {
        // 解析查询
        QueryParser.UpdateQueryData data = queryParser.parseUpdateQuery(query);
        String tableName = data.getTableName();
        String condition = data.getCondition();

        // 查找适用的索引
        String indexName = findApplicableIndex(tableName, condition, indexManager);

        // 创建查询计划
        return new QueryPlanImpl(
                QueryType.UPDATE,
                tableName,
                condition,
                new ArrayList<>(),
                indexName,
                indexManager,
                transactionManager,
                query);
    }

    /**
     * 优化DELETE查询
     */
    private QueryPlan optimizeDeleteQuery(String query, IndexManager indexManager, TransactionManager transactionManager) {
        // 解析查询
        QueryParser.DeleteQueryData data = queryParser.parseDeleteQuery(query);
        String tableName = data.getTableName();
        String condition = data.getCondition();

        // 查找适用的索引
        String indexName = findApplicableIndex(tableName, condition, indexManager);

        // 创建查询计划
        return new QueryPlanImpl(
                QueryType.DELETE,
                tableName,
                condition,
                new ArrayList<>(),
                indexName,
                indexManager,
                transactionManager,
                query);
    }

    /**
     * 找出适用于条件的索引
     */
    private String findApplicableIndex(String tableName, String condition, IndexManager indexManager) {
        if (condition == null || condition.isEmpty() || indexManager == null) {
            return null;
        }

        QueryParser.ConditionData conditionData = queryParser.parseCondition(condition);
        if (conditionData == null) {
            return null;
        }

        // 现阶段简单处理，仅支持等值条件的索引查找
        String columnName = conditionData.getColumn();
        String operator = conditionData.getOperator();

        // 目前只支持等值查询的索引优化
        if ("=".equals(operator)) {
            try {
                // 使用IndexManagerImpl的方法获取列索引（如果实现了该接口）
                if (indexManager instanceof DB.index.Impl.IndexManagerImpl) {
                    DB.index.Impl.IndexManagerImpl indexManagerImpl = (DB.index.Impl.IndexManagerImpl) indexManager;
                    String indexName = indexManagerImpl.getColumnIndex(tableName, columnName);
                    if (indexName != null && indexManagerImpl.indexExists(indexName)) {
                        log.info("找到适用的索引: {}", indexName);
                        return indexName;
                    }
                }
                
                // 如果没有直接获取表索引的方法，我们可以通过检查表结构来推断
                Table table = tableManager.getTable(tableName);
                if (table != null) {
                    // 检查是否是主键
                    List<String> primaryKeys = table.getPrimaryKeys();
                    if (primaryKeys != null && primaryKeys.contains(columnName)) {
                        String pkIndexName = tableName + "_" + columnName + "_pk";
                        // 检查索引是否存在
                        if (indexManager instanceof DB.index.Impl.IndexManagerImpl) {
                            DB.index.Impl.IndexManagerImpl indexManagerImpl = (DB.index.Impl.IndexManagerImpl) indexManager;
                            if (indexManagerImpl.indexExists(pkIndexName)) {
                                log.info("使用主键索引: {}", pkIndexName);
                                return pkIndexName;
                            }
                        } else {
                            log.info("尝试使用主键索引: {}", pkIndexName);
                            return pkIndexName;
                        }
                    }
                    
                    // 检查是否是唯一键
                    for (Column column : table.getColumns()) {
                        if (column.getName().equals(columnName) && column.isUnique()) {
                            String ukIndexName = tableName + "_" + columnName + "_uk";
                            // 检查索引是否存在
                            if (indexManager instanceof DB.index.Impl.IndexManagerImpl) {
                                DB.index.Impl.IndexManagerImpl indexManagerImpl = (DB.index.Impl.IndexManagerImpl) indexManager;
                                if (indexManagerImpl.indexExists(ukIndexName)) {
                                    log.info("使用唯一键索引: {}", ukIndexName);
                                    return ukIndexName;
                                }
                            } else {
                                log.info("尝试使用唯一键索引: {}", ukIndexName);
                                return ukIndexName;
                            }
                        }
                    }
                    
                    // 尝试常规索引
                    String idxName = tableName + "_" + columnName + "_idx";
                    if (indexManager instanceof DB.index.Impl.IndexManagerImpl) {
                        DB.index.Impl.IndexManagerImpl indexManagerImpl = (DB.index.Impl.IndexManagerImpl) indexManager;
                        if (indexManagerImpl.indexExists(idxName)) {
                            log.info("使用常规索引: {}", idxName);
                            return idxName;
                        }
                    } else {
                        log.info("尝试使用常规索引: {}", idxName);
                        return idxName;
                    }
                }
                
                return null;
            } catch (Exception e) {
                log.warn("查找索引失败: {}.{}", tableName, columnName, e);
                return null;
            }
        }

        return null;
    }

    /**
     * 从条件数据中提取值
     */
    private Object parseValueFromCondition(QueryParser.ConditionData condition) {
        if (condition == null) {
            return null;
        }
        return parseValue(condition.getValue());
    }

    /**
     * 解析字符串值为适当的Java类型
     */
    private Object parseValue(String valueStr) {
        if (valueStr.startsWith("'") && valueStr.endsWith("'")) {
            // 字符串值
            return valueStr.substring(1, valueStr.length() - 1);
        } else if (valueStr.matches("-?\\d+")) {
            // 整数值
            return Integer.parseInt(valueStr);
        } else if (valueStr.matches("-?\\d+\\.\\d+")) {
            // 浮点值
            return Double.parseDouble(valueStr);
        } else if (valueStr.equalsIgnoreCase("true") || valueStr.equalsIgnoreCase("false")) {
            // 布尔值
            return Boolean.parseBoolean(valueStr);
        } else if (valueStr.equalsIgnoreCase("null")) {
            // NULL值
            return null;
        } else {
            // 默认为字符串
            return valueStr;
        }
    }

    /**
     * 过滤记录中的列
     */
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
}