package DB.table.impl;

import DB.page.interfaces.PageManager;
import DB.page.models.Page;
import DB.table.interfaces.TableManager;
import DB.table.models.Table;
import DB.table.models.Column;
import DB.transaction.interfaces.TransactionManager;
import DB.record.models.Record;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.io.IOException;

/**
 * 表管理器实现类
 */
@Slf4j
@RequiredArgsConstructor
public class TableManagerImpl implements TableManager {
    @Getter
    private final PageManager pageManager;
    private final Map<String, Table> tables = new ConcurrentHashMap<>();
    private final Map<String, Integer> tablePageCounts = new ConcurrentHashMap<>();

    @Override
    public void createTable(Table table, TransactionManager transactionManager) {
        long xid = transactionManager.beginTransaction();
        try {
            // 检查表是否已存在
            if (tables.containsKey(table.getName())) {
                throw new IllegalArgumentException("Table " + table.getName() + " already exists");
            }

            // 验证表定义
            validateTableDefinition(table);

            // 创建初始页面
            int initialPageId = pageManager.createPage().getHeader().getPageId();
            tablePageCounts.put(table.getName(), 1);

            // 设置表的创建时间
            table.setCreateTime(System.currentTimeMillis());
            table.setLastModifiedTime(System.currentTimeMillis());
            table.setPageCount(1);

            // 保存表定义
            tables.put(table.getName(), table);

            transactionManager.commitTransaction(xid);
            log.info("Created table: {}", table.getName());
        } catch (Exception e) {
            transactionManager.rollbackTransaction(xid);
            throw new RuntimeException("Failed to create table: " + table.getName(), e);
        }
    }

    @Override
    public void dropTable(String tableName, TransactionManager transactionManager) {
        long xid = transactionManager.beginTransaction();
        try {
            // 检查表是否存在
            if (!tables.containsKey(tableName)) {
                throw new IllegalArgumentException("Table " + tableName + " does not exist");
            }

            // 删除表定义
            Table table = tables.remove(tableName);
            int pageCount = tablePageCounts.remove(tableName);

            // 删除表的所有页面
            for (int i = 0; i < pageCount; i++) {
                try {
                    // 获取页面ID
                    int pageId = i; // 简化处理，实际应该根据表名查找对应的页面ID
                    Page page = pageManager.readPage(pageId);
                    if (page != null) {
                        // 设置页面为删除状态
                        page.setDirty(true);
                        // 写入空页面或者标记为可重用
                        pageManager.writePage(page);
                    }
                } catch (IOException e) {
                    log.error("删除表页面失败: " + tableName + ", pageId: " + i, e);
                }
            }

            transactionManager.commitTransaction(xid);
            log.info("Dropped table: {}", tableName);
        } catch (Exception e) {
            transactionManager.rollbackTransaction(xid);
            throw new RuntimeException("Failed to drop table: " + tableName, e);
        }
    }

    /**
     * 删除表，同时删除相关的索引
     * @param tableName 表名
     * @param transactionManager 事务管理器
     * @param indexManager 索引管理器
     */
    public void dropTable(String tableName, TransactionManager transactionManager, DB.index.interfaces.IndexManager indexManager) {
        long xid = transactionManager.beginTransaction();
        try {
            // 检查表是否存在
            if (!tables.containsKey(tableName)) {
                throw new IllegalArgumentException("Table " + tableName + " does not exist");
            }

            // 删除表定义
            Table table = tables.remove(tableName);
            int pageCount = tablePageCounts.remove(tableName);

            // 删除表的所有页面
            for (int i = 0; i < pageCount; i++) {
                try {
                    // 获取页面ID
                    int pageId = i; // 简化处理，实际应该根据表名查找对应的页面ID
                    Page page = pageManager.readPage(pageId);
                    if (page != null) {
                        // 设置页面为删除状态
                        page.setDirty(true);
                        // 写入空页面或者标记为可重用
                        pageManager.writePage(page);
                    }
                } catch (IOException e) {
                    log.error("删除表页面失败: " + tableName + ", pageId: " + i, e);
                }
            }

            // 删除相关的索引
            if (indexManager != null) {
                if (indexManager instanceof DB.index.Impl.IndexManagerImpl) {
                    // 如果是我们的索引管理器实现，可以直接获取表的所有索引
                    DB.index.Impl.IndexManagerImpl indexManagerImpl = (DB.index.Impl.IndexManagerImpl) indexManager;
                    List<String> indexes = indexManagerImpl.getTableIndexes(tableName);
                    for (String indexName : indexes) {
                        try {
                            indexManager.dropIndex(indexName, transactionManager);
                            log.info("删除表相关索引: {}", indexName);
                        } catch (Exception e) {
                            log.error("删除索引失败: " + indexName, e);
                        }
                    }
                } else {
                    // 对于其他实现，尝试按照命名规则删除索引
                    // 获取表的所有索引并删除
                    List<String> columnsWithIndex = new ArrayList<>();
                    for (Column column : table.getColumns()) {
                        // 检查列是否有索引（通常是主键、唯一键或其他显式创建的索引）
                        if (column.isPrimaryKey() || column.isUnique()) {
                            columnsWithIndex.add(column.getName());
                        }
                    }
                    
                    // 尝试删除可能的索引
                    for (String columnName : columnsWithIndex) {
                        // 尝试删除主键索引
                        if (columnName.equals(table.getPrimaryKeys().get(0))) {
                            String pkIndexName = tableName + "_" + columnName + "_pk";
                            try {
                                indexManager.dropIndex(pkIndexName, transactionManager);
                                log.info("删除主键索引: {}", pkIndexName);
                            } catch (Exception e) {
                                log.error("删除主键索引失败: " + pkIndexName, e);
                            }
                        }
                        
                        // 尝试删除唯一键索引
                        if (table.getUniqueKeys().contains(columnName)) {
                            String ukIndexName = tableName + "_" + columnName + "_uk";
                            try {
                                indexManager.dropIndex(ukIndexName, transactionManager);
                                log.info("删除唯一键索引: {}", ukIndexName);
                            } catch (Exception e) {
                                log.error("删除唯一键索引失败: " + ukIndexName, e);
                            }
                        }
                        
                        // 尝试删除普通索引
                        String idxName = tableName + "_" + columnName + "_idx";
                        try {
                            indexManager.dropIndex(idxName, transactionManager);
                            log.info("删除索引: {}", idxName);
                        } catch (Exception e) {
                            log.error("删除索引失败: " + idxName, e);
                        }
                    }
                }
            }

            transactionManager.commitTransaction(xid);
            log.info("Dropped table: {}", tableName);
        } catch (Exception e) {
            transactionManager.rollbackTransaction(xid);
            throw new RuntimeException("Failed to drop table: " + tableName, e);
        }
    }

    @Override
    public void alterTable(String tableName, String alterType, String alterDefinition, TransactionManager transactionManager) {
        long xid = transactionManager.beginTransaction();
        try {
            // 检查表是否存在
            Table table = tables.get(tableName);
            if (table == null) {
                throw new IllegalArgumentException("Table " + tableName + " does not exist");
            }

            // 根据修改类型执行不同的操作
            switch (alterType.toUpperCase()) {
                case "ADD COLUMN":
                    addColumn(table, alterDefinition);
                    break;
                case "DROP COLUMN":
                    dropColumn(table, alterDefinition);
                    break;
                case "MODIFY COLUMN":
                    modifyColumn(table, alterDefinition);
                    break;
                case "ADD CONSTRAINT":
                    addConstraint(table, alterDefinition);
                    break;
                case "DROP CONSTRAINT":
                    dropConstraint(table, alterDefinition);
                    break;
                default:
                    throw new IllegalArgumentException("Unsupported alter type: " + alterType);
            }

            // 更新表的最后修改时间
            table.setLastModifiedTime(System.currentTimeMillis());

            transactionManager.commitTransaction(xid);
            log.info("Altered table: {}", tableName);
        } catch (Exception e) {
            transactionManager.rollbackTransaction(xid);
            throw new RuntimeException("Failed to alter table: " + tableName, e);
        }
    }

    @Override
    public Table getTable(String tableName) {
        return tables.get(tableName);
    }

    @Override
    public List<String> getAllTables() {
        return new ArrayList<>(tables.keySet());
    }

    /**
     * 查找表中符合条件的记录
     * @param tableName 表名
     * @param condition 条件字符串
     * @param recordManager 记录管理器
     * @param transactionManager 事务管理器
     * @return 符合条件的记录列表
     */
    public List<Record> findRecords(String tableName, String condition, 
                                   DB.record.interfaces.RecordManager recordManager, 
                                   TransactionManager transactionManager) {
        Table table = getTable(tableName);
        if (table == null) {
            throw new IllegalArgumentException("表不存在: " + tableName);
        }

        List<Record> results = new ArrayList<>();
        long xid = -1;
        if (transactionManager != null) {
            xid = transactionManager.beginTransaction();
        }

        // 获取表的所有页面并扫描记录
        int pageCount = tablePageCounts.getOrDefault(tableName, 0);
        for (int i = 0; i < pageCount; i++) {
            try {
                // 假设每个表的页面ID是连续的，从表创建时的初始页面ID开始
                // 实际实现可能需要维护页面ID的映射
                // 这里简化处理，假设可以根据表名和页面索引获取页面
                Page page = pageManager.readPage(i);
                if (page == null) continue;

                List<Record> pageRecords = recordManager.getAllRecords(page);
                
                // 如果没有条件，返回所有记录
                if (condition == null || condition.isEmpty()) {
                    results.addAll(pageRecords);
                    continue;
                }
                
                // 应用条件过滤
                for (Record record : pageRecords) {
                    if (matchesCondition(record, condition, table)) {
                        results.add(record);
                    }
                }
            } catch (Exception e) {
                log.error("扫描表页面时出错: " + tableName, e);
            }
        }

        // 如果开启了事务，提交事务
        if (transactionManager != null && xid != -1) {
            transactionManager.commitTransaction(xid);
        }
        
        return results;
    }
    
    /**
     * 更新表中的记录
     * @param tableName 表名
     * @param record 要更新的记录
     * @param updateValues 更新的字段值映射
     * @param recordManager 记录管理器
     * @param transactionManager 事务管理器
     * @return 更新后的记录
     */
    public Record updateRecord(String tableName, Record record, Map<String, Object> updateValues,
                              DB.record.interfaces.RecordManager recordManager,
                              TransactionManager transactionManager) {
        Table table = getTable(tableName);
        if (table == null) {
            throw new IllegalArgumentException("表不存在: " + tableName);
        }
        
        long xid = transactionManager.beginTransaction();
        boolean newTransaction = true;
        
        try {
            // 1. 获取记录所在页面
            int pageId = record.getPageId();
            Page page = pageManager.readPage(pageId);
            if (page == null) {
                throw new IllegalStateException("找不到记录所在页面: pageId=" + pageId);
            }
            
            // 2. 应用更新值到记录
            for (Map.Entry<String, Object> entry : updateValues.entrySet()) {
                record.setFieldValue(entry.getKey(), entry.getValue());
            }
            
            // 3. 将记录序列化为字节数组（此处简化处理）
            byte[] newData = serializeRecord(record, table);
            
            // 4. 更新记录
            Record updatedRecord = recordManager.updateRecord(page, record, newData, xid);
            
            // 5. 设置记录的字段值映射
            Map<String, Object> fields = record.getFields();
            updatedRecord.setFields(new HashMap<>(fields));
            
            // 如果是新事务，提交事务
            if (newTransaction) {
                transactionManager.commitTransaction(xid);
            }
            
            return updatedRecord;
        } catch (Exception e) {
            // 如果是新事务，回滚事务
            if (newTransaction) {
                transactionManager.rollbackTransaction(xid);
            }
            throw new RuntimeException("更新记录失败", e);
        }
    }
    
    /**
     * 判断记录是否匹配条件
     * @param record 记录
     * @param condition 条件字符串
     * @param table 表定义
     * @return 是否匹配
     */
    private boolean matchesCondition(Record record, String condition, Table table) {
        // 简单条件解析，格式如: "column operator value"
        // 例如: "id = 1" 或 "age > 30"
        
        // 分割条件字符串
        String[] parts = condition.split("\\s+");
        if (parts.length != 3) {
            throw new IllegalArgumentException("无效的条件格式: " + condition);
        }
        
        String columnName = parts[0];
        String operator = parts[1];
        String valueStr = parts[2];
        
        // 处理字符串值（去除引号）
        if (valueStr.startsWith("'") && valueStr.endsWith("'")) {
            valueStr = valueStr.substring(1, valueStr.length() - 1);
        }
        
        // 获取记录中的字段值
        Object recordValue = record.getFieldValue(columnName);
        
        // 类型转换
        Object conditionValue = convertValue(valueStr, getColumnType(table, columnName));
        
        // 比较
        return compareValues(recordValue, conditionValue, operator);
    }
    
    /**
     * 根据列名获取列类型
     * @param table 表定义
     * @param columnName 列名
     * @return 列类型
     */
    private Column.DataType getColumnType(Table table, String columnName) {
        for (Column column : table.getColumns()) {
            if (column.getName().equals(columnName)) {
                return column.getDataType();
            }
        }
        throw new IllegalArgumentException("列不存在: " + columnName);
    }
    
    /**
     * 根据类型转换字符串值
     * @param valueStr 字符串值
     * @param dataType 数据类型
     * @return 转换后的值
     */
    private Object convertValue(String valueStr, Column.DataType dataType) {
        switch (dataType) {
            case INT:
                return Integer.parseInt(valueStr);
            case FLOAT:
            case DOUBLE:
                return Double.parseDouble(valueStr);
            case BOOLEAN:
                return Boolean.parseBoolean(valueStr);
            case DATE:
            case TIMESTAMP:
                // 日期时间类型的转换（简化处理）
                return valueStr;
            case VARCHAR:
            default:
                return valueStr;
        }
    }
    
    /**
     * 比较两个值
     * @param value1 第一个值
     * @param value2 第二个值
     * @param operator 操作符
     * @return 比较结果
     */
    private boolean compareValues(Object value1, Object value2, String operator) {
        if (value1 == null || value2 == null) {
            // NULL值处理
            return "=".equals(operator) ? (value1 == value2) : (value1 != value2);
        }
        
        // 数字类型比较
        if (value1 instanceof Number && value2 instanceof Number) {
            double num1 = ((Number) value1).doubleValue();
            double num2 = ((Number) value2).doubleValue();
            
            switch (operator) {
                case "=": return Double.compare(num1, num2) == 0;
                case "!=": return Double.compare(num1, num2) != 0;
                case ">": return Double.compare(num1, num2) > 0;
                case ">=": return Double.compare(num1, num2) >= 0;
                case "<": return Double.compare(num1, num2) < 0;
                case "<=": return Double.compare(num1, num2) <= 0;
                default: throw new IllegalArgumentException("不支持的操作符: " + operator);
            }
        }
        
        // 字符串比较
        if (value1 instanceof String && value2 instanceof String) {
            int result = ((String) value1).compareTo((String) value2);
            
            switch (operator) {
                case "=": return result == 0;
                case "!=": return result != 0;
                case ">": return result > 0;
                case ">=": return result >= 0;
                case "<": return result < 0;
                case "<=": return result <= 0;
                default: throw new IllegalArgumentException("不支持的操作符: " + operator);
            }
        }
        
        // 布尔值比较
        if (value1 instanceof Boolean && value2 instanceof Boolean) {
            boolean bool1 = (Boolean) value1;
            boolean bool2 = (Boolean) value2;
            
            switch (operator) {
                case "=": return bool1 == bool2;
                case "!=": return bool1 != bool2;
                default: throw new IllegalArgumentException("布尔类型只支持 = 和 != 操作符");
            }
        }
        
        // 默认使用equals比较
        if ("=".equals(operator)) {
            return value1.equals(value2);
        } else if ("!=".equals(operator)) {
            return !value1.equals(value2);
        } else {
            throw new IllegalArgumentException("不兼容的类型比较: " + value1.getClass() + " 和 " + value2.getClass());
        }
    }
    
    /**
     * 序列化记录为字节数组
     * @param record 记录
     * @param table 表定义
     * @return 序列化后的字节数组
     */
    private byte[] serializeRecord(Record record, Table table) {
        // 简化实现，实际应根据表结构和字段类型进行序列化
        // 这里只是一个示例
        StringBuilder sb = new StringBuilder();
        
        for (Column column : table.getColumns()) {
            String fieldName = column.getName();
            Object value = record.getFieldValue(fieldName);
            
            if (value != null) {
                sb.append(fieldName).append("=").append(value).append(";");
            }
        }
        
        return sb.toString().getBytes();
    }

    /**
     * 添加列
     */
    private void addColumn(Table table, String columnDefinition) {
        // 解析列定义
        Column column = parseColumnDefinition(columnDefinition);
        
        // 检查列名是否已存在
        for (Column existingColumn : table.getColumns()) {
            if (existingColumn.getName().equals(column.getName())) {
                throw new IllegalArgumentException("Column " + column.getName() + " already exists");
            }
        }

        // 添加列
        table.getColumns().add(column);
        
        // 如果是主键，添加到主键列表
        if (column.isPrimaryKey()) {
            table.getPrimaryKeys().add(column.getName());
        }
        
        // 如果是唯一键，添加到唯一键列表
        if (column.isUnique()) {
            table.getUniqueKeys().add(column.getName());
        }
    }

    /**
     * 删除列
     */
    private void dropColumn(Table table, String columnName) {
        // 检查列是否存在
        Column columnToRemove = null;
        for (Column column : table.getColumns()) {
            if (column.getName().equals(columnName)) {
                columnToRemove = column;
                break;
            }
        }
        
        if (columnToRemove == null) {
            throw new IllegalArgumentException("Column " + columnName + " does not exist");
        }

        // 检查是否是主键或外键
        if (table.getPrimaryKeys().contains(columnName)) {
            throw new IllegalArgumentException("Cannot drop primary key column: " + columnName);
        }
        
        for (Table.ForeignKey fk : table.getForeignKeys()) {
            if (fk.getColumns().contains(columnName)) {
                throw new IllegalArgumentException("Cannot drop foreign key column: " + columnName);
            }
        }

        // 删除列
        table.getColumns().remove(columnToRemove);
        table.getPrimaryKeys().remove(columnName);
        table.getUniqueKeys().remove(columnName);
    }

    /**
     * 修改列
     */
    private void modifyColumn(Table table, String columnDefinition) {
        // 解析列定义
        Column newColumn = parseColumnDefinition(columnDefinition);
        
        // 查找要修改的列
        int index = -1;
        for (int i = 0; i < table.getColumns().size(); i++) {
            if (table.getColumns().get(i).getName().equals(newColumn.getName())) {
                index = i;
                break;
            }
        }
        
        if (index == -1) {
            throw new IllegalArgumentException("Column " + newColumn.getName() + " does not exist");
        }

        // 检查修改是否合法
        Column oldColumn = table.getColumns().get(index);
        validateColumnModification(oldColumn, newColumn);

        // 更新列定义
        table.getColumns().set(index, newColumn);
        
        // 更新主键和唯一键
        if (newColumn.isPrimaryKey() && !table.getPrimaryKeys().contains(newColumn.getName())) {
            table.getPrimaryKeys().add(newColumn.getName());
        } else if (!newColumn.isPrimaryKey()) {
            table.getPrimaryKeys().remove(newColumn.getName());
        }
        
        if (newColumn.isUnique() && !table.getUniqueKeys().contains(newColumn.getName())) {
            table.getUniqueKeys().add(newColumn.getName());
        } else if (!newColumn.isUnique()) {
            table.getUniqueKeys().remove(newColumn.getName());
        }
    }

    /**
     * 添加约束
     */
    private void addConstraint(Table table, String constraintDefinition) {
        // 解析约束定义
        String[] parts = constraintDefinition.split("\\s+");
        String constraintType = parts[0].toUpperCase();
        
        switch (constraintType) {
            case "PRIMARY":
                addPrimaryKey(table, parts);
                break;
            case "FOREIGN":
                addForeignKey(table, parts);
                break;
            case "UNIQUE":
                addUniqueKey(table, parts);
                break;
            case "CHECK":
                addCheckConstraint(table, constraintDefinition);
                break;
            default:
                throw new IllegalArgumentException("Unsupported constraint type: " + constraintType);
        }
    }

    /**
     * 删除约束
     */
    private void dropConstraint(Table table, String constraintName) {
        // 检查约束是否存在
        boolean found = false;
        
        // 检查主键
        if (table.getPrimaryKeys().contains(constraintName)) {
            table.getPrimaryKeys().remove(constraintName);
            found = true;
        }
        
        // 检查外键
        for (Table.ForeignKey fk : table.getForeignKeys()) {
            if (fk.getName().equals(constraintName)) {
                table.getForeignKeys().remove(fk);
                found = true;
                break;
            }
        }
        
        // 检查唯一键
        if (table.getUniqueKeys().contains(constraintName)) {
            table.getUniqueKeys().remove(constraintName);
            found = true;
        }
        
        // 检查检查约束
        if (table.getCheckConstraints().contains(constraintName)) {
            table.getCheckConstraints().remove(constraintName);
            found = true;
        }
        
        if (!found) {
            throw new IllegalArgumentException("Constraint " + constraintName + " does not exist");
        }
    }

    /**
     * 解析列定义
     */
    private Column parseColumnDefinition(String definition) {
        // 解析列定义，格式：column_name data_type [length] [NOT NULL] [PRIMARY KEY] [UNIQUE] [DEFAULT value] [CHECK (condition)]
        String[] parts = definition.trim().split("\\s+");
        if (parts.length < 2) {
            throw new IllegalArgumentException("Invalid column definition: " + definition);
        }

        Column.ColumnBuilder builder = Column.builder();
        int index = 0;

        // 解析列名
        builder.name(parts[index++]);

        // 解析数据类型
        String typeStr = parts[index++].toUpperCase();
        Column.DataType dataType;
        int length = 0;
        try {
            dataType = Column.DataType.valueOf(typeStr);
            length = dataType.getDefaultLength();
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Invalid data type: " + typeStr);
        }

        // 解析长度（如果有）
        if (index < parts.length && parts[index].startsWith("(")) {
            String lengthStr = parts[index].substring(1, parts[index].length() - 1);
            try {
                length = Integer.parseInt(lengthStr);
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("Invalid length: " + lengthStr);
            }
            index++;
        }

        builder.dataType(dataType);
        builder.length(length);

        // 解析其他属性
        while (index < parts.length) {
            String token = parts[index++].toUpperCase();
            switch (token) {
                case "NOT":
                    if (index < parts.length && parts[index].equalsIgnoreCase("NULL")) {
                        builder.nullable(false);
                        index++;
                    }
                    break;
                case "PRIMARY":
                    if (index < parts.length && parts[index].equalsIgnoreCase("KEY")) {
                        builder.primaryKey(true);
                        index++;
                    }
                    break;
                case "UNIQUE":
                    builder.unique(true);
                    break;
                case "DEFAULT":
                    if (index < parts.length) {
                        String defaultValue = parts[index++];
                        // 处理带引号的字符串
                        if (defaultValue.startsWith("'") && defaultValue.endsWith("'")) {
                            defaultValue = defaultValue.substring(1, defaultValue.length() - 1);
                        }
                        builder.defaultValue(defaultValue);
                    }
                    break;
                case "CHECK":
                    if (index < parts.length && parts[index].equals("(")) {
                        StringBuilder checkCondition = new StringBuilder();
                        int parenCount = 1;
                        index++;
                        while (index < parts.length && parenCount > 0) {
                            if (parts[index].equals("(")) parenCount++;
                            if (parts[index].equals(")")) parenCount--;
                            if (parenCount > 0) {
                                checkCondition.append(parts[index]).append(" ");
                            }
                            index++;
                        }
                        builder.checkConstraint(checkCondition.toString().trim());
                    }
                    break;
            }
        }

        return builder.build();
    }

    /**
     * 验证列修改
     */
    private void validateColumnModification(Column oldColumn, Column newColumn) {
        // 检查数据类型修改
        if (oldColumn.getDataType() != newColumn.getDataType()) {
            throw new IllegalArgumentException("Cannot change column data type");
        }
        
        // 检查长度修改
        if (oldColumn.getLength() != newColumn.getLength()) {
            throw new IllegalArgumentException("Cannot change column length");
        }
        
        // 检查主键修改
        if (oldColumn.isPrimaryKey() != newColumn.isPrimaryKey()) {
            throw new IllegalArgumentException("Cannot change primary key status");
        }
    }

    /**
     * 添加主键
     */
    private void addPrimaryKey(Table table, String[] parts) {
        if (parts.length < 4 || !parts[1].equalsIgnoreCase("KEY") || !parts[2].equals("(")) {
            throw new IllegalArgumentException("Invalid primary key definition");
        }

        List<String> pkColumns = new ArrayList<>();
        int i = 3;
        while (i < parts.length && !parts[i].equals(")")) {
            String columnName = parts[i].replace(",", "").trim();
            // 验证列是否存在
            boolean found = false;
            for (Column column : table.getColumns()) {
                if (column.getName().equals(columnName)) {
                    found = true;
                    break;
                }
            }
            if (!found) {
                throw new IllegalArgumentException("Column " + columnName + " does not exist");
            }
            pkColumns.add(columnName);
            i++;
        }

        // 更新主键
        table.getPrimaryKeys().clear();
        table.getPrimaryKeys().addAll(pkColumns);

        // 更新列的primaryKey属性
        for (Column column : table.getColumns()) {
            column.setPrimaryKey(pkColumns.contains(column.getName()));
        }
    }

    /**
     * 添加外键
     */
    private void addForeignKey(Table table, String[] parts) {
        if (parts.length < 8 || !parts[1].equalsIgnoreCase("KEY") || !parts[2].equals("(")) {
            throw new IllegalArgumentException("Invalid foreign key definition");
        }

        Table.ForeignKey fk = new Table.ForeignKey();
        List<String> columns = new ArrayList<>();
        List<String> refColumns = new ArrayList<>();

        // 解析外键列
        int i = 3;
        while (i < parts.length && !parts[i].equals(")")) {
            String columnName = parts[i].replace(",", "").trim();
            // 验证列是否存在
            boolean found = false;
            for (Column column : table.getColumns()) {
                if (column.getName().equals(columnName)) {
                    found = true;
                    break;
                }
            }
            if (!found) {
                throw new IllegalArgumentException("Column " + columnName + " does not exist");
            }
            columns.add(columnName);
            i++;
        }

        // 解析引用表
        i += 2; // 跳过 ) REFERENCES
        String refTable = parts[i++];

        // 解析引用列
        i += 1; // 跳过 (
        while (i < parts.length && !parts[i].equals(")")) {
            String columnName = parts[i].replace(",", "").trim();
            refColumns.add(columnName);
            i++;
        }

        // 解析ON DELETE和ON UPDATE
        String onDelete = "NO ACTION";
        String onUpdate = "NO ACTION";
        while (i < parts.length) {
            if (parts[i].equalsIgnoreCase("ON")) {
                i++;
                if (parts[i].equalsIgnoreCase("DELETE")) {
                    i++;
                    onDelete = parts[i];
                } else if (parts[i].equalsIgnoreCase("UPDATE")) {
                    i++;
                    onUpdate = parts[i];
                }
            }
            i++;
        }

        // 设置外键属性
        fk.setName(table.getName() + "_" + String.join("_", columns) + "_fk");
        fk.setColumns(columns);
        fk.setRefTable(refTable);
        fk.setRefColumns(refColumns);
        fk.setOnDelete(onDelete);
        fk.setOnUpdate(onUpdate);

        // 添加外键
        table.getForeignKeys().add(fk);
    }

    /**
     * 添加唯一键
     */
    private void addUniqueKey(Table table, String[] parts) {
        if (parts.length < 3 || !parts[1].equals("(")) {
            throw new IllegalArgumentException("Invalid unique key definition");
        }

        List<String> uniqueColumns = new ArrayList<>();
        int i = 2;
        while (i < parts.length && !parts[i].equals(")")) {
            String columnName = parts[i].replace(",", "").trim();
            // 验证列是否存在
            boolean found = false;
            for (Column column : table.getColumns()) {
                if (column.getName().equals(columnName)) {
                    found = true;
                    break;
                }
            }
            if (!found) {
                throw new IllegalArgumentException("Column " + columnName + " does not exist");
            }
            uniqueColumns.add(columnName);
            i++;
        }

        // 更新唯一键
        table.getUniqueKeys().addAll(uniqueColumns);

        // 更新列的unique属性
        for (Column column : table.getColumns()) {
            column.setUnique(uniqueColumns.contains(column.getName()));
        }
    }

    /**
     * 添加检查约束
     */
    private void addCheckConstraint(Table table, String definition) {
        // 解析检查约束名称和条件
        String[] parts = definition.split("\\s+", 3);
        if (parts.length < 3 || !parts[1].equals("(")) {
            throw new IllegalArgumentException("Invalid check constraint definition");
        }

        String constraintName = table.getName() + "_check_" + System.currentTimeMillis();
        String condition = parts[2].substring(0, parts[2].length() - 1); // 去掉最后的 )

        // 验证条件中引用的列是否存在
        String[] tokens = condition.split("\\s+");
        for (String token : tokens) {
            if (token.matches("[a-zA-Z_][a-zA-Z0-9_]*")) { // 可能是列名
                boolean found = false;
                for (Column column : table.getColumns()) {
                    if (column.getName().equals(token)) {
                        found = true;
                        break;
                    }
                }
                if (!found) {
                    throw new IllegalArgumentException("Column " + token + " does not exist in check constraint");
                }
            }
        }

        // 添加检查约束
        table.getCheckConstraints().add(constraintName + ":" + condition);
    }

    /**
     * 验证表定义
     */
    private void validateTableDefinition(Table table) {
        // 检查表名
        if (table.getName() == null || table.getName().isEmpty()) {
            throw new IllegalArgumentException("Table name cannot be empty");
        }

        // 检查列定义
        if (table.getColumns() == null || table.getColumns().isEmpty()) {
            throw new IllegalArgumentException("Table must have at least one column");
        }

        // 检查主键
        if (table.getPrimaryKeys() != null && !table.getPrimaryKeys().isEmpty()) {
            for (String pk : table.getPrimaryKeys()) {
                boolean found = false;
                for (Column column : table.getColumns()) {
                    if (column.getName().equals(pk)) {
                        found = true;
                        break;
                    }
                }
                if (!found) {
                    throw new IllegalArgumentException("Primary key column " + pk + " does not exist");
                }
            }
        }

        // 检查外键
        if (table.getForeignKeys() != null) {
            for (Table.ForeignKey fk : table.getForeignKeys()) {
                // 检查外键列是否存在
                for (String column : fk.getColumns()) {
                    boolean found = false;
                    for (Column tableColumn : table.getColumns()) {
                        if (tableColumn.getName().equals(column)) {
                            found = true;
                            break;
                        }
                    }
                    if (!found) {
                        throw new IllegalArgumentException("Foreign key column " + column + " does not exist");
                    }
                }

                // 检查引用表是否存在
                if (!tables.containsKey(fk.getRefTable())) {
                    throw new IllegalArgumentException("Referenced table " + fk.getRefTable() + " does not exist");
                }

                // 检查引用列是否存在
                Table refTable = tables.get(fk.getRefTable());
                for (String refColumn : fk.getRefColumns()) {
                    boolean found = false;
                    for (Column column : refTable.getColumns()) {
                        if (column.getName().equals(refColumn)) {
                            found = true;
                            break;
                        }
                    }
                    if (!found) {
                        throw new IllegalArgumentException("Referenced column " + refColumn + " does not exist");
                    }
                }
            }
        }
    }

    /**
     * 为表的列创建索引
     * @param tableName 表名
     * @param columnName 列名
     * @param indexName 索引名称
     * @param indexManager 索引管理器
     * @param transactionManager 事务管理器
     */
    public void createIndex(String tableName, String columnName, String indexName, 
                         DB.index.interfaces.IndexManager indexManager, 
                         TransactionManager transactionManager) {
        // 检查表是否存在
        Table table = getTable(tableName);
        if (table == null) {
            throw new IllegalArgumentException("表不存在: " + tableName);
        }
        
        // 检查列是否存在
        boolean columnExists = false;
        for (Column column : table.getColumns()) {
            if (column.getName().equals(columnName)) {
                columnExists = true;
                break;
            }
        }
        
        if (!columnExists) {
            throw new IllegalArgumentException("列不存在: " + columnName);
        }
        
        // 创建索引
        indexManager.createIndex(tableName, columnName, indexName, transactionManager);
        
        log.info("为表 {} 的列 {} 创建索引 {}", tableName, columnName, indexName);
    }
    
    /**
     * 为表的列创建主键索引
     * @param tableName 表名
     * @param columnName 列名
     * @param indexManager 索引管理器
     * @param transactionManager 事务管理器
     */
    public void createPrimaryKeyIndex(String tableName, String columnName, 
                                   DB.index.interfaces.IndexManager indexManager, 
                                   TransactionManager transactionManager) {
        // 检查表是否存在
        Table table = getTable(tableName);
        if (table == null) {
            throw new IllegalArgumentException("表不存在: " + tableName);
        }
        
        // 检查列是否为主键
        boolean isPrimaryKey = false;
        for (Column column : table.getColumns()) {
            if (column.getName().equals(columnName) && column.isPrimaryKey()) {
                isPrimaryKey = true;
                break;
            }
        }
        
        if (!isPrimaryKey) {
            throw new IllegalArgumentException("列 " + columnName + " 不是主键");
        }
        
        // 创建主键索引
        String indexName = tableName + "_" + columnName + "_pk";
        
        // 如果是IndexManagerImpl，使用它的专用方法
        if (indexManager instanceof DB.index.Impl.IndexManagerImpl) {
            DB.index.Impl.IndexManagerImpl indexManagerImpl = (DB.index.Impl.IndexManagerImpl) indexManager;
            indexManagerImpl.createPrimaryKeyIndex(tableName, columnName, transactionManager);
        } else {
            indexManager.createIndex(tableName, columnName, indexName, transactionManager);
        }
        
        log.info("为表 {} 的主键列 {} 创建索引 {}", tableName, columnName, indexName);
    }
    
    /**
     * 为表的唯一列创建唯一索引
     * @param tableName 表名
     * @param columnName 列名
     * @param indexManager 索引管理器
     * @param transactionManager 事务管理器
     */
    public void createUniqueIndex(String tableName, String columnName, 
                               DB.index.interfaces.IndexManager indexManager, 
                               TransactionManager transactionManager) {
        // 检查表是否存在
        Table table = getTable(tableName);
        if (table == null) {
            throw new IllegalArgumentException("表不存在: " + tableName);
        }
        
        // 检查列是否有唯一约束
        boolean isUnique = false;
        for (Column column : table.getColumns()) {
            if (column.getName().equals(columnName) && column.isUnique()) {
                isUnique = true;
                break;
            }
        }
        
        if (!isUnique) {
            throw new IllegalArgumentException("列 " + columnName + " 没有唯一约束");
        }
        
        // 创建唯一索引
        String indexName = tableName + "_" + columnName + "_uk";
        
        // 如果是IndexManagerImpl，使用它的专用方法
        if (indexManager instanceof DB.index.Impl.IndexManagerImpl) {
            DB.index.Impl.IndexManagerImpl indexManagerImpl = (DB.index.Impl.IndexManagerImpl) indexManager;
            indexManagerImpl.createUniqueIndex(tableName, columnName, transactionManager);
        } else {
            indexManager.createIndex(tableName, columnName, indexName, transactionManager);
        }
        
        log.info("为表 {} 的唯一列 {} 创建索引 {}", tableName, columnName, indexName);
    }
    
    /**
     * 为表的所有主键列创建索引
     * @param tableName 表名
     * @param indexManager 索引管理器
     * @param transactionManager 事务管理器
     */
    public void createPrimaryKeyIndexes(String tableName, 
                                     DB.index.interfaces.IndexManager indexManager, 
                                     TransactionManager transactionManager) {
        // 检查表是否存在
        Table table = getTable(tableName);
        if (table == null) {
            throw new IllegalArgumentException("表不存在: " + tableName);
        }
        
        // 获取主键列
        List<String> primaryKeys = table.getPrimaryKeys();
        if (primaryKeys == null || primaryKeys.isEmpty()) {
            log.info("表 {} 没有主键", tableName);
            return;
        }
        
        // 为每个主键列创建索引
        for (String columnName : primaryKeys) {
            try {
                createPrimaryKeyIndex(tableName, columnName, indexManager, transactionManager);
            } catch (Exception e) {
                log.error("为主键列 {} 创建索引失败", columnName, e);
            }
        }
    }
    
    /**
     * 为表的所有唯一列创建索引
     * @param tableName 表名
     * @param indexManager 索引管理器
     * @param transactionManager 事务管理器
     */
    public void createUniqueIndexes(String tableName, 
                                 DB.index.interfaces.IndexManager indexManager, 
                                 TransactionManager transactionManager) {
        // 检查表是否存在
        Table table = getTable(tableName);
        if (table == null) {
            throw new IllegalArgumentException("表不存在: " + tableName);
        }
        
        // 获取唯一键列
        List<String> uniqueKeys = table.getUniqueKeys();
        if (uniqueKeys == null || uniqueKeys.isEmpty()) {
            log.info("表 {} 没有唯一键", tableName);
            return;
        }
        
        // 为每个唯一键列创建索引
        for (String columnName : uniqueKeys) {
            try {
                createUniqueIndex(tableName, columnName, indexManager, transactionManager);
            } catch (Exception e) {
                log.error("为唯一列 {} 创建索引失败", columnName, e);
            }
        }
    }
    
    /**
     * 重建表的所有索引
     * @param tableName 表名
     * @param indexManager 索引管理器
     * @param transactionManager 事务管理器
     */
    public void rebuildAllIndexes(String tableName, 
                               DB.index.interfaces.IndexManager indexManager, 
                               TransactionManager transactionManager) {
        // 检查表是否存在
        Table table = getTable(tableName);
        if (table == null) {
            throw new IllegalArgumentException("表不存在: " + tableName);
        }
        
        // 删除现有索引（如果有）
        try {
            dropTable(tableName, transactionManager, indexManager);
            // 重新创建表
            createTable(table, transactionManager);
        } catch (Exception e) {
            log.error("重建表索引失败: {}", tableName, e);
            throw new RuntimeException("重建索引失败", e);
        }
        
        // 创建主键索引
        createPrimaryKeyIndexes(tableName, indexManager, transactionManager);
        
        // 创建唯一键索引
        createUniqueIndexes(tableName, indexManager, transactionManager);
        
        log.info("表 {} 的所有索引重建完成", tableName);
    }
} 