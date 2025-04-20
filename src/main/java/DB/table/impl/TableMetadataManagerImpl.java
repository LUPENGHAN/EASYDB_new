package DB.table.impl;

import DB.page.interfaces.PageManager;
import DB.record.models.Record;
import DB.table.interfaces.TableMetadataManager;
import DB.table.models.Column;
import DB.table.models.Table;
import DB.transaction.interfaces.TransactionManager;
import lombok.extern.slf4j.Slf4j;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * 表元数据管理器实现类
 */
@Slf4j
public class TableMetadataManagerImpl implements TableMetadataManager {
    // 元数据表名
    private static final String META_TABLES = "sys_tables";
    private static final String META_COLUMNS = "sys_columns";
    private static final String META_CONSTRAINTS = "sys_constraints";
    private static final String META_STATISTICS = "sys_statistics";
    
    // 内存中的表元数据缓存
    private final Map<String, Table> tablesCache = new ConcurrentHashMap<>();
    private final Map<String, List<Column>> columnsCache = new ConcurrentHashMap<>();
    private final Map<String, TableStatisticsImpl> statisticsCache = new ConcurrentHashMap<>();
    
    private final PageManager pageManager;
    
    public TableMetadataManagerImpl(PageManager pageManager) {
        this.pageManager = pageManager;
        initializeMetaTables();
    }
    
    /**
     * 初始化元数据表
     */
    private void initializeMetaTables() {
        // 如果元数据表不存在，创建它们
        if (!tableExists(META_TABLES)) {
            createMetaTables();
        }
        
        // 加载所有表的元数据到缓存
        loadAllTablesMetadata();
    }
    
    /**
     * 创建元数据表
     */
    private void createMetaTables() {
        log.info("Creating metadata tables");
        
        // 创建表信息表（sys_tables）
        Table tablesTable = Table.builder()
                .name(META_TABLES)
                .column(Column.builder()
                        .name("table_name")
                        .dataType(Column.DataType.VARCHAR)
                        .length(128)
                        .primaryKey(true)
                        .nullable(false)
                        .build())
                .column(Column.builder()
                        .name("page_count")
                        .dataType(Column.DataType.INT)
                        .nullable(false)
                        .defaultValue(0)
                        .build())
                .column(Column.builder()
                        .name("row_count")
                        .dataType(Column.DataType.BIGINT)
                        .nullable(false)
                        .defaultValue(0L)
                        .build())
                .column(Column.builder()
                        .name("create_time")
                        .dataType(Column.DataType.BIGINT)
                        .nullable(false)
                        .build())
                .column(Column.builder()
                        .name("last_modified_time")
                        .dataType(Column.DataType.BIGINT)
                        .nullable(false)
                        .build())
                .build();
        
        // 创建列信息表（sys_columns）
        Table columnsTable = Table.builder()
                .name(META_COLUMNS)
                .column(Column.builder()
                        .name("table_name")
                        .dataType(Column.DataType.VARCHAR)
                        .length(128)
                        .primaryKey(true)
                        .nullable(false)
                        .build())
                .column(Column.builder()
                        .name("column_name")
                        .dataType(Column.DataType.VARCHAR)
                        .length(128)
                        .primaryKey(true)
                        .nullable(false)
                        .build())
                .column(Column.builder()
                        .name("data_type")
                        .dataType(Column.DataType.VARCHAR)
                        .length(50)
                        .nullable(false)
                        .build())
                .column(Column.builder()
                        .name("length")
                        .dataType(Column.DataType.INT)
                        .nullable(false)
                        .defaultValue(0)
                        .build())
                .column(Column.builder()
                        .name("nullable")
                        .dataType(Column.DataType.BOOLEAN)
                        .nullable(false)
                        .defaultValue(false)
                        .build())
                .column(Column.builder()
                        .name("primary_key")
                        .dataType(Column.DataType.BOOLEAN)
                        .nullable(false)
                        .defaultValue(false)
                        .build())
                .column(Column.builder()
                        .name("unique_key")
                        .dataType(Column.DataType.BOOLEAN)
                        .nullable(false)
                        .defaultValue(false)
                        .build())
                .column(Column.builder()
                        .name("default_value")
                        .dataType(Column.DataType.VARCHAR)
                        .length(255)
                        .nullable(true)
                        .build())
                .column(Column.builder()
                        .name("check_constraint")
                        .dataType(Column.DataType.VARCHAR)
                        .length(255)
                        .nullable(true)
                        .build())
                .build();
        
        // 创建约束信息表（sys_constraints）
        Table constraintsTable = Table.builder()
                .name(META_CONSTRAINTS)
                .column(Column.builder()
                        .name("constraint_name")
                        .dataType(Column.DataType.VARCHAR)
                        .length(128)
                        .primaryKey(true)
                        .nullable(false)
                        .build())
                .column(Column.builder()
                        .name("table_name")
                        .dataType(Column.DataType.VARCHAR)
                        .length(128)
                        .nullable(false)
                        .build())
                .column(Column.builder()
                        .name("constraint_type")
                        .dataType(Column.DataType.VARCHAR)
                        .length(20)
                        .nullable(false)
                        .build())
                .column(Column.builder()
                        .name("columns")
                        .dataType(Column.DataType.VARCHAR)
                        .length(255)
                        .nullable(false)
                        .build())
                .column(Column.builder()
                        .name("ref_table")
                        .dataType(Column.DataType.VARCHAR)
                        .length(128)
                        .nullable(true)
                        .build())
                .column(Column.builder()
                        .name("ref_columns")
                        .dataType(Column.DataType.VARCHAR)
                        .length(255)
                        .nullable(true)
                        .build())
                .column(Column.builder()
                        .name("on_delete")
                        .dataType(Column.DataType.VARCHAR)
                        .length(20)
                        .nullable(true)
                        .build())
                .column(Column.builder()
                        .name("on_update")
                        .dataType(Column.DataType.VARCHAR)
                        .length(20)
                        .nullable(true)
                        .build())
                .build();
        
        // 创建统计信息表（sys_statistics）
        Table statisticsTable = Table.builder()
                .name(META_STATISTICS)
                .column(Column.builder()
                        .name("table_name")
                        .dataType(Column.DataType.VARCHAR)
                        .length(128)
                        .primaryKey(true)
                        .nullable(false)
                        .build())
                .column(Column.builder()
                        .name("column_name")
                        .dataType(Column.DataType.VARCHAR)
                        .length(128)
                        .primaryKey(true)
                        .nullable(false)
                        .build())
                .column(Column.builder()
                        .name("cardinality")
                        .dataType(Column.DataType.BIGINT)
                        .nullable(false)
                        .defaultValue(0L)
                        .build())
                .column(Column.builder()
                        .name("min_value")
                        .dataType(Column.DataType.VARCHAR)
                        .length(255)
                        .nullable(true)
                        .build())
                .column(Column.builder()
                        .name("max_value")
                        .dataType(Column.DataType.VARCHAR)
                        .length(255)
                        .nullable(true)
                        .build())
                .column(Column.builder()
                        .name("update_time")
                        .dataType(Column.DataType.BIGINT)
                        .nullable(false)
                        .build())
                .build();
        
        // 存储元数据表定义
        storeTableDefinition(tablesTable);
        storeTableDefinition(columnsTable);
        storeTableDefinition(constraintsTable);
        storeTableDefinition(statisticsTable);
    }
    
    /**
     * 存储表定义（仅用于元数据表）
     */
    private void storeTableDefinition(Table table) {
        tablesCache.put(table.getName(), table);
        columnsCache.put(table.getName(), table.getColumns());
        
        // 为元数据表创建初始页面
        // 在实际系统中，这里会调用PageManager创建物理页面
        // 此处简化处理
    }
    
    /**
     * 加载所有表的元数据到缓存
     */
    private void loadAllTablesMetadata() {
        // 在实际系统中，这里会从元数据表中读取所有表定义
        // 此处简化处理，只加载元数据表自身
        // 实际应用中需要从META_TABLES表中读取所有表记录，然后加载它们的元数据
    }
    
    @Override
    public List<String> getAllTableNames() {
        return new ArrayList<>(tablesCache.keySet());
    }
    
    @Override
    public List<Column> getColumns(String tableName) {
        if (!tableExists(tableName)) {
            throw new IllegalArgumentException("Table does not exist: " + tableName);
        }
        
        return new ArrayList<>(columnsCache.get(tableName));
    }
    
    @Override
    public List<String> getPrimaryKeys(String tableName) {
        if (!tableExists(tableName)) {
            throw new IllegalArgumentException("Table does not exist: " + tableName);
        }
        
        return columnsCache.get(tableName).stream()
                .filter(Column::isPrimaryKey)
                .map(Column::getName)
                .collect(Collectors.toList());
    }
    
    @Override
    public Map<String, Table.ForeignKey> getForeignKeys(String tableName) {
        if (!tableExists(tableName)) {
            throw new IllegalArgumentException("Table does not exist: " + tableName);
        }
        
        Table table = tablesCache.get(tableName);
        Map<String, Table.ForeignKey> result = new HashMap<>();
        
        if (table.getForeignKeys() != null) {
            for (Table.ForeignKey fk : table.getForeignKeys()) {
                result.put(fk.getName(), fk);
            }
        }
        
        return result;
    }
    
    @Override
    public List<String> getUniqueKeys(String tableName) {
        if (!tableExists(tableName)) {
            throw new IllegalArgumentException("Table does not exist: " + tableName);
        }
        
        return columnsCache.get(tableName).stream()
                .filter(Column::isUnique)
                .map(Column::getName)
                .collect(Collectors.toList());
    }
    
    @Override
    public List<String> getCheckConstraints(String tableName) {
        if (!tableExists(tableName)) {
            throw new IllegalArgumentException("Table does not exist: " + tableName);
        }
        
        Table table = tablesCache.get(tableName);
        return table.getCheckConstraints() != null ? 
                new ArrayList<>(table.getCheckConstraints()) : 
                new ArrayList<>();
    }
    
    @Override
    public void createTableMetadata(Table table, TransactionManager transactionManager) {
        if (tableExists(table.getName())) {
            throw new IllegalArgumentException("Table already exists: " + table.getName());
        }
        
        long xid = transactionManager.beginTransaction();
        
        try {
            // 更新缓存
            tablesCache.put(table.getName(), table);
            columnsCache.put(table.getName(), table.getColumns());
            
            // 在元数据表中创建记录
            // 1. 在 META_TABLES 表中插入表记录
            Record tableRecord = new Record();
            tableRecord.setFieldValue("table_name", table.getName());
            tableRecord.setFieldValue("page_count", table.getPageCount());
            tableRecord.setFieldValue("row_count", table.getRowCount());
            tableRecord.setFieldValue("create_time", table.getCreateTime());
            tableRecord.setFieldValue("last_modified_time", table.getLastModifiedTime());
            
            // 2. 在 META_COLUMNS 表中插入列记录
            for (Column column : table.getColumns()) {
                Record columnRecord = new Record();
                columnRecord.setFieldValue("table_name", table.getName());
                columnRecord.setFieldValue("column_name", column.getName());
                columnRecord.setFieldValue("data_type", column.getDataType().name());
                columnRecord.setFieldValue("length", column.getLength());
                columnRecord.setFieldValue("nullable", column.isNullable());
                columnRecord.setFieldValue("primary_key", column.isPrimaryKey());
                columnRecord.setFieldValue("unique_key", column.isUnique());
                columnRecord.setFieldValue("default_value", column.getDefaultValue() != null ? 
                        column.getDefaultValue().toString() : null);
                columnRecord.setFieldValue("check_constraint", column.getCheckConstraint());
            }
            
            // 3. 在 META_CONSTRAINTS 表中插入约束记录
            if (table.getForeignKeys() != null) {
                for (Table.ForeignKey fk : table.getForeignKeys()) {
                    Record constraintRecord = new Record();
                    constraintRecord.setFieldValue("constraint_name", fk.getName());
                    constraintRecord.setFieldValue("table_name", table.getName());
                    constraintRecord.setFieldValue("constraint_type", "FOREIGN KEY");
                    constraintRecord.setFieldValue("columns", String.join(",", fk.getColumns()));
                    constraintRecord.setFieldValue("ref_table", fk.getRefTable());
                    constraintRecord.setFieldValue("ref_columns", String.join(",", fk.getRefColumns()));
                    constraintRecord.setFieldValue("on_delete", fk.getOnDelete());
                    constraintRecord.setFieldValue("on_update", fk.getOnUpdate());
                }
            }
            
            // 4. 初始化统计信息
            TableStatisticsImpl statistics = new TableStatisticsImpl(table.getName());
            statistics.setRowCount(0);
            statistics.setPageCount(table.getPageCount());
            
            for (Column column : table.getColumns()) {
                statistics.setColumnCardinality(column.getName(), 0);
                statistics.setColumnMinValue(column.getName(), null);
                statistics.setColumnMaxValue(column.getName(), null);
            }
            
            statisticsCache.put(table.getName(), statistics);
            
            transactionManager.commitTransaction(xid);
        } catch (Exception e) {
            transactionManager.rollbackTransaction(xid);
            throw new RuntimeException("Failed to create table metadata: " + table.getName(), e);
        }
    }
    
    @Override
    public void dropTableMetadata(String tableName, TransactionManager transactionManager) {
        if (!tableExists(tableName)) {
            throw new IllegalArgumentException("Table does not exist: " + tableName);
        }
        
        long xid = transactionManager.beginTransaction();
        
        try {
            // 从元数据表中删除记录
            // 1. 从 META_TABLES 表中删除表记录
            // 2. 从 META_COLUMNS 表中删除列记录
            // 3. 从 META_CONSTRAINTS 表中删除约束记录
            // 4. 从 META_STATISTICS 表中删除统计信息
            
            // 更新缓存
            tablesCache.remove(tableName);
            columnsCache.remove(tableName);
            statisticsCache.remove(tableName);
            
            transactionManager.commitTransaction(xid);
        } catch (Exception e) {
            transactionManager.rollbackTransaction(xid);
            throw new RuntimeException("Failed to drop table metadata: " + tableName, e);
        }
    }
    
    @Override
    public void updateTableMetadata(Table table, TransactionManager transactionManager) {
        if (!tableExists(table.getName())) {
            throw new IllegalArgumentException("Table does not exist: " + table.getName());
        }
        
        long xid = transactionManager.beginTransaction();
        
        try {
            // 更新元数据表中的记录
            // 此处简化，实际中需要更新相关的元数据表记录
            
            // 更新缓存
            tablesCache.put(table.getName(), table);
            columnsCache.put(table.getName(), table.getColumns());
            
            transactionManager.commitTransaction(xid);
        } catch (Exception e) {
            transactionManager.rollbackTransaction(xid);
            throw new RuntimeException("Failed to update table metadata: " + table.getName(), e);
        }
    }
    
    @Override
    public boolean tableExists(String tableName) {
        return tablesCache.containsKey(tableName);
    }
    
    @Override
    public Table getTableMetadata(String tableName) {
        if (!tableExists(tableName)) {
            throw new IllegalArgumentException("Table does not exist: " + tableName);
        }
        
        return tablesCache.get(tableName);
    }
    
    @Override
    public TableStatistics getTableStatistics(String tableName) {
        if (!tableExists(tableName)) {
            throw new IllegalArgumentException("Table does not exist: " + tableName);
        }
        
        return statisticsCache.getOrDefault(tableName, 
                new TableStatisticsImpl(tableName));
    }
    
    /**
     * 表统计信息实现类
     */
    private static class TableStatisticsImpl implements TableStatistics {
        private final String tableName;
        private long rowCount;
        private int pageCount;
        private long size;
        private final Map<String, Long> columnCardinality = new HashMap<>();
        private final Map<String, Object> columnMinValue = new HashMap<>();
        private final Map<String, Object> columnMaxValue = new HashMap<>();
        
        public TableStatisticsImpl(String tableName) {
            this.tableName = tableName;
        }
        
        @Override
        public long getRowCount() {
            return rowCount;
        }
        
        public void setRowCount(long rowCount) {
            this.rowCount = rowCount;
        }
        
        @Override
        public int getPageCount() {
            return pageCount;
        }
        
        public void setPageCount(int pageCount) {
            this.pageCount = pageCount;
        }
        
        @Override
        public long getSize() {
            return size;
        }
        
        public void setSize(long size) {
            this.size = size;
        }
        
        @Override
        public long getColumnCardinality(String columnName) {
            return columnCardinality.getOrDefault(columnName, 0L);
        }
        
        public void setColumnCardinality(String columnName, long cardinality) {
            columnCardinality.put(columnName, cardinality);
        }
        
        @Override
        public Object getColumnMinValue(String columnName) {
            return columnMinValue.get(columnName);
        }
        
        public void setColumnMinValue(String columnName, Object minValue) {
            columnMinValue.put(columnName, minValue);
        }
        
        @Override
        public Object getColumnMaxValue(String columnName) {
            return columnMaxValue.get(columnName);
        }
        
        public void setColumnMaxValue(String columnName, Object maxValue) {
            columnMaxValue.put(columnName, maxValue);
        }
    }
} 