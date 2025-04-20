package DB.table.interfaces;

import DB.table.models.Column;
import DB.table.models.Table;
import DB.transaction.interfaces.TransactionManager;

import java.util.List;
import java.util.Map;

/**
 * 表元数据管理器接口
 */
public interface TableMetadataManager {
    /**
     * 获取所有表名
     * @return 表名列表
     */
    List<String> getAllTableNames();
    
    /**
     * 获取表的所有列
     * @param tableName 表名
     * @return 列列表
     */
    List<Column> getColumns(String tableName);
    
    /**
     * 获取表的主键列
     * @param tableName 表名
     * @return 主键列列表
     */
    List<String> getPrimaryKeys(String tableName);
    
    /**
     * 获取表的外键
     * @param tableName 表名
     * @return 外键映射（外键名 -> 引用表和列）
     */
    Map<String, Table.ForeignKey> getForeignKeys(String tableName);
    
    /**
     * 获取表的唯一键
     * @param tableName 表名
     * @return 唯一键列表
     */
    List<String> getUniqueKeys(String tableName);
    
    /**
     * 获取表的检查约束
     * @param tableName 表名
     * @return 检查约束列表
     */
    List<String> getCheckConstraints(String tableName);
    
    /**
     * 创建表元数据
     * @param table 表定义
     * @param transactionManager 事务管理器
     */
    void createTableMetadata(Table table, TransactionManager transactionManager);
    
    /**
     * 删除表元数据
     * @param tableName 表名
     * @param transactionManager 事务管理器
     */
    void dropTableMetadata(String tableName, TransactionManager transactionManager);
    
    /**
     * 更新表元数据
     * @param table 表定义
     * @param transactionManager 事务管理器
     */
    void updateTableMetadata(Table table, TransactionManager transactionManager);
    
    /**
     * 检查表是否存在
     * @param tableName 表名
     * @return 是否存在
     */
    boolean tableExists(String tableName);
    
    /**
     * 获取表定义
     * @param tableName 表名
     * @return 表定义
     */
    Table getTableMetadata(String tableName);
    
    /**
     * 获取表的统计信息
     * @param tableName 表名
     * @return 表统计信息
     */
    TableStatistics getTableStatistics(String tableName);
    
    /**
     * 表统计信息
     */
    interface TableStatistics {
        /**
         * 获取表的行数
         * @return 行数
         */
        long getRowCount();
        
        /**
         * 获取表的页面数
         * @return 页面数
         */
        int getPageCount();
        
        /**
         * 获取表的大小（字节数）
         * @return 大小
         */
        long getSize();
        
        /**
         * 获取列的基数（不同值的数量）
         * @param columnName 列名
         * @return 基数
         */
        long getColumnCardinality(String columnName);
        
        /**
         * 获取列的最小值
         * @param columnName 列名
         * @return 最小值
         */
        Object getColumnMinValue(String columnName);
        
        /**
         * 获取列的最大值
         * @param columnName 列名
         * @return 最大值
         */
        Object getColumnMaxValue(String columnName);
    }
} 