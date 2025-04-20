package DB.table.interfaces;

import DB.page.interfaces.PageManager;
import DB.table.models.Table;
import DB.transaction.interfaces.TransactionManager;

import java.util.List;

/**
 * 表管理器接口
 */
public interface TableManager {
    /**
     * 创建表
     * @param table 表定义
     * @param transactionManager 事务管理器
     */
    void createTable(Table table, TransactionManager transactionManager);

    /**
     * 删除表
     * @param tableName 表名
     * @param transactionManager 事务管理器
     */
    void dropTable(String tableName, TransactionManager transactionManager);

    /**
     * 修改表结构
     * @param tableName 表名
     * @param alterType 修改类型
     * @param alterDefinition 修改定义
     * @param transactionManager 事务管理器
     */
    void alterTable(String tableName, String alterType, String alterDefinition, TransactionManager transactionManager);

    /**
     * 获取表定义
     * @param tableName 表名
     * @return 表定义
     */
    Table getTable(String tableName);

    /**
     * 获取所有表名
     * @return 表名列表
     */
    List<String> getAllTables();

    /**
     * 获取页面管理器
     * @return 页面管理器
     */
    PageManager getPageManager();
} 