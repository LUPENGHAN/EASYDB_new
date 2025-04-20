package DB.index.interfaces;

import DB.page.models.Page;
import DB.record.models.Record;
import DB.transaction.interfaces.TransactionManager;

import java.util.List;

/**
 * 索引管理器接口
 */
public interface IndexManager {
    /**
     * 创建索引
     * @param tableName 表名
     * @param columnName 列名
     * @param indexName 索引名
     * @param transactionManager 事务管理器
     */
    void createIndex(String tableName, String columnName, String indexName, TransactionManager transactionManager);

    /**
     * 删除索引
     * @param indexName 索引名
     * @param transactionManager 事务管理器
     */
    void dropIndex(String indexName, TransactionManager transactionManager);

    /**
     * 插入索引项
     * @param indexName 索引名
     * @param key 键值
     * @param record 记录
     * @param transactionManager 事务管理器
     */
    void insertIndex(String indexName, Object key, Record record, TransactionManager transactionManager);

    /**
     * 删除索引项
     * @param indexName 索引名
     * @param key 键值
     * @param record 记录
     * @param transactionManager 事务管理器
     */
    void deleteIndex(String indexName, Object key, Record record, TransactionManager transactionManager);

    /**
     * 范围查询
     * @param indexName 索引名
     * @param minKey 最小键值
     * @param maxKey 最大键值
     * @param transactionManager 事务管理器
     * @return 记录列表
     */
    List<Record> rangeQuery(String indexName, Object minKey, Object maxKey, TransactionManager transactionManager);

    /**
     * 精确查询
     * @param indexName 索引名
     * @param key 键值
     * @param transactionManager 事务管理器
     * @return 记录列表
     */
    List<Record> exactQuery(String indexName, Object key, TransactionManager transactionManager);

    /**
     * 获取索引的根页面
     * @param indexName 索引名
     * @return 根页面
     */
    Page getIndexRoot(String indexName);

    /**
     * 更新索引
     * @param indexName 索引名
     * @param oldKey 旧键值
     * @param newKey 新键值
     * @param record 记录
     * @param transactionManager 事务管理器
     */
    void updateIndex(String indexName, Object oldKey, Object newKey, Record record, TransactionManager transactionManager);
} 