package DB.query.interfaces;

import DB.index.interfaces.IndexManager;
import DB.record.models.Record;
import DB.transaction.interfaces.TransactionManager;

import java.util.List;

/**
 * 查询优化器接口
 */
public interface QueryOptimizer {
    /**
     * 优化查询并生成查询计划
     */
    QueryPlan optimizeQuery(String query, IndexManager indexManager, TransactionManager transactionManager);

    /**
     * 执行查询计划
     */
    List<Record> executeQuery(QueryPlan plan);

    /**
     * 查询计划接口
     */
    interface QueryPlan {
        /**
         * 获取查询类型
         */
        QueryType getQueryType();

        /**
         * 获取表名
         */
        String getTableName();

        /**
         * 获取查询条件
         */
        String getCondition();

        /**
         * 获取选择的列
         */
        List<String> getSelectedColumns();

        /**
         * 获取使用的索引名
         */
        String getIndexName();

        /**
         * 获取索引管理器
         */
        IndexManager getIndexManager();

        /**
         * 获取事务管理器
         */
        TransactionManager getTransactionManager();
    }

    /**
     * 查询类型枚举
     */
    enum QueryType {
        SELECT,
        INSERT,
        UPDATE,
        DELETE
    }
} 