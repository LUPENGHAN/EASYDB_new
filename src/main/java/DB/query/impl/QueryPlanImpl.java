package DB.query.impl;

import DB.index.interfaces.IndexManager;
import DB.query.interfaces.QueryComponents.QueryPlan;
import DB.query.interfaces.QueryComponents.QueryType;
import DB.transaction.interfaces.TransactionManager;

import java.util.List;

/**
 * 查询计划实现类
 * 封装了执行查询所需的所有信息
 */
public class QueryPlanImpl implements QueryPlan {
    private final QueryType queryType;
    private final String tableName;
    private final String condition;
    private final List<String> selectedColumns;
    private final String indexName;
    private final IndexManager indexManager;
    private final TransactionManager transactionManager;
    private final String metadata; // 存储原始查询字符串

    public QueryPlanImpl(
            QueryType queryType,
            String tableName,
            String condition,
            List<String> selectedColumns,
            String indexName,
            IndexManager indexManager,
            TransactionManager transactionManager,
            String metadata) {
        this.queryType = queryType;
        this.tableName = tableName;
        this.condition = condition;
        this.selectedColumns = selectedColumns;
        this.indexName = indexName;
        this.indexManager = indexManager;
        this.transactionManager = transactionManager;
        this.metadata = metadata;
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

    @Override
    public String getMetadata() {
        return metadata;
    }
}