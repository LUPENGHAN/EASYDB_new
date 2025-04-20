package DB.query.impl;

import DB.index.interfaces.IndexManager;
import DB.query.interfaces.QueryOptimizer;
import DB.transaction.interfaces.TransactionManager;

import java.util.List;

public class QueryPlanImpl implements QueryOptimizer.QueryPlan {
    private final QueryOptimizer.QueryType queryType;
    private final String tableName;
    private final String condition;
    private final List<String> selectedColumns;
    private final String indexName;
    private final IndexManager indexManager;
    private final TransactionManager transactionManager;

    public QueryPlanImpl(QueryOptimizer.QueryType queryType, String tableName, String condition,
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
    public QueryOptimizer.QueryType getQueryType() {
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