package DB.query.impl;

import DB.query.interfaces.QueryComponents.ExtendedQueryExecutor;
import DB.query.interfaces.QueryComponents.PreparedQuery;
import DB.query.interfaces.QueryComponents.QueryType;
import DB.record.models.Record;
import DB.transaction.interfaces.TransactionManager;
import lombok.extern.slf4j.Slf4j;

import java.util.List;

/**
 * 预编译查询实现
 */
@Slf4j
public class PreparedQueryImpl implements PreparedQuery {
    private final String sql;
    private final ExtendedQueryExecutor queryExecutor;
    private final int paramCount;
    private final QueryType queryType;

    /**
     * 构造函数
     * @param sql 带参数占位符的SQL
     * @param queryExecutor 查询执行器
     */
    public PreparedQueryImpl(String sql, ExtendedQueryExecutor queryExecutor) {
        this(sql, null, queryExecutor);
    }

    /**
     * 带查询类型的构造函数
     * @param sql 带参数占位符的SQL
     * @param queryType 查询类型
     * @param queryExecutor 查询执行器
     */
    public PreparedQueryImpl(String sql, QueryType queryType, ExtendedQueryExecutor queryExecutor) {
        this.sql = sql;
        this.queryExecutor = queryExecutor;
        this.queryType = queryType;
        this.paramCount = SqlParameterUtils.countParams(sql);
    }

    @Override
    public List<Record> execute(Object[] params, TransactionManager transactionManager) {
        if (params.length != paramCount) {
            throw new IllegalArgumentException("预期 " + paramCount + " 个参数，但是收到 " + params.length + " 个参数");
        }

        // 替换参数占位符
        String finalSql = SqlParameterUtils.applyParams(sql, params);

        // 执行查询
        try {
            return queryExecutor.executeQuery(finalSql);
        } catch (Exception e) {
            log.error("执行查询失败: {}", finalSql, e);
            throw new RuntimeException("查询执行失败", e);
        }
    }

    @Override
    public int executeUpdate(Object[] params, TransactionManager transactionManager) {
        if (queryType != null && queryType == QueryType.SELECT) {
            throw new IllegalArgumentException("不能使用executeUpdate方法执行SELECT查询");
        }

        if (params.length != paramCount) {
            throw new IllegalArgumentException("预期 " + paramCount + " 个参数，但是收到 " + params.length + " 个参数");
        }

        // 替换参数占位符
        String finalSql = SqlParameterUtils.applyParams(sql, params);

        // 执行更新
        try {
            return queryExecutor.executeUpdate(finalSql);
        } catch (Exception e) {
            log.error("执行更新失败: {}", finalSql, e);
            throw new RuntimeException("更新执行失败", e);
        }
    }

    @Override
    public void close() {
        // 释放资源
        log.debug("关闭预编译查询");
    }
}