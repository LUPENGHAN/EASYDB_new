package DB.query.impl;

import DB.query.interfaces.PreparedQuery;
import DB.query.interfaces.QueryExecutor;
import DB.record.models.Record;
import DB.transaction.interfaces.TransactionManager;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 预编译查询实现
 */
@Slf4j
public class PreparedQueryImpl implements PreparedQuery {
    private final String sql;
    private final QueryExecutor queryExecutor;
    private final Object[] params;
    private final int paramCount;
    
    // 参数占位符正则表达式
    private static final Pattern PARAM_PATTERN = Pattern.compile("\\?");

    /**
     * 构造函数
     * @param sql 带参数占位符的SQL
     * @param queryExecutor 查询执行器
     */
    public PreparedQueryImpl(String sql, QueryExecutor queryExecutor) {
        this.sql = sql;
        this.queryExecutor = queryExecutor;
        this.paramCount = countParams(sql);
        this.params = new Object[paramCount];
    }

    /**
     * 计算SQL中的参数占位符数量
     * @param sql SQL语句
     * @return 参数数量
     */
    private int countParams(String sql) {
        Matcher matcher = PARAM_PATTERN.matcher(sql);
        int count = 0;
        while (matcher.find()) {
            count++;
        }
        return count;
    }

    @Override
    public List<Record> execute(Object[] params, TransactionManager transactionManager) {
        if (params.length != paramCount) {
            throw new IllegalArgumentException("Expected " + paramCount + " parameters, but got " + params.length);
        }
        
        // 替换参数占位符
        String finalSql = replacePlaceholders(sql, params);
        
        // 执行查询
        try {
            return queryExecutor.executeQuery(finalSql);
        } catch (Exception e) {
            log.error("Failed to execute query: {}", finalSql, e);
            throw new RuntimeException("Query execution failed", e);
        }
    }

    @Override
    public int executeUpdate(Object[] params, TransactionManager transactionManager) {
        if (params.length != paramCount) {
            throw new IllegalArgumentException("Expected " + paramCount + " parameters, but got " + params.length);
        }
        
        // 替换参数占位符
        String finalSql = replacePlaceholders(sql, params);
        
        // 执行更新
        try {
            // 执行查询（对于UPDATE/INSERT/DELETE会返回空记录列表）
            queryExecutor.executeQuery(finalSql);
            // 这里应该返回实际影响的行数，但现在简化处理
            return 1;
        } catch (Exception e) {
            log.error("Failed to execute update: {}", finalSql, e);
            throw new RuntimeException("Update execution failed", e);
        }
    }

    @Override
    public void close() {
        // 释放资源
        log.debug("Closing prepared query");
    }

    /**
     * 替换SQL中的参数占位符
     * @param sql SQL语句
     * @param params 参数值
     * @return 替换后的SQL
     */
    private String replacePlaceholders(String sql, Object[] params) {
        StringBuilder result = new StringBuilder();
        Matcher matcher = PARAM_PATTERN.matcher(sql);
        int lastEnd = 0;
        int paramIndex = 0;
        
        while (matcher.find() && paramIndex < params.length) {
            result.append(sql, lastEnd, matcher.start());
            Object param = params[paramIndex++];
            
            // 根据参数类型格式化
            if (param == null) {
                result.append("NULL");
            } else if (param instanceof String) {
                result.append("'").append(param).append("'");
            } else {
                result.append(param);
            }
            
            lastEnd = matcher.end();
        }
        
        result.append(sql.substring(lastEnd));
        return result.toString();
    }
} 