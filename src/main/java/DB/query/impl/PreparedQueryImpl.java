package DB.query.impl;

import DB.query.interfaces.PreparedQuery;
import DB.query.interfaces.QueryOptimizer;
import DB.query.interfaces.QueryExecutor;
import DB.record.models.Record;
import DB.transaction.interfaces.TransactionManager;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 预编译查询实现类
 */
@Slf4j
public class PreparedQueryImpl implements PreparedQuery {
    private final String sql;
    private final QueryOptimizer queryOptimizer;
    private final QueryExecutor queryExecutor;
    private boolean closed = false;
    private static final Pattern PARAM_PATTERN = Pattern.compile("\\?");

    public PreparedQueryImpl(String sql, QueryOptimizer queryOptimizer, QueryExecutor queryExecutor) {
        this.sql = sql;
        this.queryOptimizer = queryOptimizer;
        this.queryExecutor = queryExecutor;
    }

    @Override
    public List<Record> execute(Object[] params, TransactionManager transactionManager) {
        if (closed) {
            throw new IllegalStateException("PreparedQuery is closed");
        }
        
        try {
            String finalSql = replaceParameters(sql, params);
            log.debug("Executing prepared query: {}", finalSql);
            return queryExecutor.execute(finalSql, transactionManager);
        } catch (Exception e) {
            log.error("Error executing prepared query: {}", sql, e);
            throw new RuntimeException("Failed to execute prepared query", e);
        }
    }

    @Override
    public int executeUpdate(Object[] params, TransactionManager transactionManager) {
        if (closed) {
            throw new IllegalStateException("PreparedQuery is closed");
        }
        
        try {
            String finalSql = replaceParameters(sql, params);
            log.debug("Executing prepared update: {}", finalSql);
            return queryExecutor.executeUpdate(finalSql, transactionManager);
        } catch (Exception e) {
            log.error("Error executing prepared update: {}", sql, e);
            throw new RuntimeException("Failed to execute prepared update", e);
        }
    }

    @Override
    public void close() {
        closed = true;
    }

    private String replaceParameters(String sql, Object[] params) {
        if (params == null || params.length == 0) {
            return sql;
        }

        Matcher matcher = PARAM_PATTERN.matcher(sql);
        StringBuffer sb = new StringBuffer();
        int paramIndex = 0;

        while (matcher.find() && paramIndex < params.length) {
            Object param = params[paramIndex++];
            String replacement = formatParameter(param);
            matcher.appendReplacement(sb, replacement);
        }
        matcher.appendTail(sb);

        if (paramIndex < params.length) {
            log.warn("More parameters provided than placeholders in SQL: {}", sql);
        }

        return sb.toString();
    }

    private String formatParameter(Object param) {
        if (param == null) {
            return "NULL";
        }
        if (param instanceof String) {
            return "'" + param.toString().replace("'", "''") + "'";
        }
        if (param instanceof Number) {
            return param.toString();
        }
        if (param instanceof Boolean) {
            return param.toString();
        }
        return "'" + param.toString().replace("'", "''") + "'";
    }
} 