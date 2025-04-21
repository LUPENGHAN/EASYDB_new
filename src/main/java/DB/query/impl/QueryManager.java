package DB.query.impl;

import DB.index.interfaces.IndexManager;
import DB.query.interfaces.QueryComponents.*;
import DB.record.models.Record;
import DB.table.interfaces.TableManager;
import DB.transaction.interfaces.TransactionManager;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 查询管理器
 * 统一实现查询服务接口，作为数据库查询功能的入口点
 */
@Slf4j
public class QueryManager implements QueryService {
    /**
     * -- GETTER --
     *  获取查询核心
     *
     * @return 查询核心
     */
    @Getter
    private final QueryCore queryCore;
    private final Map<String, PreparedQuery> preparedQueries = new ConcurrentHashMap<>();

    /**
     * 构造查询管理器
     *
     * @param tableManager 表管理器
     * @param indexManager 索引管理器
     * @param transactionManager 事务管理器
     */
    public QueryManager(TableManager tableManager, IndexManager indexManager, TransactionManager transactionManager) {
        // 创建查询解析器
        QueryParser queryParser = new QueryParserImpl();

        // 创建查询核心
        this.queryCore = new QueryCore(tableManager, indexManager, queryParser, transactionManager);
    }


    @Override
    public List<Record> executeQuery(String sql) {
        try {
            return queryCore.executeQuery(sql);
        } catch (Exception e) {
            log.error("执行查询失败: {}", sql, e);
            // 确保异常消息中包含表不存在信息
            String errorMsg = (e.getMessage() != null && !e.getMessage().isEmpty())
                    ? e.getMessage()
                    : "表不存在或操作无效";
            throw new RuntimeException("执行查询失败: " + errorMsg, e);
        }
    }
    @Override
    public int executeUpdate(String sql) {
        try {
            return queryCore.executeUpdate(sql);
        } catch (Exception e) {
            log.error("执行更新失败: {}", sql, e);
            String errorMsg = (e.getMessage() != null && !e.getMessage().isEmpty())
                    ? e.getMessage()
                    : "表不存在或操作无效";
            throw new RuntimeException("执行更新失败: " + errorMsg, e);
        }
    }

    @Override
    public PreparedQuery prepareQuery(String sql) {
        // 检查是否已存在预编译查询
        return preparedQueries.computeIfAbsent(sql, key -> {
            // 解析SQL语句以确定查询类型
            QueryType queryType = ((QueryParserImpl)queryCore.queryParser).determineQueryType(key);

            // 创建预编译查询
            return new PreparedQueryImpl(key, queryType, queryCore);
        });
    }

    /**
     * 创建数据库客户端
     * @return 数据库客户端
     */
    public DatabaseClient createClient() {
        return new DatabaseClient(queryCore, queryCore.transactionManager);
    }
}