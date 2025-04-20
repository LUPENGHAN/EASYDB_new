package DB.query.interfaces;

import DB.index.interfaces.IndexManager;
import DB.record.models.Record;
import DB.transaction.interfaces.TransactionManager;

import java.util.List;
import java.util.Map;

/**
 * 数据库查询组件接口集合
 * 整合查询相关的所有接口定义
 */
public class QueryComponents {

    /**
     * 查询类型枚举
     */
    public enum QueryType {
        /** 查询数据 */
        SELECT,
        /** 插入数据 */
        INSERT,
        /** 更新数据 */
        UPDATE,
        /** 删除数据 */
        DELETE
    }

    /**
     * 查询服务接口
     * 作为数据库查询功能的统一入口，整合解析、优化和执行
     */
    public interface QueryService {
        /**
         * 执行SQL查询
         * @param sql SQL查询语句
         * @return 执行结果（SELECT语句返回结果集，其他语句返回空列表）
         */
        List<Record> executeQuery(String sql);

        /**
         * 执行更新操作
         * @param sql SQL更新语句（INSERT, UPDATE, DELETE）
         * @return 影响的行数
         */
        int executeUpdate(String sql);

        /**
         * 创建预编译查询
         * @param sql 包含参数占位符的SQL语句
         * @return 预编译查询对象
         */
        PreparedQuery prepareQuery(String sql);
    }

    /**
     * 查询执行器接口
     * 负责执行查询计划并返回结果
     */
    public interface QueryExecutor {
        /**
         * 执行查询计划
         * @param plan 查询计划
         * @return 查询结果记录列表
         */
        List<Record> executeQuery(QueryPlan plan);

        /**
         * 执行SELECT查询计划
         * @param plan SELECT查询计划
         * @return 查询结果记录列表
         */
        List<Record> executeSelectQuery(QueryPlan plan);

        /**
         * 执行INSERT查询计划
         * @param plan INSERT查询计划
         * @return 影响的记录列表
         */
        List<Record> executeInsertQuery(QueryPlan plan);

        /**
         * 执行UPDATE查询计划
         * @param plan UPDATE查询计划
         * @return 更新后的记录列表
         */
        List<Record> executeUpdateQuery(QueryPlan plan);

        /**
         * 执行DELETE查询计划
         * @param plan DELETE查询计划
         * @return 删除的记录列表
         */
        List<Record> executeDeleteQuery(QueryPlan plan);
    }

    /**
     * 扩展的查询执行器接口
     * 增加了直接执行SQL字符串的能力
     */
    public interface ExtendedQueryExecutor extends QueryExecutor {
        /**
         * 直接执行SQL查询
         * @param sql SQL查询语句
         * @return 查询结果记录列表
         */
        List<Record> executeQuery(String sql);

        /**
         * 执行SQL更新操作
         * @param sql SQL更新语句
         * @return 影响的行数
         */
        int executeUpdate(String sql);
    }

    /**
     * 查询优化器接口
     * 负责分析和优化SQL查询，生成高效的查询计划
     */
    public interface QueryOptimizer {
        /**
         * 优化查询并生成查询计划
         * @param query SQL查询字符串
         * @param indexManager 索引管理器，用于查找适用的索引
         * @param transactionManager 事务管理器，用于执行查询的事务上下文
         * @return 优化后的查询计划
         */
        QueryPlan optimizeQuery(String query, IndexManager indexManager, TransactionManager transactionManager);
    }

    /**
     * 查询计划接口
     * 封装了执行查询所需的所有信息
     */
    public interface QueryPlan {
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

        /**
         * 获取元数据（原始SQL查询语句）
         */
        String getMetadata();
    }

    /**
     * SQL查询解析器接口
     * 负责将SQL查询字符串解析为结构化数据
     */
    public interface QueryParser {
        /**
         * 确定查询类型
         * @param query SQL查询字符串
         * @return 查询类型
         */
        QueryType determineQueryType(String query);

        /**
         * 解析SELECT查询
         * @param query SELECT查询字符串
         * @return 包含表名、列名和条件的解析结果
         */
        SelectQueryData parseSelectQuery(String query);

        /**
         * 解析INSERT查询
         * @param query INSERT查询字符串
         * @return 包含表名、列名和值的解析结果
         */
        InsertQueryData parseInsertQuery(String query);

        /**
         * 解析UPDATE查询
         * @param query UPDATE查询字符串
         * @return 包含表名、更新内容和条件的解析结果
         */
        UpdateQueryData parseUpdateQuery(String query);

        /**
         * 解析DELETE查询
         * @param query DELETE查询字符串
         * @return 包含表名和条件的解析结果
         */
        DeleteQueryData parseDeleteQuery(String query);

        /**
         * 解析条件表达式
         * @param condition 条件表达式字符串
         * @return 解析后的条件数据
         */
        ConditionData parseCondition(String condition);

        /**
         * SELECT查询数据类
         */
        class SelectQueryData {
            private final String tableName;
            private final List<String> columns;
            private final String condition;

            public SelectQueryData(String tableName, List<String> columns, String condition) {
                this.tableName = tableName;
                this.columns = columns;
                this.condition = condition;
            }

            public String getTableName() { return tableName; }
            public List<String> getColumns() { return columns; }
            public String getCondition() { return condition; }
        }

        /**
         * INSERT查询数据类
         */
        class InsertQueryData {
            private final String tableName;
            private final List<String> columns;
            private final List<String> values;

            public InsertQueryData(String tableName, List<String> columns, List<String> values) {
                this.tableName = tableName;
                this.columns = columns;
                this.values = values;
            }

            public String getTableName() { return tableName; }
            public List<String> getColumns() { return columns; }
            public List<String> getValues() { return values; }
        }

        /**
         * UPDATE查询数据类
         */
        class UpdateQueryData {
            private final String tableName;
            private final Map<String, String> setValues;
            private final String condition;

            public UpdateQueryData(String tableName, Map<String, String> setValues, String condition) {
                this.tableName = tableName;
                this.setValues = setValues;
                this.condition = condition;
            }

            public String getTableName() { return tableName; }
            public Map<String, String> getSetValues() { return setValues; }
            public String getCondition() { return condition; }
        }

        /**
         * DELETE查询数据类
         */
        class DeleteQueryData {
            private final String tableName;
            private final String condition;

            public DeleteQueryData(String tableName, String condition) {
                this.tableName = tableName;
                this.condition = condition;
            }

            public String getTableName() { return tableName; }
            public String getCondition() { return condition; }
        }

        /**
         * 条件数据类
         */
        class ConditionData {
            private final String column;
            private final String operator;
            private final String value;

            public ConditionData(String column, String operator, String value) {
                this.column = column;
                this.operator = operator;
                this.value = value;
            }

            public String getColumn() { return column; }
            public String getOperator() { return operator; }
            public String getValue() { return value; }
        }
    }

    /**
     * 预编译查询接口
     * 支持参数化SQL查询，提高查询效率和安全性
     */
    public interface PreparedQuery {
        /**
         * 执行预编译查询
         * @param params 查询参数
         * @param txManager 事务管理器
         * @return 查询结果记录列表
         */
        List<Record> execute(Object[] params, TransactionManager txManager);

        /**
         * 执行预编译更新
         * @param params 查询参数
         * @param txManager 事务管理器
         * @return 影响的行数
         */
        int executeUpdate(Object[] params, TransactionManager txManager);

        /**
         * 关闭预编译查询
         * 释放相关资源
         */
        void close();
    }
}