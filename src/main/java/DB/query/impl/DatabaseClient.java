package DB.query.impl;

import DB.query.interfaces.QueryComponents.*;
import DB.record.models.Record;
import DB.transaction.interfaces.TransactionManager;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 数据库客户端
 * 提供用户友好的查询接口
 */
@Slf4j
@RequiredArgsConstructor
public class DatabaseClient {
    private final ExtendedQueryExecutor queryExecutor;
    private final TransactionManager transactionManager;
    private final Map<String, PreparedQuery> preparedQueries = new HashMap<>();

    /**
     * 执行查询
     * @param sql SQL语句
     * @return 结果集
     */
    public ResultSet executeQuery(String sql) {
        try {
            List<Record> records = queryExecutor.executeQuery(sql);
            return new ResultSet(records);
        } catch (Exception e) {
            log.error("执行查询出错: {}", sql, e);
            throw new RuntimeException("查询执行失败", e);
        }
    }

    /**
     * 执行更新操作
     * @param sql SQL语句
     * @return 影响的行数
     */
    public int executeUpdate(String sql) {
        try {
            return queryExecutor.executeUpdate(sql);
        } catch (Exception e) {
            log.error("执行更新出错: {}", sql, e);
            throw new RuntimeException("更新执行失败", e);
        }
    }

    /**
     * 准备查询
     * @param sql SQL语句
     * @return 预编译查询
     */
    public PreparedStatement prepareStatement(String sql) {
        try {
            if (preparedQueries.containsKey(sql)) {
                return new PreparedStatement(preparedQueries.get(sql), transactionManager);
            }

            PreparedQuery query = new PreparedQueryImpl(sql, queryExecutor);
            preparedQueries.put(sql, query);
            return new PreparedStatement(query, transactionManager);
        } catch (Exception e) {
            log.error("准备语句出错: {}", sql, e);
            throw new RuntimeException("语句准备失败", e);
        }
    }

    /**
     * 开始事务
     * @return 事务ID
     */
    public long beginTransaction() {
        return transactionManager.beginTransaction();
    }

    /**
     * 提交事务
     * @param transactionId 事务ID
     */
    public void commitTransaction(long transactionId) {
        transactionManager.commitTransaction(transactionId);
    }

    /**
     * 回滚事务
     * @param transactionId 事务ID
     */
    public void rollbackTransaction(long transactionId) {
        transactionManager.rollbackTransaction(transactionId);
    }

    /**
     * 关闭客户端
     */
    public void close() {
        for (PreparedQuery query : preparedQueries.values()) {
            try {
                query.close();
            } catch (Exception e) {
                log.warn("关闭预编译查询出错", e);
            }
        }
        preparedQueries.clear();
    }

    /**
     * 预编译语句类
     */
    public static class PreparedStatement {
        private final PreparedQuery query;
        private final TransactionManager transactionManager;
        private final Object[] params;
        private int paramIndex = 0;

        public PreparedStatement(PreparedQuery query, TransactionManager transactionManager) {
            this.query = query;
            this.transactionManager = transactionManager;
            this.params = new Object[20]; // 假设最多20个参数
        }

        /**
         * 设置整数参数
         * @param index 参数索引（从1开始）
         * @param value 参数值
         */
        public void setInt(int index, int value) {
            params[index - 1] = value;
            paramIndex = Math.max(paramIndex, index);
        }

        /**
         * 设置长整数参数
         * @param index 参数索引（从1开始）
         * @param value 参数值
         */
        public void setLong(int index, long value) {
            params[index - 1] = value;
            paramIndex = Math.max(paramIndex, index);
        }

        /**
         * 设置字符串参数
         * @param index 参数索引（从1开始）
         * @param value 参数值
         */
        public void setString(int index, String value) {
            params[index - 1] = value;
            paramIndex = Math.max(paramIndex, index);
        }

        /**
         * 设置布尔参数
         * @param index 参数索引（从1开始）
         * @param value 参数值
         */
        public void setBoolean(int index, boolean value) {
            params[index - 1] = value;
            paramIndex = Math.max(paramIndex, index);
        }

        /**
         * 设置任意对象参数
         * @param index 参数索引（从1开始）
         * @param value 参数值
         */
        public void setObject(int index, Object value) {
            params[index - 1] = value;
            paramIndex = Math.max(paramIndex, index);
        }

        /**
         * 执行查询
         * @return 结果集
         */
        public ResultSet executeQuery() {
            try {
                Object[] actualParams = new Object[paramIndex];
                System.arraycopy(params, 0, actualParams, 0, paramIndex);
                List<Record> records = query.execute(actualParams, transactionManager);
                return new ResultSet(records);
            } catch (Exception e) {
                log.error("执行预编译查询出错", e);
                throw new RuntimeException("预编译查询执行失败", e);
            }
        }

        /**
         * 执行更新
         * @return 影响的行数
         */
        public int executeUpdate() {
            try {
                Object[] actualParams = new Object[paramIndex];
                System.arraycopy(params, 0, actualParams, 0, paramIndex);
                return query.executeUpdate(actualParams, transactionManager);
            } catch (Exception e) {
                log.error("执行预编译更新出错", e);
                throw new RuntimeException("预编译更新执行失败", e);
            }
        }

        /**
         * 关闭语句
         */
        public void close() {
            try {
                // 这里不关闭PreparedQuery，因为它可能被其他PreparedStatement共享
            } catch (Exception e) {
                log.warn("关闭语句出错", e);
            }
        }
    }

    /**
     * 结果集类
     */
    public static class ResultSet {
        private final List<Record> records;
        private int currentIndex = -1;
        private Record currentRecord;

        public ResultSet(List<Record> records) {
            this.records = records != null ? records : List.of();
        }

        /**
         * 移动到下一行
         * @return 是否有下一行
         */
        public boolean next() {
            if (currentIndex + 1 < records.size()) {
                currentIndex++;
                currentRecord = records.get(currentIndex);
                return true;
            }
            return false;
        }

        /**
         * 获取整数字段值
         * @param columnName 列名
         * @return 整数值
         */
        public int getInt(String columnName) {
            if (currentRecord == null) {
                throw new IllegalStateException("没有当前记录。请先调用next()。");
            }

            Object value = currentRecord.getFieldValue(columnName);
            if (value == null) {
                return 0;
            } else if (value instanceof Number) {
                return ((Number) value).intValue();
            } else {
                try {
                    return Integer.parseInt(value.toString());
                } catch (NumberFormatException e) {
                    throw new IllegalArgumentException("列 " + columnName + " 不是整数");
                }
            }
        }

        /**
         * 获取长整数字段值
         * @param columnName 列名
         * @return 长整数值
         */
        public long getLong(String columnName) {
            if (currentRecord == null) {
                throw new IllegalStateException("没有当前记录。请先调用next()。");
            }

            Object value = currentRecord.getFieldValue(columnName);
            if (value == null) {
                return 0L;
            } else if (value instanceof Number) {
                return ((Number) value).longValue();
            } else {
                try {
                    return Long.parseLong(value.toString());
                } catch (NumberFormatException e) {
                    throw new IllegalArgumentException("列 " + columnName + " 不是长整数");
                }
            }
        }

        /**
         * 获取字符串字段值
         * @param columnName 列名
         * @return 字符串值
         */
        public String getString(String columnName) {
            if (currentRecord == null) {
                throw new IllegalStateException("没有当前记录。请先调用next()。");
            }

            Object value = currentRecord.getFieldValue(columnName);
            return value != null ? value.toString() : null;
        }

        /**
         * 获取布尔字段值
         * @param columnName 列名
         * @return 布尔值
         */
        public boolean getBoolean(String columnName) {
            if (currentRecord == null) {
                throw new IllegalStateException("没有当前记录。请先调用next()。");
            }

            Object value = currentRecord.getFieldValue(columnName);
            if (value == null) {
                return false;
            } else if (value instanceof Boolean) {
                return (Boolean) value;
            } else if (value instanceof Number) {
                return ((Number) value).intValue() != 0;
            } else {
                return Boolean.parseBoolean(value.toString());
            }
        }

        /**
         * 获取任意类型字段值
         * @param columnName 列名
         * @return 字段值
         */
        public Object getObject(String columnName) {
            if (currentRecord == null) {
                throw new IllegalStateException("没有当前记录。请先调用next()。");
            }

            return currentRecord.getFieldValue(columnName);
        }

        /**
         * 获取所有记录
         * @return 记录列表
         */
        public List<Record> getRecords() {
            return records;
        }

        /**
         * 获取记录数
         * @return 记录数
         */
        public int size() {
            return records.size();
        }

        /**
         * 检查结果集是否为空
         * @return 是否为空
         */
        public boolean isEmpty() {
            return records.isEmpty();
        }
    }
}