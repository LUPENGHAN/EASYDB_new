package DB.query.interfaces;

import DB.record.models.Record;
import DB.transaction.interfaces.TransactionManager;

import java.util.List;

/**
 * 查询执行器接口
 */
public interface QueryExecutor {
    /**
     * 执行SQL查询
     * @param sql SQL查询语句
     * @param transactionManager 事务管理器
     * @return 查询结果记录列表
     */
    List<Record> execute(String sql, TransactionManager transactionManager);

    /**
     * 执行SQL更新操作
     * @param sql SQL更新语句
     * @param transactionManager 事务管理器
     * @return 影响的行数
     */
    int executeUpdate(String sql, TransactionManager transactionManager);

    /**
     * 预编译SQL查询
     * @param sql SQL查询语句
     * @return 预编译查询对象
     */
    PreparedQuery prepare(String sql);

    /**
     * 预编译查询接口
     */
    interface PreparedQuery {
        /**
         * 设置参数
         * @param index 参数索引
         * @param value 参数值
         */
        void setParameter(int index, Object value);

        /**
         * 执行查询
         * @param transactionManager 事务管理器
         * @return 查询结果
         */
        List<Record> execute(TransactionManager transactionManager);

        /**
         * 执行更新
         * @param transactionManager 事务管理器
         * @return 影响的行数
         */
        int executeUpdate(TransactionManager transactionManager);
    }
} 