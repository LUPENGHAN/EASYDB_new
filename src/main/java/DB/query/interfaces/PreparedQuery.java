package DB.query.interfaces;

import DB.record.models.Record;
import DB.transaction.interfaces.TransactionManager;

import java.util.List;

/**
 * 预编译查询接口
 */
public interface PreparedQuery {
    /**
     * 执行预编译查询
     * @param params 参数数组
     * @param transactionManager 事务管理器
     * @return 查询结果
     */
    List<Record> execute(Object[] params, TransactionManager transactionManager);

    /**
     * 执行预编译更新
     * @param params 参数数组
     * @param transactionManager 事务管理器
     * @return 影响的行数
     */
    int executeUpdate(Object[] params, TransactionManager transactionManager);

    /**
     * 关闭预编译查询
     */
    void close();
} 