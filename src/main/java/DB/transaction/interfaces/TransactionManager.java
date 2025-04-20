package DB.transaction.interfaces;

import DB.concurrency.models.Lock;
import DB.concurrency.models.LockType;
import DB.page.models.Page;
import DB.record.models.Record;
import java.util.List;

/**
 * 事务管理器接口
 */
public interface TransactionManager {
    /**
     * 开始新事务
     * @return 事务ID
     */
    long beginTransaction();

    /**
     * 提交事务
     * @param transactionId 事务ID
     */
    void commitTransaction(long transactionId);

    /**
     * 回滚事务
     * @param transactionId 事务ID
     */
    void rollbackTransaction(long transactionId);

    /**
     * 获取事务状态
     * @param transactionId 事务ID
     * @return 事务状态
     */
    TransactionStatus getTransactionStatus(long transactionId);

    /**
     * 获取事务修改的页面
     * @param transactionId 事务ID
     * @return 页面列表
     */
    List<Page> getModifiedPages(long transactionId);

    /**
     * 获取事务修改的记录
     * @param transactionId 事务ID
     * @return 记录列表
     */
    List<Record> getModifiedRecords(long transactionId);

    /**
     * 获取锁
     * @param transactionId 事务ID
     * @param page 页面
     * @param lockType 锁类型
     * @return 锁对象
     */
    Lock acquireLock(long transactionId, Page page, LockType lockType);

    /**
     * 释放锁
     * @param transactionId 事务ID
     * @param page 页面
     */
    void releaseLock(long transactionId, Page page);

    /**
     * 检查是否持有锁
     * @param transactionId 事务ID
     * @param page 页面
     * @return 是否持有锁
     */
    boolean holdsLock(long transactionId, Page page);

    /**
     * 事务状态枚举
     */
    enum TransactionStatus {
        ACTIVE,
        COMMITTED,
        ROLLED_BACK,
        ABORTED
    }
} 