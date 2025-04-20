package DB.transaction.impl;

import DB.concurrency.interfaces.LockManager;
import DB.concurrency.models.Lock;
import DB.concurrency.models.LockType;
import DB.log.interfaces.LogManager;
import DB.page.models.Page;
import DB.record.models.Record;
import DB.transaction.interfaces.TransactionManager;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 事务管理器实现
 */
@Slf4j
public class TransactionManagerImpl implements TransactionManager {
    private final LogManager logManager;
    private final LockManager lockManager;
    private final AtomicLong nextXid;
    private final Map<Long, TransactionStatus> transactionStatus;
    private final Map<Long, Map<Page, Lock>> transactionLocks;
    private final Map<Long, List<Page>> modifiedPagesMap;
    private final Map<Long, List<Record>> modifiedRecordsMap;

    public TransactionManagerImpl(LogManager logManager, LockManager lockManager) {
        this.logManager = logManager;
        this.lockManager = lockManager;
        this.nextXid = new AtomicLong(1);
        this.transactionStatus = new ConcurrentHashMap<>();
        this.transactionLocks = new ConcurrentHashMap<>();
        this.modifiedPagesMap = new ConcurrentHashMap<>();
        this.modifiedRecordsMap = new ConcurrentHashMap<>();
    }

    @Override
    public long beginTransaction() {
        long xid = nextXid.getAndIncrement();
        transactionStatus.put(xid, TransactionStatus.ACTIVE);
        transactionLocks.put(xid, new ConcurrentHashMap<>());
        modifiedPagesMap.put(xid, new ArrayList<>());
        modifiedRecordsMap.put(xid, new ArrayList<>());
        logManager.writeRedoLog(xid, 0, (short) 0, new byte[0]); // 写入开始事务日志
        return xid;
    }

    @Override
    public void commitTransaction(long xid) {
        if (!transactionStatus.containsKey(xid)) {
            throw new IllegalArgumentException("Transaction " + xid + " does not exist");
        }

        try {
            // 释放所有锁
            lockManager.releaseAllLocks(xid);
            transactionLocks.remove(xid);
            modifiedPagesMap.remove(xid);
            modifiedRecordsMap.remove(xid);

            // 写入提交日志
            logManager.writeRedoLog(xid, 0, (short) 0, new byte[0]);

            // 更新事务状态
            transactionStatus.put(xid, TransactionStatus.COMMITTED);
        } catch (Exception e) {
            log.error("Failed to commit transaction " + xid, e);
            rollbackTransaction(xid);
            throw new RuntimeException("Failed to commit transaction " + xid, e);
        }
    }

    @Override
    public void rollbackTransaction(long xid) {
        if (!transactionStatus.containsKey(xid)) {
            throw new IllegalArgumentException("Transaction " + xid + " does not exist");
        }

        try {
            // 释放所有锁
            lockManager.releaseAllLocks(xid);
            transactionLocks.remove(xid);
            modifiedPagesMap.remove(xid);
            modifiedRecordsMap.remove(xid);

            // 写入回滚日志
            logManager.writeUndoLog(xid, 0, new byte[0]);

            // 更新事务状态
            transactionStatus.put(xid, TransactionStatus.ABORTED);
        } catch (Exception e) {
            log.error("Failed to rollback transaction " + xid, e);
            throw new RuntimeException("Failed to rollback transaction " + xid, e);
        }
    }

    @Override
    public TransactionStatus getTransactionStatus(long xid) {
        return transactionStatus.getOrDefault(xid, TransactionStatus.ABORTED);
    }

    @Override
    public Lock acquireLock(long xid, Page page, LockType lockType) {
        if (getTransactionStatus(xid) != TransactionStatus.ACTIVE) {
            throw new IllegalStateException("Transaction " + xid + " is not active");
        }

        Lock lock = lockManager.acquireLock(xid, lockType, page.getHeader().getPageId(), 0);
        if (lock != null) {
            transactionLocks.computeIfAbsent(xid, k -> new ConcurrentHashMap<>())
                    .put(page, lock);
            if (!modifiedPagesMap.get(xid).contains(page)) {
                modifiedPagesMap.get(xid).add(page);
            }
        }
        return lock;
    }

    @Override
    public void releaseLock(long xid, Page page) {
        Map<Page, Lock> locks = transactionLocks.get(xid);
        if (locks != null) {
            Lock lock = locks.remove(page);
            if (lock != null) {
                lockManager.releaseLock(lock);
            }
        }
    }

    @Override
    public boolean holdsLock(long xid, Page page) {
        Map<Page, Lock> locks = transactionLocks.get(xid);
        return locks != null && locks.containsKey(page);
    }

    @Override
    public List<Page> getModifiedPages(long xid) {
        return new ArrayList<>(modifiedPagesMap.getOrDefault(xid, new ArrayList<>()));
    }

    @Override
    public List<Record> getModifiedRecords(long xid) {
        return new ArrayList<>(modifiedRecordsMap.getOrDefault(xid, new ArrayList<>()));
    }

    public void addModifiedRecord(long xid, Record record) {
        if (!modifiedRecordsMap.get(xid).contains(record)) {
            modifiedRecordsMap.get(xid).add(record);
        }
    }
} 