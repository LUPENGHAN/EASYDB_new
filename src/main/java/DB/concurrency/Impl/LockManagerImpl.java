package DB.concurrency.Impl;

import DB.concurrency.interfaces.LockManager;
import DB.concurrency.models.Lock;
import DB.concurrency.models.LockType;
import lombok.Getter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 锁管理器实现类
 */
@Getter
public class LockManagerImpl implements LockManager {
    // 锁表：key为资源ID（pageId:slotId），value为锁列表
    private final Map<String, List<Lock>> lockTable;
    // 事务锁表：key为事务ID，value为该事务持有的所有锁
    private final Map<Long, List<Lock>> transactionLocks;
    // 全局锁，用于保护锁表的并发访问
    private final ReentrantLock globalLock;
    // 锁等待超时时间（毫秒）
    private static final long LOCK_TIMEOUT = 5000;

    public LockManagerImpl() {
        this.lockTable = new ConcurrentHashMap<>();
        this.transactionLocks = new ConcurrentHashMap<>();
        this.globalLock = new ReentrantLock();
    }

    @Override
    public Lock acquireLock(long xid, LockType type, int pageId, int slotId) {
        String resourceId = getResourceId(pageId, slotId);
        Lock lock = new Lock(xid, type, pageId, slotId);

        try {
            if (globalLock.tryLock(LOCK_TIMEOUT, TimeUnit.MILLISECONDS)) {
                try {
                    // 检查死锁
                    if (hasDeadlock(xid)) {
                        throw new RuntimeException("Deadlock detected");
                    }

                    // 获取锁
                    List<Lock> locks = lockTable.computeIfAbsent(resourceId, k -> new ArrayList<>());
                    
                    // 检查锁兼容性
                    if (isCompatible(locks, type)) {
                        locks.add(lock);
                        transactionLocks.computeIfAbsent(xid, k -> new ArrayList<>()).add(lock);
                        
                        // 根据锁类型获取相应的锁
                        if (type == LockType.SHARED) {
                            lock.lockRead();
                        } else {
                            lock.lockWrite();
                        }
                        
                        return lock;
                    } else {
                        throw new RuntimeException("Lock conflict");
                    }
                } finally {
                    globalLock.unlock();
                }
            } else {
                throw new RuntimeException("Lock acquisition timeout");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Lock acquisition interrupted", e);
        }
    }

    @Override
    public void releaseLock(Lock lock) {
        String resourceId = getResourceId(lock.getPageId(), lock.getSlotId());
        
        globalLock.lock();
        try {
            List<Lock> locks = lockTable.get(resourceId);
            if (locks != null) {
                locks.remove(lock);
                if (locks.isEmpty()) {
                    lockTable.remove(resourceId);
                }
            }
            
            List<Lock> transactionLockList = transactionLocks.get(lock.getXid());
            if (transactionLockList != null) {
                transactionLockList.remove(lock);
                if (transactionLockList.isEmpty()) {
                    transactionLocks.remove(lock.getXid());
                }
            }
            
            // 释放实际的锁
            if (lock.getType() == LockType.SHARED) {
                lock.unlockRead();
            } else {
                lock.unlockWrite();
            }
        } finally {
            globalLock.unlock();
        }
    }

    @Override
    public void releaseAllLocks(long xid) {
        List<Lock> locks = transactionLocks.remove(xid);
        if (locks != null) {
            for (Lock lock : locks) {
                releaseLock(lock);
            }
        }
    }

    @Override
    public boolean hasDeadlock(long xid) {
        // 使用深度优先搜索检测死锁
        Map<Long, Boolean> visited = new HashMap<>();
        Map<Long, Boolean> recursionStack = new HashMap<>();
        
        for (Long txId : transactionLocks.keySet()) {
            if (isCyclic(txId, visited, recursionStack)) {
                return true;
            }
        }
        
        return false;
    }

    @Override
    public List<Lock> getLocksByXid(long xid) {
        return new ArrayList<>(transactionLocks.getOrDefault(xid, new ArrayList<>()));
    }

    @Override
    public List<Lock> getLocksByPage(int pageId) {
        List<Lock> result = new ArrayList<>();
        for (List<Lock> locks : lockTable.values()) {
            for (Lock lock : locks) {
                if (lock.getPageId() == pageId) {
                    result.add(lock);
                }
            }
        }
        return result;
    }

    @Override
    public List<Lock> getLocksByRecord(int pageId, int slotId) {
        String resourceId = getResourceId(pageId, slotId);
        return new ArrayList<>(lockTable.getOrDefault(resourceId, new ArrayList<>()));
    }

    @Override
    public Lock upgradeLock(Lock lock) {
        if (!lock.canUpgrade()) {
            throw new IllegalStateException("Cannot upgrade lock");
        }
        
        globalLock.lock();
        try {
            String resourceId = getResourceId(lock.getPageId(), lock.getSlotId());
            List<Lock> locks = lockTable.get(resourceId);
            
            // 检查是否只有一个读锁
            if (locks.size() == 1 && locks.get(0) == lock) {
                lock.upgrade();
                return lock;
            } else {
                throw new IllegalStateException("Cannot upgrade lock due to other locks");
            }
        } finally {
            globalLock.unlock();
        }
    }

    /**
     * 检查锁是否兼容
     */
    private boolean isCompatible(List<Lock> existingLocks, LockType requestedType) {
        if (existingLocks.isEmpty()) {
            return true;
        }
        
        if (requestedType == LockType.SHARED) {
            // 共享锁与共享锁兼容
            return existingLocks.stream().allMatch(l -> l.getType() == LockType.SHARED);
        } else {
            // 排他锁与任何锁都不兼容
            return false;
        }
    }

    /**
     * 获取资源ID
     */
    private String getResourceId(int pageId, int slotId) {
        return pageId + ":" + slotId;
    }

    /**
     * 使用DFS检测死锁
     */
    private boolean isCyclic(long xid, Map<Long, Boolean> visited, Map<Long, Boolean> recursionStack) {
        if (recursionStack.getOrDefault(xid, false)) {
            return true;
        }
        
        if (visited.getOrDefault(xid, false)) {
            return false;
        }
        
        visited.put(xid, true);
        recursionStack.put(xid, true);
        
        List<Lock> locks = transactionLocks.get(xid);
        if (locks != null) {
            for (Lock lock : locks) {
                String resourceId = getResourceId(lock.getPageId(), lock.getSlotId());
                List<Lock> resourceLocks = lockTable.get(resourceId);
                if (resourceLocks != null) {
                    for (Lock otherLock : resourceLocks) {
                        if (otherLock.getXid() != xid && isCyclic(otherLock.getXid(), visited, recursionStack)) {
                            return true;
                        }
                    }
                }
            }
        }
        
        recursionStack.put(xid, false);
        return false;
    }
} 