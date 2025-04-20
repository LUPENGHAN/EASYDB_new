package DB.concurrency.models;


import lombok.Data;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * 锁对象
 */
@Data
public class Lock {
    private final long xid;           // 事务ID
    private final LockType type;      // 锁类型
    private final int pageId;         // 页面ID
    private final int slotId;         // 槽位ID
    private final long timestamp;     // 加锁时间戳
    private final ReentrantReadWriteLock lock; // 实际的锁对象

    public Lock(long xid, LockType type, int pageId, int slotId) {
        this.xid = xid;
        this.type = type;
        this.pageId = pageId;
        this.slotId = slotId;
        this.timestamp = System.currentTimeMillis();
        this.lock = new ReentrantReadWriteLock();
    }

    /**
     * 获取读锁
     */
    public void lockRead() {
        lock.readLock().lock();
    }

    /**
     * 释放读锁
     */
    public void unlockRead() {
        lock.readLock().unlock();
    }

    /**
     * 获取写锁
     */
    public void lockWrite() {
        lock.writeLock().lock();
    }

    /**
     * 释放写锁
     */
    public void unlockWrite() {
        lock.writeLock().unlock();
    }

    /**
     * 检查是否可以被升级为排他锁
     */
    public boolean canUpgrade() {
        return type == LockType.SHARED && lock.getReadHoldCount() == 1;
    }

    /**
     * 升级为排他锁
     */
    public void upgrade() {
        if (!canUpgrade()) {
            throw new IllegalStateException("Cannot upgrade lock");
        }
        lock.readLock().unlock();
        lock.writeLock().lock();
    }

    // 添加显式的getter方法
    public long getXid() {
        return xid;
    }
    
    public LockType getType() {
        return type;
    }
    
    public int getPageId() {
        return pageId;
    }
    
    public int getSlotId() {
        return slotId;
    }
    
    public long getTimestamp() {
        return timestamp;
    }
    
    public ReentrantReadWriteLock getLock() {
        return lock;
    }
} 