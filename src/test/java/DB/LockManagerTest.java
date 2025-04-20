package DB;

import DB.concurrency.interfaces.LockManager;
import DB.concurrency.Impl.LockManagerImpl;
import DB.concurrency.models.Lock;
import DB.concurrency.models.LockType;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 锁管理器测试类
 */
public class LockManagerTest {
    
    private LockManager lockManager;
    
    @BeforeEach
    void setUp() {
        lockManager = new LockManagerImpl();
    }
    
    @Test
    @DisplayName("测试获取共享锁")
    void testAcquireSharedLock() {
        // 定义资源
        int pageId = 123;
        int slotId = 0;
        long xid = 1001;
        
        // 获取共享锁
        Lock lock = lockManager.acquireLock(xid, LockType.SHARED, pageId, slotId);
        
        // 验证锁属性
        assertNotNull(lock, "获取的锁不应为空");
        assertEquals(xid, lock.getXid(), "锁的事务ID应匹配");
        assertEquals(LockType.SHARED, lock.getType(), "锁类型应为共享锁");
        assertEquals(pageId, lock.getPageId(), "锁的页面ID应匹配");
        assertEquals(slotId, lock.getSlotId(), "锁的槽位ID应匹配");
    }
    
    @Test
    @DisplayName("测试获取排他锁")
    void testAcquireExclusiveLock() {
        // 定义资源
        int pageId = 123;
        int slotId = 0;
        long xid = 1001;
        
        // 获取排他锁
        Lock lock = lockManager.acquireLock(xid, LockType.EXCLUSIVE, pageId, slotId);
        
        // 验证锁属性
        assertNotNull(lock, "获取的锁不应为空");
        assertEquals(xid, lock.getXid(), "锁的事务ID应匹配");
        assertEquals(LockType.EXCLUSIVE, lock.getType(), "锁类型应为排他锁");
        assertEquals(pageId, lock.getPageId(), "锁的页面ID应匹配");
        assertEquals(slotId, lock.getSlotId(), "锁的槽位ID应匹配");
    }
    
    @Test
    @DisplayName("测试锁兼容性：多个事务获取共享锁")
    void testMultipleSharedLocks() {
        // 定义资源
        int pageId = 123;
        int slotId = 0;
        
        // 第一个事务获取共享锁
        long xid1 = 1001;
        Lock lock1 = lockManager.acquireLock(xid1, LockType.SHARED, pageId, slotId);
        assertNotNull(lock1, "第一个事务应能获取共享锁");
        
        // 第二个事务尝试获取共享锁
        long xid2 = 1002;
        Lock lock2 = lockManager.acquireLock(xid2, LockType.SHARED, pageId, slotId);
        assertNotNull(lock2, "第二个事务也应能获取共享锁");
    }
    
    @Test
    @DisplayName("测试锁冲突：排他锁与共享锁冲突")
    void testLockConflict() {
        // 定义资源
        int pageId = 123;
        int slotId = 0;
        
        // 第一个事务获取排他锁
        long xid1 = 1001;
        Lock lock1 = lockManager.acquireLock(xid1, LockType.EXCLUSIVE, pageId, slotId);
        assertNotNull(lock1, "第一个事务应能获取排他锁");
        
        // 第二个事务尝试获取共享锁
        long xid2 = 1002;
        Lock lock2 = lockManager.acquireLock(xid2, LockType.SHARED, pageId, slotId);
        assertNull(lock2, "当资源已被排他锁占用时，第二个事务不应能获取共享锁");
    }
    
    @Test
    @DisplayName("测试释放锁")
    void testReleaseLock() {
        // 定义资源
        int pageId = 123;
        int slotId = 0;
        long xid = 1001;
        
        // 获取排他锁
        Lock lock = lockManager.acquireLock(xid, LockType.EXCLUSIVE, pageId, slotId);
        assertNotNull(lock, "应能获取排他锁");
        
        // 释放锁
        lockManager.releaseLock(lock);
        
        // 验证锁已释放：另一个事务应能获取该资源的锁
        long xid2 = 1002;
        Lock lock2 = lockManager.acquireLock(xid2, LockType.EXCLUSIVE, pageId, slotId);
        assertNotNull(lock2, "释放锁后，另一个事务应能获取该资源的锁");
    }
    
    @Test
    @DisplayName("测试释放事务的所有锁")
    void testReleaseAllLocks() {
        // 定义事务
        long xid = 1001;
        
        // 获取多个资源的锁
        Lock lock1 = lockManager.acquireLock(xid, LockType.SHARED, 123, 0);
        Lock lock2 = lockManager.acquireLock(xid, LockType.EXCLUSIVE, 124, 0);
        Lock lock3 = lockManager.acquireLock(xid, LockType.SHARED, 125, 0);
        
        // 释放该事务的所有锁
        lockManager.releaseAllLocks(xid);
        
        // 验证锁已释放：所有资源都应能被其他事务获取
        long xid2 = 1002;
        Lock newLock1 = lockManager.acquireLock(xid2, LockType.EXCLUSIVE, 123, 0);
        Lock newLock2 = lockManager.acquireLock(xid2, LockType.EXCLUSIVE, 124, 0);
        Lock newLock3 = lockManager.acquireLock(xid2, LockType.EXCLUSIVE, 125, 0);
        
        assertNotNull(newLock1, "释放所有锁后，资源1应能被获取");
        assertNotNull(newLock2, "释放所有锁后，资源2应能被获取");
        assertNotNull(newLock3, "释放所有锁后，资源3应能被获取");
    }
    
    @Test
    @DisplayName("测试获取事务持有的所有锁")
    void testGetLocksByXid() {
        // 定义事务
        long xid = 1001;
        
        // 获取多个资源的锁
        Lock lock1 = lockManager.acquireLock(xid, LockType.SHARED, 123, 0);
        Lock lock2 = lockManager.acquireLock(xid, LockType.EXCLUSIVE, 124, 0);
        Lock lock3 = lockManager.acquireLock(xid, LockType.SHARED, 125, 0);
        
        // 获取该事务持有的所有锁
        List<Lock> locks = lockManager.getLocksByXid(xid);
        
        // 验证结果
        assertEquals(3, locks.size(), "应有3个锁");
        assertTrue(locks.contains(lock1), "锁列表应包含锁1");
        assertTrue(locks.contains(lock2), "锁列表应包含锁2");
        assertTrue(locks.contains(lock3), "锁列表应包含锁3");
    }
    
    @Test
    @DisplayName("测试获取页面的所有锁")
    void testGetLocksByPage() {
        // 定义页面
        int pageId = 123;
        
        // 不同事务获取该页面的锁
        Lock lock1 = lockManager.acquireLock(1001, LockType.SHARED, pageId, 0);
        Lock lock2 = lockManager.acquireLock(1002, LockType.SHARED, pageId, 1);
        
        // 获取页面的所有锁
        List<Lock> locks = lockManager.getLocksByPage(pageId);
        
        // 验证结果
        assertEquals(2, locks.size(), "应有2个锁");
        assertTrue(locks.contains(lock1), "锁列表应包含锁1");
        assertTrue(locks.contains(lock2), "锁列表应包含锁2");
    }
    
    @Test
    @DisplayName("测试获取记录的所有锁")
    void testGetLocksByRecord() {
        // 定义记录
        int pageId = 123;
        int slotId = 5;
        
        // 不同事务获取该记录的锁
        Lock lock1 = lockManager.acquireLock(1001, LockType.SHARED, pageId, slotId);
        
        // 获取记录的所有锁
        List<Lock> locks = lockManager.getLocksByRecord(pageId, slotId);
        
        // 验证结果
        assertEquals(1, locks.size(), "应有1个锁");
        assertTrue(locks.contains(lock1), "锁列表应包含锁1");
    }
    
    @Test
    @DisplayName("测试锁升级：从共享锁升级到排他锁")
    void testUpgradeLock() {
        // 定义资源
        int pageId = 123;
        int slotId = 0;
        long xid = 1001;
        
        // 获取共享锁
        Lock sharedLock = lockManager.acquireLock(xid, LockType.SHARED, pageId, slotId);
        assertNotNull(sharedLock, "应能获取共享锁");
        assertEquals(LockType.SHARED, sharedLock.getType(), "锁类型应为共享锁");
        
        // 升级到排他锁
        Lock exclusiveLock = lockManager.upgradeLock(sharedLock);
        assertNotNull(exclusiveLock, "应能升级到排他锁");
        assertEquals(LockType.EXCLUSIVE, exclusiveLock.getType(), "升级后锁类型应为排他锁");
        assertEquals(xid, exclusiveLock.getXid(), "升级后锁的事务ID应保持不变");
        assertEquals(pageId, exclusiveLock.getPageId(), "升级后锁的页面ID应保持不变");
        assertEquals(slotId, exclusiveLock.getSlotId(), "升级后锁的槽位ID应保持不变");
    }
    
    @Test
    @DisplayName("测试死锁检测")
    void testDeadlockDetection() {
        // 创建可能导致死锁的情况
        int pageId1 = 123;
        int pageId2 = 124;
        long xid1 = 1001;
        long xid2 = 1002;
        boolean deadlockDetected = false;
        
        System.out.println("\n===== 开始死锁测试 =====");
        
        // 事务1获取资源1的锁
        Lock lock1 = lockManager.acquireLock(xid1, LockType.EXCLUSIVE, pageId1, 0);
        assertNotNull(lock1, "事务1应能获取资源1的排他锁");
        
        // 事务2获取资源2的锁
        Lock lock2 = lockManager.acquireLock(xid2, LockType.EXCLUSIVE, pageId2, 0);
        assertNotNull(lock2, "事务2应能获取资源2的排他锁");
        
        System.out.println("\n----- 构建死锁环 -----");
        
        // 事务2尝试获取资源1的锁（这会被阻塞，因为资源1已被事务1持有）
        Lock lock3 = lockManager.acquireLock(xid2, LockType.EXCLUSIVE, pageId1, 0);
        assertNull(lock3, "事务2暂时不能获取资源1的排他锁");
        
        // 手动检查死锁是否已经发生
        deadlockDetected = lockManager.hasDeadlock(xid2);
        
        // 如果未检测到死锁，事务1尝试获取资源2的锁（这会被阻塞，因为资源2已被事务2持有）
        if (!deadlockDetected) {
            Lock lock4 = lockManager.acquireLock(xid1, LockType.EXCLUSIVE, pageId2, 0);
            assertNull(lock4, "事务1暂时不能获取资源2的排他锁");
            
            // 现在应该有死锁了，再次检查
            deadlockDetected = lockManager.hasDeadlock(xid2) || lockManager.hasDeadlock(xid1);
        }
        
        System.out.println("\n===== 死锁检测结果: " + (deadlockDetected ? "存在死锁" : "无死锁") + " =====");
        
        assertTrue(deadlockDetected, "应检测到死锁");
    }
} 