package DB;

import DB.concurrency.models.Lock;
import DB.concurrency.models.LockType;
import DB.log.impl.LogManagerImpl;
import DB.concurrency.interfaces.LockManager;
import DB.concurrency.Impl.LockManagerImpl;
import DB.transaction.impl.TransactionManagerImpl;
import DB.transaction.interfaces.TransactionManager;
import DB.page.models.Page;
import DB.record.models.Record;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;

import java.util.List;
import java.io.File;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 事务管理测试类
 */
public class TransactionTest {
    
    private TransactionManager transactionManager;
    private LockManager lockManager;
    private LogManagerImpl logManager;
    
    @BeforeEach
    void setUp() {
        // 初始化事务管理器及其依赖
        lockManager = new LockManagerImpl();
        
        // 确保测试日志目录存在
        File logDir = new File("./test_logs");
        if (!logDir.exists()) {
            logDir.mkdirs();
        }
        
        logManager = new LogManagerImpl("./test_logs");
        logManager.init(); // 调用init方法初始化FileChannel
        
        transactionManager = new TransactionManagerImpl(logManager, lockManager);
    }
    
    @Test
    @DisplayName("测试事务生命周期：开始、提交和回滚")
    void testTransactionLifecycle() {
        // 测试开始事务
        long xid1 = transactionManager.beginTransaction();
        assertTrue(xid1 > 0, "事务ID应该是正数");
        assertEquals(TransactionManager.TransactionStatus.ACTIVE, 
                transactionManager.getTransactionStatus(xid1), 
                "新创建的事务应该处于活跃状态");
        
        // 测试提交事务
        transactionManager.commitTransaction(xid1);
        assertEquals(TransactionManager.TransactionStatus.COMMITTED, 
                transactionManager.getTransactionStatus(xid1), 
                "提交后的事务应该被标记为已提交");
        
        // 测试开始新事务并回滚
        long xid2 = transactionManager.beginTransaction();
        assertTrue(xid2 > 0, "事务ID应该是正数");
        transactionManager.rollbackTransaction(xid2);
        assertEquals(TransactionManager.TransactionStatus.ABORTED, 
                transactionManager.getTransactionStatus(xid2), 
                "回滚后的事务应该被标记为已中止");
    }
    
    @Test
    @DisplayName("测试并发事务")
    void testConcurrentTransactions() {
        // 开始多个事务
        long xid1 = transactionManager.beginTransaction();
        long xid2 = transactionManager.beginTransaction();
        long xid3 = transactionManager.beginTransaction();
        
        // 验证事务ID是唯一的
        assertNotEquals(xid1, xid2, "事务ID应该是唯一的");
        assertNotEquals(xid2, xid3, "事务ID应该是唯一的");
        assertNotEquals(xid1, xid3, "事务ID应该是唯一的");
        
        // 提交和回滚不同事务
        transactionManager.commitTransaction(xid1);
        assertEquals(TransactionManager.TransactionStatus.COMMITTED, 
                transactionManager.getTransactionStatus(xid1), 
                "事务1应该已提交");
        
        transactionManager.rollbackTransaction(xid2);
        assertEquals(TransactionManager.TransactionStatus.ABORTED, 
                transactionManager.getTransactionStatus(xid2), 
                "事务2应该已中止");
        
        transactionManager.commitTransaction(xid3);
        assertEquals(TransactionManager.TransactionStatus.COMMITTED, 
                transactionManager.getTransactionStatus(xid3), 
                "事务3应该已提交");
    }
    
    @Test
    @DisplayName("测试锁获取和释放")
    void testLockAcquisition() {
        long xid = transactionManager.beginTransaction();
        
        // 创建测试页面
        Page page = new Page(100);

        
        // 获取锁
        Lock lock = transactionManager.acquireLock(xid, page, LockType.SHARED);
        assertNotNull(lock, "应该能够获取共享锁");
        
        // 验证锁持有状态
        assertTrue(transactionManager.holdsLock(xid, page),
                "事务应该持有共享锁");
        
        // 释放锁
        transactionManager.releaseLock(xid, page);
        
        // 验证锁已释放
        assertFalse(transactionManager.holdsLock(xid, page),
                "锁应该已被释放");
    }
    
    @Test
    @DisplayName("测试跟踪修改的页面和记录")
    void testTrackingModifiedPagesAndRecords() {
        long xid = transactionManager.beginTransaction();
        
        // 创建测试页面

        Page page1 = new Page(1);

        // 创建测试记录
        Record record = Record.builder()
                .pageId(1)
                .slotId(1)
                .build();
        
        // 获取锁以自动跟踪修改的页面
        transactionManager.acquireLock(xid, page1, LockType.EXCLUSIVE);
        
        // 手动添加修改的记录
        ((TransactionManagerImpl)transactionManager).addModifiedRecord(xid, record);
        
        // 获取修改的页面和记录
        List<Page> modifiedPages = transactionManager.getModifiedPages(xid);
        List<Record> modifiedRecords = transactionManager.getModifiedRecords(xid);
        
        // 验证修改被正确跟踪
        assertEquals(1, modifiedPages.size(), "应有一个修改的页面");
        assertEquals(1, modifiedRecords.size(), "应有一个修改的记录");
        assertEquals(page1.getHeader().getPageId(), modifiedPages.get(0).getHeader().getPageId(), 
                "应返回正确的修改页面");
        assertEquals(record.getPageId(), modifiedRecords.get(0).getPageId(), 
                "应返回正确的修改记录");
    }
    
    @Test
    @DisplayName("测试事务提交时释放锁和清理资源")
    void testResourceCleanupOnCommit() {
        long xid = transactionManager.beginTransaction();
        
        // 创建测试页面并获取锁
        Page page = new Page(123);

        
        transactionManager.acquireLock(xid, page, LockType.EXCLUSIVE);
        assertTrue(transactionManager.holdsLock(xid, page), "事务应该持有该页面的锁");
        
        // 提交事务
        transactionManager.commitTransaction(xid);
        
        // 验证锁已释放
        assertFalse(transactionManager.holdsLock(xid, page), "提交后锁应该被释放");
        
        // 验证修改跟踪已清理
        assertTrue(transactionManager.getModifiedPages(xid).isEmpty(), "提交后修改页面列表应该为空");
        assertTrue(transactionManager.getModifiedRecords(xid).isEmpty(), "提交后修改记录列表应该为空");
    }
    
    @Test
    @DisplayName("测试事务回滚时释放锁和清理资源")
    void testResourceCleanupOnRollback() {
        long xid = transactionManager.beginTransaction();
        
        // 创建测试页面并获取锁
        Page page = new Page(456);

        
        transactionManager.acquireLock(xid, page, LockType.EXCLUSIVE);
        assertTrue(transactionManager.holdsLock(xid, page), "事务应该持有该页面的锁");
        
        // 回滚事务
        transactionManager.rollbackTransaction(xid);
        
        // 验证锁已释放
        assertFalse(transactionManager.holdsLock(xid, page), "回滚后锁应该被释放");
        
        // 验证修改跟踪已清理
        assertTrue(transactionManager.getModifiedPages(xid).isEmpty(), "回滚后修改页面列表应该为空");
        assertTrue(transactionManager.getModifiedRecords(xid).isEmpty(), "回滚后修改记录列表应该为空");
    }
} 