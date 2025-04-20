package DB;

import DB.log.interfaces.LogManager;
import DB.log.impl.LogManagerImpl;
import DB.log.models.LogRecord;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.file.Path;
import java.util.List;
import java.nio.channels.FileChannel;
import java.lang.reflect.Field;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 日志管理器测试类
 */
public class LogManagerTest {
    
    private LogManager logManager;
    private String logDir;
    
    @BeforeEach
    void setUp(@TempDir Path tempDir) {
        // 使用临时目录创建测试日志目录
        logDir = tempDir.toString();
        logManager = new LogManagerImpl(logDir);
        logManager.init();
    }
    
    @AfterEach
    void tearDown() {
        try {
            // 关闭日志管理器
            if (logManager != null) {
                logManager.close();
            }
            
            // 确保所有资源释放
            System.gc();
            Thread.sleep(100);
            
            // 删除测试日志目录中的文件
            File logDirFile = new File(logDir);
            if (logDirFile.exists() && logDirFile.isDirectory()) {
                File[] files = logDirFile.listFiles();
                if (files != null) {
                    for (File file : files) {
                        boolean deleted = file.delete();
                        if (!deleted) {
                            System.err.println("警告：无法删除日志文件 " + file.getAbsolutePath());
                        }
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    @Test
    @DisplayName("测试写入和读取重做日志")
    void testWriteAndReadRedoLog() {
        // 写入重做日志
        long xid = 1001;
        int pageId = 123;
        short offset = 456;
        byte[] newData = new byte[]{1, 2, 3, 4, 5};
        
        long lsn = logManager.writeRedoLog(xid, pageId, offset, newData);
        
        // 验证写入成功
        assertTrue(lsn > 0, "日志序列号应为正数");
        
        // 读取日志记录
        LogRecord logRecord = logManager.readLog(lsn);
        
        // 验证读取结果
        assertNotNull(logRecord, "读取的日志记录不应为空");
        assertEquals(LogRecord.TYPE_REDO, logRecord.getLogType(), "日志类型应为重做日志");
        assertEquals(xid, logRecord.getXid(), "事务ID应匹配");
        assertEquals(pageId, logRecord.getPageID(), "页面ID应匹配");
        assertEquals(offset, logRecord.getOffset(), "偏移量应匹配");
        assertArrayEquals(newData, logRecord.getNewData(), "新数据应匹配");
    }
    
    @Test
    @DisplayName("测试写入和读取撤销日志")
    void testWriteAndReadUndoLog() {
        // 写入撤销日志
        long xid = 1001;
        int operationType = LogRecord.UNDO_UPDATE;
        byte[] undoData = new byte[]{5, 4, 3, 2, 1};
        
        long lsn = logManager.writeUndoLog(xid, operationType, undoData);
        
        // 验证写入成功
        assertTrue(lsn > 0, "日志序列号应为正数");
        
        // 读取日志记录
        LogRecord logRecord = logManager.readLog(lsn);
        
        // 验证读取结果
        assertNotNull(logRecord, "读取的日志记录不应为空");
        assertEquals(LogRecord.TYPE_UNDO, logRecord.getLogType(), "日志类型应为撤销日志");
        assertEquals(xid, logRecord.getXid(), "事务ID应匹配");
        assertEquals(operationType, logRecord.getOperationType(), "操作类型应匹配");
        assertArrayEquals(undoData, logRecord.getUndoData(), "撤销数据应匹配");
    }
    
    @Test
    @DisplayName("测试获取事务的所有日志")
    void testGetTransactionLogs() {
        // 写入多条日志
        long xid = 1001;
        
        // 写入重做日志
        long lsn1 = logManager.writeRedoLog(xid, 123, (short)456, new byte[]{1, 2, 3});
        
        // 写入撤销日志
        long lsn2 = logManager.writeUndoLog(xid, LogRecord.UNDO_UPDATE, new byte[]{4, 5, 6});
        
        // 获取事务的所有日志
        List<LogRecord> logs = logManager.getTransactionLogs(xid);
        
        // 验证结果
        assertEquals(2, logs.size(), "应有2条日志");
        
        // 验证第一条日志
        LogRecord log1 = logs.get(0);
        assertEquals(LogRecord.TYPE_REDO, log1.getLogType(), "第一条日志类型应为重做日志");
        assertEquals(xid, log1.getXid(), "第一条日志的事务ID应匹配");
        
        // 验证第二条日志
        LogRecord log2 = logs.get(1);
        assertEquals(LogRecord.TYPE_UNDO, log2.getLogType(), "第二条日志类型应为撤销日志");
        assertEquals(xid, log2.getXid(), "第二条日志的事务ID应匹配");
    }
    
    @Test
    @DisplayName("测试创建检查点")
    void testCreateCheckpoint() {
        // 创建检查点
        logManager.createCheckpoint();
        
        // 获取活跃事务列表
        List<Long> activeTransactions = logManager.getActiveTransactions();
        
        // 由于没有活跃事务，列表应为空
        assertTrue(activeTransactions.isEmpty(), "活跃事务列表应为空");
    }
    
    @Test
    @DisplayName("测试获取活跃事务")
    void testGetActiveTransactions() {
        // 写入事务开始日志
        long xid1 = 1001;
        long xid2 = 1002;
        
        logManager.writeRedoLog(xid1, 0, (short)0, new byte[0]); // 事务1开始
        logManager.writeRedoLog(xid2, 0, (short)0, new byte[0]); // 事务2开始
        
        // 获取活跃事务列表
        List<Long> activeTransactions = logManager.getActiveTransactions();
        
        // 验证结果
        assertEquals(2, activeTransactions.size(), "应有2个活跃事务");
        assertTrue(activeTransactions.contains(xid1), "活跃事务列表应包含事务1");
        assertTrue(activeTransactions.contains(xid2), "活跃事务列表应包含事务2");
    }
    
    @Test
    @DisplayName("测试日志恢复")
    void testRecover() {
        // 写入多条日志
        long xid1 = 1001;
        long xid2 = 1002;
        
        System.out.println("开始写入日志...");
        logManager.writeRedoLog(xid1, 123, (short)456, new byte[]{1, 2, 3}); // 事务1操作
        logManager.writeRedoLog(xid2, 234, (short)567, new byte[]{4, 5, 6}); // 事务2操作
        logManager.writeUndoLog(xid1, LogRecord.UNDO_UPDATE, new byte[]{7, 8, 9}); // 事务1回滚
        
        System.out.println("检查当前活跃事务...");
        List<Long> currentTransactions = logManager.getActiveTransactions();
        System.out.println("当前活跃事务数量: " + currentTransactions.size());
        for (Long xid : currentTransactions) {
            System.out.println("当前活跃事务ID: " + xid);
        }
        
        // 确保日志文件已被刷新到磁盘，但不关闭原始logManager
        try {
            Field channelField = LogManagerImpl.class.getDeclaredField("channel");
            channelField.setAccessible(true);
            FileChannel channel = (FileChannel) channelField.get(logManager);
            channel.force(true);
            System.out.println("强制刷新日志到磁盘");
        } catch (Exception e) {
            e.printStackTrace();
        }
        
        // 创建新的日志管理器实例，模拟恢复
        System.out.println("创建新的日志管理器实例...");
        LogManagerImpl newLogManager = new LogManagerImpl(logDir);
        newLogManager.init(); // 初始化
        
        // 验证恢复后的状态
        System.out.println("验证恢复后的活跃事务...");
        List<Long> activeTransactions = newLogManager.getActiveTransactions();
        System.out.println("恢复后活跃事务数量: " + activeTransactions.size());
        for (Long xid : activeTransactions) {
            System.out.println("恢复后活跃事务ID: " + xid);
        }
        
        assertEquals(1, activeTransactions.size(), "应有1个活跃事务");
        assertTrue(activeTransactions.contains(xid2), "活跃事务列表应包含事务2");
        
        // 关闭新实例
        newLogManager.close();
    }
    
    @Test
    @DisplayName("测试日志序列化和反序列化")
    void testLogSerializeAndDeserialize() {
        // 创建重做日志记录
        long xid = 1001;
        int pageId = 123;
        short offset = 456;
        byte[] newData = new byte[]{1, 2, 3, 4, 5};
        
        LogRecord redoLog = LogRecord.createRedoLog(xid, pageId, offset, newData);
        
        // 序列化
        byte[] serialized = redoLog.serialize();
        
        // 反序列化
        LogRecord deserialized = LogRecord.deserialize(serialized);
        
        // 验证反序列化结果
        assertNotNull(deserialized, "反序列化的日志记录不应为空");
        assertEquals(LogRecord.TYPE_REDO, deserialized.getLogType(), "日志类型应为重做日志");
        assertEquals(xid, deserialized.getXid(), "事务ID应匹配");
        assertEquals(pageId, deserialized.getPageID(), "页面ID应匹配");
        assertEquals(offset, deserialized.getOffset(), "偏移量应匹配");
        assertArrayEquals(newData, deserialized.getNewData(), "新数据应匹配");
    }
} 