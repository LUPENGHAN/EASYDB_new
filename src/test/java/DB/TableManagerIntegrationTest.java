package DB;

import DB.concurrency.Impl.LockManagerImpl;
import DB.concurrency.interfaces.LockManager;
import DB.index.Impl.BPlusTreeIndex;
import DB.index.interfaces.IndexManager;
import DB.log.impl.LogManagerImpl;
import DB.page.interfaces.PageManager;
import DB.page.impl.PageManagerImpl;
import DB.query.impl.QueryCore;
import DB.query.impl.QueryParserImpl;
import DB.record.interfaces.RecordManager;
import DB.record.impl.RecordManagerImpl;
import DB.record.models.Record;
import DB.table.interfaces.TableManager;
import DB.table.impl.TableManagerImpl;
import DB.table.models.Column;
import DB.table.models.Table;
import DB.transaction.interfaces.TransactionManager;
import DB.transaction.impl.TransactionManagerImpl;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 表管理器与查询核心集成测试类
 */
public class TableManagerIntegrationTest {
    
    private PageManager pageManager;
    private RecordManager recordManager;
    private TableManager tableManager;
    private TransactionManager transactionManager;
    private QueryCore queryCore;
    private LogManagerImpl logManager;
    private LockManager lockManager;
    private IndexManager indexManager;
    private String dbFilePath;
    
    @BeforeEach
    void setUp(@TempDir Path tempDir) throws Exception {
        // 使用临时目录创建测试数据库文件
        dbFilePath = tempDir.resolve("test.db").toString();
        String logDir = tempDir.resolve("logs").toString();
        new File(logDir).mkdirs();
        
        // 初始化各组件
        pageManager = new PageManagerImpl(dbFilePath);
        recordManager = new RecordManagerImpl(pageManager);
        lockManager = new LockManagerImpl();
        logManager = new LogManagerImpl(logDir);
        transactionManager = new TransactionManagerImpl(logManager, lockManager);
        tableManager = new TableManagerImpl(pageManager);
        indexManager = new BPlusTreeIndex(100);
        
        // 创建查询核心
        queryCore = new QueryCore(
            tableManager,
            indexManager,
            recordManager,
            new QueryParserImpl(),
            transactionManager
        );
        
        // 创建测试表
        createTestTable();
        
        // 插入测试数据
        insertTestData();
    }
    
    @AfterEach
    void tearDown() {
        try {
            // 关闭事务管理器
            if (transactionManager != null) {
                // 尝试回滚未提交的事务
                try {
                    long xid = transactionManager.beginTransaction();
                    transactionManager.rollbackTransaction(xid);
                } catch (Exception ignored) {
                    // 忽略回滚异常
                }
            }
            
            // 关闭页面管理器
            if (pageManager instanceof PageManagerImpl) {
                ((PageManagerImpl) pageManager).close();
            }
            
            // 关闭日志管理器
            if (logManager != null) {
                logManager.close();
            }
            
            // 确保所有资源释放后再清理文件
            System.gc();
            Thread.sleep(100);
            
            // 删除测试数据库文件
            File dbFile = new File(dbFilePath);
            if (dbFile.exists()) {
                boolean deleted = dbFile.delete();
                if (!deleted) {
                    System.err.println("警告：无法删除数据库文件 " + dbFilePath);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    /**
     * 创建测试表
     */
    private void createTestTable() {
        // 创建用户表
        Table usersTable = Table.builder()
                .name("users")
                .columns(Arrays.asList(
                        Column.builder()
                                .name("id")
                                .dataType(Column.DataType.INT)
                                .primaryKey(true)
                                .nullable(false)
                                .build(),
                        Column.builder()
                                .name("username")
                                .dataType(Column.DataType.VARCHAR)
                                .length(50)
                                .nullable(false)
                                .build(),
                        Column.builder()
                                .name("email")
                                .dataType(Column.DataType.VARCHAR)
                                .length(100)
                                .nullable(true)
                                .build(),
                        Column.builder()
                                .name("age")
                                .dataType(Column.DataType.INT)
                                .nullable(true)
                                .build(),
                        Column.builder()
                                .name("active")
                                .dataType(Column.DataType.BOOLEAN)
                                .defaultValue("true")
                                .nullable(false)
                                .build()
                ))
                .build();
        
        // 创建表
        tableManager.createTable(usersTable, transactionManager);
    }
    
    /**
     * 插入测试数据
     */
    private void insertTestData() {
        // 执行插入SQL
        queryCore.executeUpdate("INSERT INTO users (id, username, email, age, active) VALUES (1, 'alice', 'alice@example.com', 30, true)");
        queryCore.executeUpdate("INSERT INTO users (id, username, email, age, active) VALUES (2, 'bob', 'bob@example.com', 25, true)");
        queryCore.executeUpdate("INSERT INTO users (id, username, email, age, active) VALUES (3, 'charlie', 'charlie@example.com', 35, false)");
    }
    
    @Test
    @DisplayName("测试表管理器与UPDATE操作的集成")
    void testTableManagerWithUpdate() {
        // 1. 先查询原始记录
        List<Record> originalRecords = queryCore.executeSelect("users", "id = 1", null);
        assertEquals(1, originalRecords.size(), "应找到1条记录");
        assertEquals(30, originalRecords.get(0).getFieldValue("age"), "原始age应为30");
        
        // 2. 执行UPDATE
        int affectedRows = queryCore.executeUpdate("UPDATE users SET age = 31 WHERE id = 1");
        assertEquals(1, affectedRows, "应影响1行");
        
        // 3. 验证更新结果
        List<Record> updatedRecords = queryCore.executeSelect("users", "id = 1", null);
        assertEquals(1, updatedRecords.size(), "应找到1条记录");
        assertEquals(31, updatedRecords.get(0).getFieldValue("age"), "更新后age应为31");
    }
    
    @Test
    @DisplayName("测试表管理器与UPDATE多个字段的集成")
    void testTableManagerWithMultiFieldUpdate() {
        // 执行UPDATE更新多个字段
        int affectedRows = queryCore.executeUpdate("UPDATE users SET age = 31, email = 'alice.new@example.com', active = false WHERE id = 1");
        assertEquals(1, affectedRows, "应影响1行");
        
        // 验证更新结果
        List<Record> updatedRecords = queryCore.executeSelect("users", "id = 1", null);
        assertEquals(1, updatedRecords.size(), "应找到1条记录");
        Record updatedRecord = updatedRecords.get(0);
        
        assertEquals(31, updatedRecord.getFieldValue("age"), "更新后age应为31");
        assertEquals("alice.new@example.com", updatedRecord.getFieldValue("email"), "更新后email应正确");
        assertEquals(false, updatedRecord.getFieldValue("active"), "更新后active应为false");
    }
    
    @Test
    @DisplayName("测试表管理器与条件更新的集成")
    void testTableManagerWithConditionalUpdate() {
        // 执行带复杂条件的UPDATE
        int affectedRows = queryCore.executeUpdate("UPDATE users SET active = false WHERE age > 25");
        assertEquals(2, affectedRows, "应影响2行");
        
        // 验证更新结果 - 查询所有记录
        List<Record> allRecords = queryCore.executeSelect("users", null, null);
        assertEquals(3, allRecords.size(), "应有3条记录");
        
        // 统计active为false的记录数
        int falseCount = 0;
        for (Record record : allRecords) {
            if (record.getFieldValue("active").equals(false)) {
                falseCount++;
            }
        }
        
        assertEquals(3, falseCount, "应有3条记录的active为false");
        
        // 验证具体记录
        List<Record> filtered = queryCore.executeSelect("users", "id = 1 OR id = 3", null);
        assertEquals(2, filtered.size(), "应找到2条记录");
        for (Record record : filtered) {
            assertEquals(false, record.getFieldValue("active"), "id为1或3的记录active应为false");
        }
    }
    
    @Test
    @DisplayName("测试表管理器与事务的集成")
    void testTableManagerWithTransaction() {
        // 开始事务
        long xid = transactionManager.beginTransaction();
        
        try {
            // 在事务中执行更新
            TableManagerImpl tableManagerImpl = (TableManagerImpl) tableManager;
            List<Record> records = tableManagerImpl.findRecords("users", "id = 1", recordManager, transactionManager);
            assertEquals(1, records.size(), "应找到1条记录");
            
            // 准备更新值
            Map<String, Object> updateValues = new HashMap<>();
            updateValues.put("age", 32);
            updateValues.put("email", "alice.tx@example.com");
            
            // 更新记录
            Record updatedRecord = tableManagerImpl.updateRecord(
                "users",
                records.get(0),
                updateValues,
                recordManager,
                transactionManager
            );
            
            // 验证事务内可以看到更新
            List<Record> recordsInTx = tableManagerImpl.findRecords("users", "id = 1", recordManager, transactionManager);
            assertEquals(32, recordsInTx.get(0).getFieldValue("age"), "事务内应能看到更新");
            
            // 回滚事务
            transactionManager.rollbackTransaction(xid);
            
            // 验证更新被回滚
            List<Record> recordsAfterRollback = queryCore.executeSelect("users", "id = 1", null);
            assertEquals(30, recordsAfterRollback.get(0).getFieldValue("age"), "回滚后应恢复原值");
            assertEquals("alice@example.com", recordsAfterRollback.get(0).getFieldValue("email"), "回滚后应恢复原值");
        } catch (Exception e) {
            // 确保事务被回滚
            transactionManager.rollbackTransaction(xid);
            fail("事务测试失败: " + e.getMessage());
        }
    }
    
    @Test
    @DisplayName("测试表管理器与NULL值处理的集成")
    void testTableManagerWithNullValues() {
        // 执行UPDATE将email设为NULL
        int affectedRows = queryCore.executeUpdate("UPDATE users SET email = null WHERE id = 1");
        assertEquals(1, affectedRows, "应影响1行");
        
        // 验证更新结果
        List<Record> updatedRecords = queryCore.executeSelect("users", "id = 1", null);
        assertEquals(1, updatedRecords.size(), "应找到1条记录");
        assertNull(updatedRecords.get(0).getFieldValue("email"), "更新后email应为null");
    }
} 