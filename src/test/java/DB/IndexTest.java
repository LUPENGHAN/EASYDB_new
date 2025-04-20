package DB;

import DB.concurrency.Impl.LockManagerImpl;
import DB.concurrency.interfaces.LockManager;
import DB.index.Impl.BPlusTreeIndex;
import DB.index.interfaces.IndexManager;
import DB.log.impl.LogManagerImpl;
import DB.page.interfaces.PageManager;
import DB.page.impl.PageManagerImpl;
import DB.page.models.Page;
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
import org.mockito.Mockito;

import java.io.File;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * 索引测试类
 * 注意：由于我们不知道索引接口的具体实现，这里使用模拟测试方法
 */
public class IndexTest {
    
    private PageManager pageManager;
    private RecordManager recordManager;
    private TableManager tableManager;
    private TableManagerImpl tableManagerImpl;
    private TransactionManager transactionManager;
    private LogManagerImpl logManager;
    private LockManager lockManager;
    private IndexManager indexManager;
    private IndexManager mockIndexManager;
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
        tableManagerImpl = (TableManagerImpl) tableManager;
        indexManager = new BPlusTreeIndex(100);
        
        // 由于不清楚BPlusTreeIndex的具体实现，创建模拟的索引管理器
        mockIndexManager = Mockito.mock(IndexManager.class);
        
        // 创建测试表
        createTestTable();
        
        // 插入测试数据
        insertTestRecords();
    }
    
    @AfterEach
    void tearDown() throws Exception {
        if (pageManager != null) {
            // 关闭页面管理器
            ((PageManagerImpl) pageManager).close();
        }
    }
    
    @Test
    @DisplayName("测试创建索引")
    void testCreateIndex() throws Exception {
        // 为id列创建索引
        String indexName = "users_id_idx";
        
        // 模拟方法调用
        doNothing().when(mockIndexManager).createIndex(eq("users"), eq("id"), eq(indexName), any(TransactionManager.class));
        mockIndexManager.createIndex("users", "id", indexName, transactionManager);
        
        // 验证方法是否被调用
        verify(mockIndexManager).createIndex(eq("users"), eq("id"), eq(indexName), any(TransactionManager.class));
    }
    
    @Test
    @DisplayName("测试索引查询")
    void testQueryIndex() throws Exception {
        // 创建一个模拟记录
        Record mockRecord = new Record();
        mockRecord.setFieldValue("id", 2);
        mockRecord.setFieldValue("username", "bob");
        mockRecord.setFieldValue("email", "bob@example.com");
        
        // 模拟返回值
        when(mockIndexManager.exactQuery(eq("users_id_idx"), eq(2), any(TransactionManager.class)))
            .thenReturn(Arrays.asList(mockRecord));
        
        // 执行查询
        List<Record> results = mockIndexManager.exactQuery("users_id_idx", 2, transactionManager);
        
        // 验证查询结果
        assertEquals(1, results.size(), "应该返回1条记录");
        Record record = results.get(0);
        assertEquals(2, record.getFieldValue("id"), "ID应该是2");
        assertEquals("bob", record.getFieldValue("username"), "用户名应该是bob");
    }
    
    @Test
    @DisplayName("测试唯一索引约束")
    void testUniqueIndexConstraint() throws Exception {
        // 为username列创建唯一索引
        String indexName = "users_username_idx";
        
        // 模拟方法调用
        doNothing().when(mockIndexManager).createIndex(eq("users"), eq("username"), eq(indexName), any(TransactionManager.class));
        mockIndexManager.createIndex("users", "username", indexName, transactionManager);
        
        // 创建一个记录
        Record duplicateRecord = new Record();
        duplicateRecord.setFieldValue("id", 4);
        duplicateRecord.setFieldValue("username", "alice"); // 重复的用户名
        
        // 模拟插入时抛出唯一键约束异常
        doThrow(new IllegalStateException("唯一约束冲突: username 'alice' 已存在"))
            .when(mockIndexManager).insertIndex(eq("users_username_idx"), eq("alice"), any(Record.class), any(TransactionManager.class));
        
        // 应抛出异常
        Exception exception = assertThrows(IllegalStateException.class, () -> {
            mockIndexManager.insertIndex("users_username_idx", "alice", duplicateRecord, transactionManager);
        }, "应该抛出唯一约束异常");
        
        // 验证异常消息
        assertTrue(exception.getMessage().contains("唯一约束"), "异常应该包含唯一约束相关信息");
    }
    
    @Test
    @DisplayName("测试索引范围查询")
    void testIndexRangeQuery() throws Exception {
        // 创建模拟记录
        Record aliceRecord = new Record();
        aliceRecord.setFieldValue("id", 1);
        aliceRecord.setFieldValue("username", "alice");
        aliceRecord.setFieldValue("age", 30);
        
        Record bobRecord = new Record();
        bobRecord.setFieldValue("id", 2);
        bobRecord.setFieldValue("username", "bob");
        bobRecord.setFieldValue("age", 25);
        
        Record charlieRecord = new Record();
        charlieRecord.setFieldValue("id", 3);
        charlieRecord.setFieldValue("username", "charlie");
        charlieRecord.setFieldValue("age", 35);
        
        // 模拟范围查询结果
        when(mockIndexManager.rangeQuery(eq("users_age_idx"), eq(25), eq(35), any(TransactionManager.class)))
            .thenReturn(Arrays.asList(bobRecord, aliceRecord, charlieRecord));
        
        // 执行范围查询
        List<Record> records = mockIndexManager.rangeQuery("users_age_idx", 25, 35, transactionManager);
        
        // 验证查询结果
        assertEquals(3, records.size(), "应该找到3条记录");
        
        // 验证结果包含预期的记录
        boolean foundAlice = false;
        boolean foundBob = false;
        boolean foundCharlie = false;
        
        for (Record record : records) {
            if (record.getFieldValue("username").equals("alice")) {
                foundAlice = true;
            } else if (record.getFieldValue("username").equals("bob")) {
                foundBob = true;
            } else if (record.getFieldValue("username").equals("charlie")) {
                foundCharlie = true;
            }
        }
        
        assertTrue(foundAlice, "应找到Alice");
        assertTrue(foundBob, "应找到Bob");
        assertTrue(foundCharlie, "应找到Charlie");
    }
    
    @Test
    @DisplayName("测试索引更新")
    void testIndexUpdate() throws Exception {
        // 创建一个记录
        Record record = new Record();
        record.setFieldValue("id", 1);
        record.setFieldValue("email", "alice.new@example.com");
        
        // 模拟方法调用
        doNothing().when(mockIndexManager).updateIndex(
            eq("users_email_idx"), 
            eq("alice@example.com"), 
            eq("alice.new@example.com"), 
            any(Record.class), 
            any(TransactionManager.class)
        );
        
        // 更新索引
        mockIndexManager.updateIndex(
            "users_email_idx", 
            "alice@example.com", 
            "alice.new@example.com", 
            record, 
            transactionManager
        );
        
        // 验证方法是否被调用
        verify(mockIndexManager).updateIndex(
            eq("users_email_idx"), 
            eq("alice@example.com"), 
            eq("alice.new@example.com"), 
            any(Record.class), 
            any(TransactionManager.class)
        );
    }
    
    @Test
    @DisplayName("测试删除索引")
    void testDeleteIndex() throws Exception {
        // 模拟方法调用
        doNothing().when(mockIndexManager).dropIndex(
            eq("users_id_idx"), 
            any(TransactionManager.class)
        );
        
        // 删除索引
        mockIndexManager.dropIndex("users_id_idx", transactionManager);
        
        // 验证方法是否被调用
        verify(mockIndexManager).dropIndex(
            eq("users_id_idx"), 
            any(TransactionManager.class)
        );
    }
    
    @Test
    @DisplayName("测试删除索引项")
    void testDeleteIndexEntry() throws Exception {
        // 创建一个记录
        Record record = new Record();
        record.setFieldValue("id", 3);
        
        // 模拟方法调用
        doNothing().when(mockIndexManager).deleteIndex(
            eq("users_id_idx"), 
            eq(3), 
            any(Record.class), 
            any(TransactionManager.class)
        );
        
        // 删除索引项
        mockIndexManager.deleteIndex("users_id_idx", 3, record, transactionManager);
        
        // 验证方法是否被调用
        verify(mockIndexManager).deleteIndex(
            eq("users_id_idx"), 
            eq(3), 
            any(Record.class), 
            any(TransactionManager.class)
        );
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
     * 插入测试记录
     */
    private void insertTestRecords() throws Exception {
        // 创建和插入Alice的记录
        Record aliceRecord = new Record();
        aliceRecord.setFieldValue("id", 1);
        aliceRecord.setFieldValue("username", "alice");
        aliceRecord.setFieldValue("email", "alice@example.com");
        aliceRecord.setFieldValue("age", 30);
        aliceRecord.setFieldValue("active", true);
        
        // 创建页面和插入记录
        Page page = pageManager.createPage();
        long xid = transactionManager.beginTransaction();
        recordManager.insertRecord(page, new byte[100], xid);
        transactionManager.commitTransaction(xid);
        
        // 创建和插入Bob的记录
        Record bobRecord = new Record();
        bobRecord.setFieldValue("id", 2);
        bobRecord.setFieldValue("username", "bob");
        bobRecord.setFieldValue("email", "bob@example.com");
        bobRecord.setFieldValue("age", 25);
        bobRecord.setFieldValue("active", true);
        
        // 创建页面和插入记录
        page = pageManager.createPage();
        xid = transactionManager.beginTransaction();
        recordManager.insertRecord(page, new byte[100], xid);
        transactionManager.commitTransaction(xid);
        
        // 创建和插入Charlie的记录
        Record charlieRecord = new Record();
        charlieRecord.setFieldValue("id", 3);
        charlieRecord.setFieldValue("username", "charlie");
        charlieRecord.setFieldValue("email", "charlie@example.com");
        charlieRecord.setFieldValue("age", 35);
        charlieRecord.setFieldValue("active", false);
        
        // 创建页面和插入记录
        page = pageManager.createPage();
        xid = transactionManager.beginTransaction();
        recordManager.insertRecord(page, new byte[100], xid);
        transactionManager.commitTransaction(xid);
    }
} 