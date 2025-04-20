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

import java.io.File;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 表结构和操作测试类
 */
public class TableTest {
    
    private PageManager pageManager;
    private RecordManager recordManager;
    private TableManager tableManager;
    private TableManagerImpl tableManagerImpl;
    private TransactionManager transactionManager;
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
        tableManagerImpl = (TableManagerImpl) tableManager;
        indexManager = new BPlusTreeIndex(100);
    }
    
    @AfterEach
    void tearDown() throws Exception {
        if (pageManager != null) {
            // 关闭页面管理器
            ((PageManagerImpl) pageManager).close();
        }
    }
    
    @Test
    @DisplayName("测试创建基本表")
    void testCreateBasicTable() {
        // 创建一个简单表结构
        Table simpleTable = Table.builder()
                .name("simple_table")
                .columns(Arrays.asList(
                        Column.builder()
                                .name("id")
                                .dataType(Column.DataType.INT)
                                .primaryKey(true)
                                .nullable(false)
                                .build(),
                        Column.builder()
                                .name("name")
                                .dataType(Column.DataType.VARCHAR)
                                .length(50)
                                .nullable(false)
                                .build()
                ))
                .build();
        
        // 创建表
        tableManager.createTable(simpleTable, transactionManager);
        
        // 验证表是否创建成功
        Table retrievedTable = tableManager.getTable("simple_table");
        assertNotNull(retrievedTable, "表应该成功创建");
        assertEquals("simple_table", retrievedTable.getName(), "表名应匹配");
        assertEquals(2, retrievedTable.getColumns().size(), "列数应为2");
        
        // 验证列定义
        Column idColumn = null;
        Column nameColumn = null;
        
        for (Column column : retrievedTable.getColumns()) {
            if (column.getName().equals("id")) {
                idColumn = column;
            } else if (column.getName().equals("name")) {
                nameColumn = column;
            }
        }
        
        assertNotNull(idColumn, "id列应存在");
        assertNotNull(nameColumn, "name列应存在");
        
        assertEquals(Column.DataType.INT, idColumn.getDataType(), "id列类型应为INT");
        assertTrue(idColumn.isPrimaryKey(), "id列应为主键");
        assertFalse(idColumn.isNullable(), "id列不应为可空");
        
        assertEquals(Column.DataType.VARCHAR, nameColumn.getDataType(), "name列类型应为VARCHAR");
        assertEquals(50, nameColumn.getLength(), "name列长度应为50");
        assertFalse(nameColumn.isNullable(), "name列不应为可空");
    }
    
    @Test
    @DisplayName("测试创建完整表结构")
    void testCreateFullFeaturedTable() {
        // 创建一个完整特性的表结构
        Table fullTable = Table.builder()
                .name("full_table")
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
                                .unique(true)
                                .nullable(false)
                                .build(),
                        Column.builder()
                                .name("email")
                                .dataType(Column.DataType.VARCHAR)
                                .length(100)
                                .unique(true)
                                .nullable(true)
                                .build(),
                        Column.builder()
                                .name("age")
                                .dataType(Column.DataType.INT)
                                .nullable(true)
                                .defaultValue("18")
                                .build(),
                        Column.builder()
                                .name("active")
                                .dataType(Column.DataType.BOOLEAN)
                                .defaultValue("true")
                                .nullable(false)
                                .build(),
                        Column.builder()
                                .name("created_at")
                                .dataType(Column.DataType.TIMESTAMP)
                                .nullable(false)
                                .build()
                ))
                .build();
        
        // 创建表
        tableManager.createTable(fullTable, transactionManager);
        
        // 验证表是否创建成功
        Table retrievedTable = tableManager.getTable("full_table");
        assertNotNull(retrievedTable, "表应该成功创建");
        assertEquals("full_table", retrievedTable.getName(), "表名应匹配");
        assertEquals(6, retrievedTable.getColumns().size(), "列数应为6");
        
        // 验证唯一列
        Column usernameColumn = null;
        Column emailColumn = null;
        
        for (Column column : retrievedTable.getColumns()) {
            if (column.getName().equals("username")) {
                usernameColumn = column;
            } else if (column.getName().equals("email")) {
                emailColumn = column;
            }
        }
        
        assertNotNull(usernameColumn, "username列应存在");
        assertNotNull(emailColumn, "email列应存在");
        
        assertTrue(usernameColumn.isUnique(), "username列应为唯一");
        assertTrue(emailColumn.isUnique(), "email列应为唯一");
    }
    
    @Test
    @DisplayName("测试表插入和查询记录")
    void testInsertAndQueryRecords() throws Exception {
        // 创建测试表
        createTestTable();
        
        // 插入记录
        insertTestRecords();
        
        // 查询所有记录
        List<Record> allRecords = tableManagerImpl.findRecords(
            "users", null, recordManager, transactionManager
        );
        
        // 验证记录数量
        assertEquals(3, allRecords.size(), "应该插入了3条记录");
        
        // 查询条件记录
        List<Record> activeUsers = tableManagerImpl.findRecords(
            "users", "active = true", recordManager, transactionManager
        );
        
        // 验证满足条件的记录数量
        assertEquals(2, activeUsers.size(), "应该有2个活跃用户");
        
        // 验证字段值
        boolean foundAlice = false;
        boolean foundBob = false;
        
        for (Record record : activeUsers) {
            if (record.getFieldValue("username").equals("alice")) {
                foundAlice = true;
                assertEquals(30, record.getFieldValue("age"), "Alice的年龄应为30");
            } else if (record.getFieldValue("username").equals("bob")) {
                foundBob = true;
                assertEquals(25, record.getFieldValue("age"), "Bob的年龄应为25");
            }
        }
        
        assertTrue(foundAlice, "应找到用户Alice");
        assertTrue(foundBob, "应找到用户Bob");
    }
    
    @Test
    @DisplayName("测试删除表")
    void testDropTable() {
        // 创建测试表
        createTestTable();
        
        // 验证表存在
        assertNotNull(tableManager.getTable("users"), "表应存在");
        
        // 删除表
        tableManager.dropTable("users", transactionManager);
        
        // 验证表已删除
        assertNull(tableManager.getTable("users"), "表应该已删除");
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
        // 插入用户记录
        insertUserRecord(new HashMap<String, Object>() {{
            put("id", 1);
            put("username", "alice");
            put("email", "alice@example.com");
            put("age", 30);
            put("active", true);
        }});
        
        insertUserRecord(new HashMap<String, Object>() {{
            put("id", 2);
            put("username", "bob");
            put("email", "bob@example.com");
            put("age", 25);
            put("active", true);
        }});
        
        insertUserRecord(new HashMap<String, Object>() {{
            put("id", 3);
            put("username", "charlie");
            put("email", "charlie@example.com");
            put("age", 35);
            put("active", false);
        }});
    }
    
    /**
     * 插入单条用户记录
     * 注意：由于RecordManager的insertRecord方法签名是(Page, byte[], long)，
     * 我们需要使用模拟实现方式，或者直接使用字节数组
     */
    private void insertUserRecord(Map<String, Object> values) throws Exception {
        // 获取用户表
        Table usersTable = tableManager.getTable("users");
        assertNotNull(usersTable, "用户表应存在");
        
        // 创建记录
        Record record = new Record();
        
        // 设置字段值
        for (Map.Entry<String, Object> entry : values.entrySet()) {
            record.setFieldValue(entry.getKey(), entry.getValue());
        }
        
        // 创建一个新页面
        Page page = pageManager.createPage();
        
        // 序列化记录为字节数组（简化实现）
        byte[] data = serializeRecord(record);
        
        // 获取事务ID
        long xid = transactionManager.beginTransaction();
        
        // 插入记录
        recordManager.insertRecord(page, data, xid);
        
        // 提交事务
        transactionManager.commitTransaction(xid);
    }
    
    /**
     * 序列化记录为字节数组（简化实现）
     */
    private byte[] serializeRecord(Record record) {
        // 这里只是一个简化的实现，实际情况下需要根据表结构和字段类型进行序列化
        // 返回一个简单的字节数组，仅用于测试
        return new byte[100]; // 返回一个固定大小的字节数组
    }
} 