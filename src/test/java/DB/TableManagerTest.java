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
 * 表管理器测试类
 */
public class TableManagerTest {
    
    private PageManager pageManager;
    private RecordManager recordManager;
    private TableManager tableManager;
    private TransactionManager transactionManager;
    private LogManagerImpl logManager;
    private LockManager lockManager;
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
        
        // 创建表管理器
        tableManager = new TableManagerImpl(pageManager);
        
        // 创建测试表
        createTestTable();
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
     * 插入测试记录
     */
    private Record insertTestRecord(Map<String, Object> values) throws Exception {
        TableManagerImpl tableManagerImpl = (TableManagerImpl) tableManager;
        
        // 创建记录
        Record record = new Record();
        for (Map.Entry<String, Object> entry : values.entrySet()) {
            record.setFieldValue(entry.getKey(), entry.getValue());
        }
        
        // 获取页面
        Page page = pageManager.readPage(0);
        if (page == null) {
            page = pageManager.createPage();
        }
        
        // 设置记录所在页面ID
        record.setPageId(page.getHeader().getPageId());
        
        // 将记录序列化
        byte[] data = ("id=" + values.get("id") + 
                       ";username=" + values.get("username") + 
                       ";email=" + values.get("email") + 
                       ";age=" + values.get("age") + 
                       ";active=" + values.get("active")).getBytes();
        
        // 插入记录
        long xid = transactionManager.beginTransaction();
        Record insertedRecord = recordManager.insertRecord(page, data, xid);
        
        // 复制字段值
        insertedRecord.setFields(new HashMap<>(record.getFields()));
        
        // 提交事务
        transactionManager.commitTransaction(xid);
        
        return insertedRecord;
    }
    
    @Test
    @DisplayName("测试创建表")
    void testCreateTable() {
        // 获取创建的表
        Table table = tableManager.getTable("users");
        
        // 验证表名
        assertNotNull(table, "表应该成功创建");
        assertEquals("users", table.getName(), "表名应为users");
        
        // 验证列
        List<Column> columns = table.getColumns();
        assertEquals(5, columns.size(), "应有5列");
        
        // 验证列名
        List<String> columnNames = new ArrayList<>();
        for (Column column : columns) {
            columnNames.add(column.getName());
        }
        assertTrue(columnNames.contains("id"), "应包含id列");
        assertTrue(columnNames.contains("username"), "应包含username列");
        assertTrue(columnNames.contains("email"), "应包含email列");
        assertTrue(columnNames.contains("age"), "应包含age列");
        assertTrue(columnNames.contains("active"), "应包含active列");
    }
    
    @Test
    @DisplayName("测试查找记录")
    void testFindRecords() throws Exception {
        // 插入测试记录
        insertTestRecord(new HashMap<String, Object>() {{
            put("id", 1);
            put("username", "alice");
            put("email", "alice@example.com");
            put("age", 30);
            put("active", true);
        }});
        
        insertTestRecord(new HashMap<String, Object>() {{
            put("id", 2);
            put("username", "bob");
            put("email", "bob@example.com");
            put("age", 25);
            put("active", true);
        }});
        
        insertTestRecord(new HashMap<String, Object>() {{
            put("id", 3);
            put("username", "charlie");
            put("email", "charlie@example.com");
            put("age", 35);
            put("active", false);
        }});
        
        // 转换为TableManagerImpl以使用findRecords方法
        TableManagerImpl tableManagerImpl = (TableManagerImpl) tableManager;
        
        // 测试无条件查询
        List<Record> allRecords = tableManagerImpl.findRecords("users", null, recordManager, transactionManager);
        assertEquals(3, allRecords.size(), "应找到3条记录");
        
        // 测试条件查询 - id = 1
        List<Record> filteredById = tableManagerImpl.findRecords("users", "id = 1", recordManager, transactionManager);
        assertEquals(1, filteredById.size(), "应找到1条记录");
        assertEquals(1, filteredById.get(0).getFieldValue("id"), "记录id应为1");
        assertEquals("alice", filteredById.get(0).getFieldValue("username"), "记录username应为alice");
        
        // 测试条件查询 - age > 25
        List<Record> filteredByAge = tableManagerImpl.findRecords("users", "age > 25", recordManager, transactionManager);
        assertEquals(2, filteredByAge.size(), "应找到2条记录");
        
        // 测试条件查询 - active = true
        List<Record> filteredByActive = tableManagerImpl.findRecords("users", "active = true", recordManager, transactionManager);
        assertEquals(2, filteredByActive.size(), "应找到2条记录");
    }
    
    @Test
    @DisplayName("测试更新记录")
    void testUpdateRecord() throws Exception {
        // 插入测试记录
        Record record = insertTestRecord(new HashMap<String, Object>() {{
            put("id", 1);
            put("username", "alice");
            put("email", "alice@example.com");
            put("age", 30);
            put("active", true);
        }});
        
        // 准备更新值
        Map<String, Object> updateValues = new HashMap<>();
        updateValues.put("age", 31);
        updateValues.put("email", "alice.new@example.com");
        
        // 转换为TableManagerImpl以使用updateRecord方法
        TableManagerImpl tableManagerImpl = (TableManagerImpl) tableManager;
        
        // 更新记录
        Record updatedRecord = tableManagerImpl.updateRecord(
                "users",
                record,
                updateValues,
                recordManager,
                transactionManager
        );
        
        // 验证更新后的记录
        assertNotNull(updatedRecord, "更新后的记录不应为空");
        assertEquals(1, updatedRecord.getFieldValue("id"), "记录id应保持不变");
        assertEquals("alice", updatedRecord.getFieldValue("username"), "记录username应保持不变");
        assertEquals("alice.new@example.com", updatedRecord.getFieldValue("email"), "记录email应已更新");
        assertEquals(31, updatedRecord.getFieldValue("age"), "记录age应已更新");
        assertEquals(true, updatedRecord.getFieldValue("active"), "记录active应保持不变");
        
        // 查询验证更新
        List<Record> records = tableManagerImpl.findRecords("users", "id = 1", recordManager, transactionManager);
        assertEquals(1, records.size(), "应找到1条记录");
        assertEquals(31, records.get(0).getFieldValue("age"), "记录在数据库中的age应已更新");
        assertEquals("alice.new@example.com", records.get(0).getFieldValue("email"), "记录在数据库中的email应已更新");
    }
    
    @Test
    @DisplayName("测试不同类型条件的匹配")
    void testConditionMatching() throws Exception {
        // 插入不同类型的测试记录
        insertTestRecord(new HashMap<String, Object>() {{
            put("id", 1);
            put("username", "alice");
            put("email", "alice@example.com");
            put("age", 30);
            put("active", true);
        }});
        
        insertTestRecord(new HashMap<String, Object>() {{
            put("id", 2);
            put("username", "bob");
            put("email", "bob@example.com");
            put("age", 25);
            put("active", true);
        }});
        
        insertTestRecord(new HashMap<String, Object>() {{
            put("id", 3);
            put("username", "charlie");
            put("email", null);
            put("age", 35);
            put("active", false);
        }});
        
        // 转换为TableManagerImpl以使用findRecords方法
        TableManagerImpl tableManagerImpl = (TableManagerImpl) tableManager;
        
        // 测试整数类型条件 - 等于
        List<Record> equalInt = tableManagerImpl.findRecords("users", "id = 2", recordManager, transactionManager);
        assertEquals(1, equalInt.size(), "应找到1条记录");
        assertEquals(2, equalInt.get(0).getFieldValue("id"), "记录id应为2");
        
        // 测试整数类型条件 - 大于
        List<Record> greaterInt = tableManagerImpl.findRecords("users", "age > 30", recordManager, transactionManager);
        assertEquals(1, greaterInt.size(), "应找到1条记录");
        assertEquals(3, greaterInt.get(0).getFieldValue("id"), "记录id应为3");
        
        // 测试字符串类型条件
        List<Record> stringEquals = tableManagerImpl.findRecords("users", "username = 'alice'", recordManager, transactionManager);
        assertEquals(1, stringEquals.size(), "应找到1条记录");
        assertEquals("alice", stringEquals.get(0).getFieldValue("username"), "记录username应为alice");
        
        // 测试布尔类型条件
        List<Record> booleanEquals = tableManagerImpl.findRecords("users", "active = false", recordManager, transactionManager);
        assertEquals(1, booleanEquals.size(), "应找到1条记录");
        assertEquals(3, booleanEquals.get(0).getFieldValue("id"), "记录id应为3");
    }
} 