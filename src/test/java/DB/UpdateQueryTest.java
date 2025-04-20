package DB;

import DB.concurrency.Impl.LockManagerImpl;
import DB.concurrency.interfaces.LockManager;
import DB.index.Impl.BPlusTreeIndex;
import DB.index.interfaces.IndexManager;
import DB.log.impl.LogManagerImpl;
import DB.page.interfaces.PageManager;
import DB.page.impl.PageManagerImpl;
import DB.page.models.Page;
import DB.query.interfaces.QueryComponents;
import DB.query.interfaces.QueryComponents.QueryPlan;
import DB.query.interfaces.QueryComponents.QueryType;
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
 * UPDATE查询测试类
 */
public class UpdateQueryTest {
    
    private PageManager pageManager;
    private RecordManager recordManager;
    private TableManager tableManager;
    private TransactionManager transactionManager;
    private QueryComponents.QueryParser queryParser;
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
        
        // 创建查询解析器
        queryParser = new QueryParserImpl();
        
        // 创建查询核心
        queryCore = new QueryCore(
            tableManager,
            indexManager,
            recordManager,
            queryParser,
            transactionManager
        );
        
        // 创建测试表和数据
        createTestTableAndData();
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
     * 创建测试表和数据
     */
    private void createTestTableAndData() throws Exception {
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
        
        // 插入测试记录
        insertTestRecords();
    }
    
    /**
     * 插入测试记录
     */
    private void insertTestRecords() throws Exception {
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
    }
    
    /**
     * 插入测试记录
     */
    private Record insertTestRecord(Map<String, Object> values) throws Exception {
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
    @DisplayName("测试解析UPDATE查询")
    void testParseUpdateQuery() {
        // 测试UPDATE查询解析
        String updateQuery = "UPDATE users SET age = 31, email = 'new@example.com' WHERE id = 1";
        
        // 解析查询
        QueryComponents.QueryParser.UpdateQueryData updateData = queryParser.parseUpdateQuery(updateQuery);
        
        // 验证解析结果
        assertEquals("users", updateData.getTableName(), "表名应为users");
        assertEquals("id = 1", updateData.getCondition(), "条件应为id = 1");
        
        // 验证SET子句
        Map<String, String> setValues = updateData.getSetValues();
        assertEquals(2, setValues.size(), "应有2个SET值");
        assertEquals("31", setValues.get("age"), "age应为31");
        assertEquals("'new@example.com'", setValues.get("email"), "email应为'new@example.com'");
    }
    
    @Test
    @DisplayName("测试优化UPDATE查询")
    void testOptimizeUpdateQuery() {
        // 测试UPDATE查询优化
        String updateQuery = "UPDATE users SET age = 31 WHERE id = 1";
        
        // 优化查询
        QueryPlan plan = queryCore.optimizeQuery(updateQuery, indexManager, transactionManager);
        
        // 验证优化结果
        assertEquals(QueryType.UPDATE, plan.getQueryType(), "查询类型应为UPDATE");
        assertEquals("users", plan.getTableName(), "表名应为users");
        assertEquals("id = 1", plan.getCondition(), "条件应为id = 1");
        assertNull(plan.getIndexName(), "索引名应为空"); // 因为我们没有创建索引
    }
    
    @Test
    @DisplayName("测试执行UPDATE查询")
    void testExecuteUpdateQuery() {
        // 测试执行UPDATE查询
        String updateQuery = "UPDATE users SET age = 31, email = 'new@example.com' WHERE id = 1";
        
        // 执行查询
        int affectedRows = queryCore.executeUpdate(updateQuery);
        
        // 验证影响的行数
        assertEquals(1, affectedRows, "应影响1行");
        
        // 验证更新结果
        List<Record> results = queryCore.executeSelect("users", "id = 1", null);
        
        assertNotNull(results, "查询结果不应为空");
        assertEquals(1, results.size(), "结果应有1条记录");
        
        Record updatedRecord = results.get(0);
        assertEquals(31, updatedRecord.getFieldValue("age"), "age应已更新为31");
        assertEquals("new@example.com", updatedRecord.getFieldValue("email"), "email应已更新");
    }
    
    @Test
    @DisplayName("测试批量更新")
    void testBatchUpdate() {
        // 测试批量更新多条记录
        String updateQuery = "UPDATE users SET active = false WHERE age > 25";
        
        // 执行查询
        int affectedRows = queryCore.executeUpdate(updateQuery);
        
        // 验证影响的行数
        assertEquals(2, affectedRows, "应影响2行");
        
        // 验证更新结果
        List<Record> results = queryCore.executeSelect("users", "age > 25", null);
        
        assertNotNull(results, "查询结果不应为空");
        assertEquals(2, results.size(), "结果应有2条记录");
        
        // 验证所有符合条件的记录都已更新
        for (Record record : results) {
            assertEquals(false, record.getFieldValue("active"), "active应已更新为false");
        }
    }
    
    @Test
    @DisplayName("测试更新不存在的表")
    void testUpdateNonExistentTable() {
        // 测试更新不存在的表
        String updateQuery = "UPDATE non_existent_table SET age = 31 WHERE id = 1";
        
        // 执行查询应抛出异常
        Exception exception = assertThrows(
            RuntimeException.class,
            () -> queryCore.executeUpdate(updateQuery)
        );
        
        // 验证异常信息
        assertTrue(exception.getMessage().contains("执行查询失败") || 
                  exception.getMessage().contains("表不存在"), 
                  "异常信息应包含'执行查询失败'或'表不存在'");
    }
    
    @Test
    @DisplayName("测试不带条件的UPDATE")
    void testUpdateWithoutCondition() {
        // 测试不带条件的UPDATE（更新所有记录）
        String updateQuery = "UPDATE users SET active = false";
        
        // 执行查询
        int affectedRows = queryCore.executeUpdate(updateQuery);
        
        // 验证影响的行数
        assertEquals(3, affectedRows, "应影响3行");
        
        // 验证更新结果
        List<Record> results = queryCore.executeSelect("users", null, null);
        
        assertNotNull(results, "查询结果不应为空");
        assertEquals(3, results.size(), "结果应有3条记录");
        
        // 验证所有记录都已更新
        for (Record record : results) {
            assertEquals(false, record.getFieldValue("active"), "active应已更新为false");
        }
    }
} 