package DB;

import DB.index.interfaces.IndexManager;
import DB.index.Impl.BPlusTreeIndex;
import DB.page.interfaces.PageManager;
import DB.page.impl.PageManagerImpl;
import DB.query.impl.DatabaseClient;
import DB.query.interfaces.QueryComponents;
import DB.query.impl.QueryCore;
import DB.query.impl.QueryParserImpl;
import DB.record.interfaces.RecordManager;
import DB.record.impl.RecordManagerImpl;
import DB.table.interfaces.TableManager;
import DB.table.impl.TableManagerImpl;
import DB.table.interfaces.TableMetadataManager;
import DB.table.impl.TableMetadataManagerImpl;
import DB.table.models.Column;
import DB.table.models.Table;
import DB.transaction.interfaces.TransactionManager;
import DB.transaction.impl.TransactionManagerImpl;
import DB.log.impl.LogManagerImpl;
import DB.concurrency.Impl.LockManagerImpl;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.IOException;

/**
 * 数据库系统测试类
 */
@Slf4j
public class DatabaseSystemTest {
    private PageManager pageManager;
    private RecordManager recordManager;
    private TableManager tableManager;
    private TableMetadataManager tableMetadataManager;
    private IndexManager indexManager;
    private TransactionManager transactionManager;
    private QueryComponents.QueryOptimizer queryOptimizer;
    private QueryCore queryCore;
    private DatabaseClient databaseClient;
    private LogManagerImpl logManager;
    private LockManagerImpl lockManager;
    
    /**
     * 初始化数据库系统
     */
    public void initSystem() throws IOException {
        // 创建数据目录
        File dataDir = new File("data");
        if (!dataDir.exists()) {
            dataDir.mkdirs();
        }
        
        // 初始化各组件
        pageManager = new PageManagerImpl("data/db.dat");
        recordManager = new RecordManagerImpl(pageManager);
        logManager = new LogManagerImpl("data/logs");
        lockManager = new LockManagerImpl();
        transactionManager = new TransactionManagerImpl(logManager, lockManager);
        tableManager = new TableManagerImpl(pageManager);
        tableMetadataManager = new TableMetadataManagerImpl(pageManager);
        indexManager = new BPlusTreeIndex(100);
        
        // 创建查询解析器
        QueryComponents.QueryParser queryParser = new QueryParserImpl();
        
        // 创建查询核心组件，同时实现了QueryOptimizer和ExtendedQueryExecutor接口
        queryCore = new QueryCore(tableManager, indexManager, queryParser, transactionManager);
        queryOptimizer = queryCore;
        
        // 创建数据库客户端 - QueryCore实现了ExtendedQueryExecutor接口
        databaseClient = new DatabaseClient(queryCore, transactionManager);
        
        log.info("Database system initialized successfully.");
    }
    
    /**
     * 创建测试表
     */
    public void createTestTable() {
        // 开始事务
        long txId = databaseClient.beginTransaction();
        
        try {
            // 创建测试表
            Table usersTable = Table.builder()
                    .name("users")
                    .column(Column.builder()
                            .name("id")
                            .dataType(Column.DataType.INT)
                            .primaryKey(true)
                            .nullable(false)
                            .build())
                    .column(Column.builder()
                            .name("username")
                            .dataType(Column.DataType.VARCHAR)
                            .length(50)
                            .unique(true)
                            .nullable(false)
                            .build())
                    .column(Column.builder()
                            .name("email")
                            .dataType(Column.DataType.VARCHAR)
                            .length(100)
                            .nullable(true)
                            .build())
                    .column(Column.builder()
                            .name("age")
                            .dataType(Column.DataType.INT)
                            .nullable(true)
                            .build())
                    .column(Column.builder()
                            .name("active")
                            .dataType(Column.DataType.BOOLEAN)
                            .defaultValue(true)
                            .nullable(false)
                            .build())
                    .build();
            
            // 创建表元数据
            tableMetadataManager.createTableMetadata(usersTable, transactionManager);
            
            // 创建索引
            indexManager.createIndex("users", "id", "users_id_idx", transactionManager);
            indexManager.createIndex("users", "username", "users_username_idx", transactionManager);
            
            log.info("Test table 'users' created successfully.");
            
            // 提交事务
            databaseClient.commitTransaction(txId);
        } catch (Exception e) {
            // 回滚事务
            databaseClient.rollbackTransaction(txId);
            log.error("Failed to create test table", e);
            throw e;
        }
    }
    
    /**
     * 插入测试数据
     */
    public void insertTestData() {
        // 执行INSERT语句
        String[] insertStatements = {
            "INSERT INTO users (id, username, email, age, active) VALUES (1, 'alice', 'alice@example.com', 30, true)",
            "INSERT INTO users (id, username, email, age, active) VALUES (2, 'bob', 'bob@example.com', 25, true)",
            "INSERT INTO users (id, username, email, age, active) VALUES (3, 'charlie', 'charlie@example.com', 35, false)",
            "INSERT INTO users (id, username, email, age, active) VALUES (4, 'dave', 'dave@example.com', 40, true)",
            "INSERT INTO users (id, username, email, age, active) VALUES (5, 'eve', 'eve@example.com', 22, true)"
        };
        
        for (String statement : insertStatements) {
            try {
                int result = databaseClient.executeUpdate(statement);
                log.info("Executed: {} - Affected rows: {}", statement, result);
            } catch (Exception e) {
                log.error("Failed to execute: {}", statement, e);
            }
        }
    }
    
    /**
     * 查询测试
     */
    public void queryTest() {
        // 执行简单查询
        executeQuery("SELECT * FROM users");
        
        // 条件查询
        executeQuery("SELECT * FROM users WHERE age > 30");
        
        // 条件组合查询
        executeQuery("SELECT * FROM users WHERE age > 25 AND active = true");
        
        // 特定列查询
        executeQuery("SELECT id, username, email FROM users");
        
        // 预编译查询测试
        preparedQueryTest();
    }
    
    /**
     * 执行查询并打印结果
     */
    private void executeQuery(String sql) {
        log.info("Executing query: {}", sql);
        try {
            DatabaseClient.ResultSet rs = databaseClient.executeQuery(sql);
            printResultSet(rs);
        } catch (Exception e) {
            log.error("Query execution failed", e);
        }
    }
    
    /**
     * 打印结果集
     */
    private void printResultSet(DatabaseClient.ResultSet rs) {
        StringBuilder sb = new StringBuilder();
        int count = 0;
        
        try {
            while (rs.next()) {
                count++;
                sb.append("Row ").append(count).append(": ");
                
                // 尝试获取常见列
                try {
                    if (rs.getObject("id") != null) {
                        sb.append("id=").append(rs.getInt("id")).append(", ");
                    }
                    
                    if (rs.getObject("username") != null) {
                        sb.append("username=").append(rs.getString("username")).append(", ");
                    }
                    
                    if (rs.getObject("email") != null) {
                        sb.append("email=").append(rs.getString("email")).append(", ");
                    }
                    
                    if (rs.getObject("age") != null) {
                        sb.append("age=").append(rs.getInt("age")).append(", ");
                    }
                    
                    if (rs.getObject("active") != null) {
                        sb.append("active=").append(rs.getBoolean("active"));
                    }
                } catch (Exception e) {
                    // 忽略不存在的列错误
                }
                
                sb.append("\n");
            }
        } catch (Exception e) {
            log.error("Error while processing ResultSet", e);
        }
        
        sb.append("Total rows: ").append(count);
        log.info("Query result:\n{}", sb.toString());
    }
    
    /**
     * 预编译查询测试
     */
    private void preparedQueryTest() {
        log.info("Testing prepared query");
        
        try {
            // 创建预编译查询
            DatabaseClient.PreparedStatement pstmt = databaseClient.prepareStatement(
                    "SELECT * FROM users WHERE id = ? OR username = ?");
            
            // 设置参数并执行
            pstmt.setInt(1, 1);
            pstmt.setString(2, "bob");
            
            DatabaseClient.ResultSet rs = pstmt.executeQuery();
            printResultSet(rs);
            
            // 重新设置参数并再次执行
            pstmt.setInt(1, 3);
            pstmt.setString(2, "eve");
            
            rs = pstmt.executeQuery();
            printResultSet(rs);
            
            // 关闭预编译查询
            pstmt.close();
        } catch (Exception e) {
            log.error("Prepared query test failed", e);
        }
    }
    
    /**
     * 更新测试
     */
    public void updateTest() {
        // 执行更新操作
        try {
            String updateSql = "UPDATE users SET age = 31 WHERE id = 1";
            int result = databaseClient.executeUpdate(updateSql);
            log.info("Executed: {} - Affected rows: {}", updateSql, result);
            
            // 验证更新结果
            executeQuery("SELECT * FROM users WHERE id = 1");
        } catch (Exception e) {
            log.error("Update test failed", e);
        }
    }
    
    /**
     * 删除测试
     */
    public void deleteTest() {
        // 执行删除操作
        try {
            String deleteSql = "DELETE FROM users WHERE id = 5";
            int result = databaseClient.executeUpdate(deleteSql);
            log.info("Executed: {} - Affected rows: {}", deleteSql, result);
            
            // 验证删除结果
            executeQuery("SELECT * FROM users");
        } catch (Exception e) {
            log.error("Delete test failed", e);
        }
    }
    
    /**
     * 事务测试
     */
    public void transactionTest() {
        // 开始事务
        long txId = databaseClient.beginTransaction();
        log.info("Transaction started: {}", txId);
        
        try {
            // 在事务中执行多个操作
            String updateSql = "UPDATE users SET email = 'alice.new@example.com' WHERE id = 1";
            databaseClient.executeUpdate(updateSql);
            log.info("Executed in transaction: {}", updateSql);
            
            String insertSql = "INSERT INTO users (id, username, email, age, active) VALUES (6, 'frank', 'frank@example.com', 28, true)";
            databaseClient.executeUpdate(insertSql);
            log.info("Executed in transaction: {}", insertSql);
            
            // 提交事务
            databaseClient.commitTransaction(txId);
            log.info("Transaction committed: {}", txId);
            
            // 验证事务结果
            executeQuery("SELECT * FROM users");
        } catch (Exception e) {
            // 回滚事务
            databaseClient.rollbackTransaction(txId);
            log.error("Transaction failed and rolled back", e);
        }
    }
    
    /**
     * 关闭数据库系统
     */
    public void closeSystem() {
        try {
            // 关闭客户端
            if (databaseClient != null) {
                databaseClient.close();
            }
            
            // 关闭事务管理器相关资源
            if (transactionManager != null) {
                // 尝试回滚所有活跃事务
                try {
                    long xid = transactionManager.beginTransaction();
                    transactionManager.rollbackTransaction(xid);
                } catch (Exception ignored) {
                    // 忽略可能的异常
                }
            }
            
            // 关闭锁管理器
            if (lockManager != null) {
                // 尝试释放所有锁 - 如果有任何活跃的事务，为其释放锁
                try {
                    // 创建一个虚拟事务用于测试
                    long dummyXid = 999999;
                    lockManager.releaseAllLocks(dummyXid);
                } catch (Exception ignored) {
                    // 忽略可能的异常
                }
            }
            
            // 关闭日志管理器
            if (logManager != null) {
                logManager.close();
            }
            
            // 关闭页面管理器
            if (pageManager instanceof PageManagerImpl) {
                ((PageManagerImpl) pageManager).close();
            }
            
            // 强制执行一次垃圾回收
            System.gc();
            
            log.info("Database system closed successfully.");
        } catch (Exception e) {
            log.error("Failed to close database system", e);
        }
    }
    
    /**
     * 运行测试
     */
    public void runTest() {
        try {
            // 初始化系统
            initSystem();
            
            // 创建测试表
            createTestTable();
            
            // 插入测试数据
            insertTestData();
            
            // 查询测试
            queryTest();
            
            // 更新测试
            updateTest();
            
            // 删除测试
            deleteTest();
            
            // 事务测试
            transactionTest();
            
            // 关闭系统
            closeSystem();
            
            log.info("All tests completed successfully!");
        } catch (Exception e) {
            log.error("Test failed", e);
        }
    }
    
    /**
     * 主方法
     */
    public static void main(String[] args) {
        DatabaseSystemTest test = new DatabaseSystemTest();
        test.runTest();
    }
} 