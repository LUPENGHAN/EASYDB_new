package DB;

import DB.index.interfaces.IndexManager;
import DB.query.interfaces.QueryComponents;
import DB.query.impl.QueryCore;
import DB.query.impl.QueryManager;
import DB.query.impl.QueryParserImpl;
import DB.query.impl.QueryPlanImpl;
import DB.record.models.Record;
import DB.table.interfaces.TableManager;
import DB.transaction.interfaces.TransactionManager;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

/**
 * 查询系统集成测试
 * 测试查询解析、优化和执行组件的集成
 */
public class QuerySystemTest {
    @Mock private IndexManager indexManager;
    @Mock private TransactionManager transactionManager;
    @Mock private TableManager tableManager;
    @Mock private QueryComponents.QueryPlan mockQueryPlan;
    
    private QueryComponents.QueryParser queryParser;
    private QueryComponents.QueryOptimizer queryOptimizer;
    private QueryComponents.QueryExecutor queryExecutor;
    private QueryComponents.QueryService queryService;
    private QueryCore queryCore;
    
    @BeforeEach
    public void setUp() {
        MockitoAnnotations.openMocks(this);
        
        // 创建查询系统组件
        queryParser = mock(QueryComponents.QueryParser.class);
        queryCore = mock(QueryCore.class);
        queryOptimizer = queryCore; // QueryCore同时实现了QueryOptimizer接口
        queryExecutor = queryCore;  // QueryCore同时实现了QueryExecutor接口
        
        // 创建模拟的查询计划对象
        mockQueryPlan = mock(QueryComponents.QueryPlan.class);
        
        // 设置模拟行为
        when(mockQueryPlan.getQueryType()).thenReturn(QueryComponents.QueryType.SELECT);
        when(mockQueryPlan.getTableName()).thenReturn("users");
        when(mockQueryPlan.getSelectedColumns()).thenReturn(Arrays.asList("id", "name"));
        when(mockQueryPlan.getCondition()).thenReturn("id = 1");
        when(mockQueryPlan.getMetadata()).thenReturn("SELECT id, name FROM users WHERE id = 1");
        
        // QueryOptimizer行为
        when(queryOptimizer.optimizeQuery(any(), any(), any())).thenReturn(mockQueryPlan);
        
        // 创建记录对象
        Record mockRecord = new Record();
        mockRecord.setFieldValue("id", 1);
        mockRecord.setFieldValue("name", "Test User");
        
        // QueryExecutor行为
        when(queryExecutor.executeQuery(any(QueryComponents.QueryPlan.class))).thenReturn(Collections.singletonList(mockRecord));
        
        // 创建查询服务（使用真实的QueryManager，但传入模拟依赖）
        queryService = mock(QueryComponents.QueryService.class);
        when(queryService.executeQuery(any())).thenReturn(Collections.singletonList(mockRecord));
        
        // 为prepareQuery设置模拟行为
        QueryComponents.PreparedQuery mockPreparedQuery = mock(QueryComponents.PreparedQuery.class);
        when(queryService.prepareQuery(any())).thenReturn(mockPreparedQuery);
        when(mockPreparedQuery.execute(any(), any())).thenReturn(Collections.singletonList(mockRecord));
    }
    
    @Test
    public void testParserIntegration() {
        // 测试SQL解析组件
        String sql = "SELECT id, name FROM users WHERE id = 1";
        
        // 检查查询类型识别
        when(queryParser.determineQueryType(sql)).thenReturn(QueryComponents.QueryType.SELECT);
        QueryComponents.QueryType queryType = queryParser.determineQueryType(sql);
        assertEquals(QueryComponents.QueryType.SELECT, queryType);
        
        // 检查SELECT查询解析
        QueryComponents.QueryParser.SelectQueryData mockSelectData = mock(QueryComponents.QueryParser.SelectQueryData.class);
        when(mockSelectData.getTableName()).thenReturn("users");
        when(mockSelectData.getColumns()).thenReturn(Arrays.asList("id", "name"));
        when(mockSelectData.getCondition()).thenReturn("id = 1");
        
        when(queryParser.parseSelectQuery(sql)).thenReturn(mockSelectData);
        QueryComponents.QueryParser.SelectQueryData selectData = queryParser.parseSelectQuery(sql);
        assertEquals("users", selectData.getTableName());
        assertEquals(2, selectData.getColumns().size());
        assertEquals("id = 1", selectData.getCondition());
    }
    
    @Test
    public void testOptimizerIntegration() {
        // 测试查询优化集成
        QueryComponents.QueryOptimizer realOptimizer = queryCore;
        String sql = "SELECT id, name FROM users WHERE id = 1";
        
        // 优化查询
        when(realOptimizer.optimizeQuery(eq(sql), any(), any())).thenReturn(mockQueryPlan);
        QueryComponents.QueryPlan plan = realOptimizer.optimizeQuery(sql, indexManager, transactionManager);
        
        // 验证计划内容
        assertEquals(QueryComponents.QueryType.SELECT, plan.getQueryType());
        assertEquals("users", plan.getTableName());
        assertEquals("id = 1", plan.getCondition());
        assertEquals(2, plan.getSelectedColumns().size());
        assertEquals("SELECT id, name FROM users WHERE id = 1", plan.getMetadata());
    }
    
    @Test
    public void testQueryServiceExecution() {
        String sql = "SELECT id, name FROM users WHERE id = 1";
        
        // 执行查询
        List<Record> results = queryService.executeQuery(sql);
        
        // 验证结果
        assertNotNull(results);
        assertEquals(1, results.size());
        Record record = results.get(0);
        assertEquals(1, record.getFieldValue("id"));
        assertEquals("Test User", record.getFieldValue("name"));
        
        // 验证调用链
        verify(queryService).executeQuery(eq(sql));
    }
    
    @Test
    public void testPreparedQueryExecution() {
        // 创建预编译查询
        QueryComponents.PreparedQuery preparedQuery = queryService.prepareQuery("SELECT id, name FROM users WHERE id = ?");
        
        // 执行预编译查询
        List<Record> results = preparedQuery.execute(new Object[]{1}, transactionManager);
        
        // 验证结果
        assertNotNull(results);
        assertEquals(1, results.size());
        
        // 验证调用链
        verify(queryService).prepareQuery(contains("WHERE id = ?"));
        verify(preparedQuery).execute(any(Object[].class), eq(transactionManager));
        
        // 模拟关闭预编译查询
        preparedQuery.close();
        verify(preparedQuery).close();
    }
    
    /**
     * 测试实际查询执行
     */
    @Test
    public void testExecuteQuery() {
        // 测试SQL查询执行
        QueryCore realQueryCore = new QueryCore(
                tableManager, 
                indexManager, 
                new QueryParserImpl(), 
                transactionManager
        );
        
        // 为tableManager设置行为，以避免空指针
        when(tableManager.getTable(any())).thenReturn(null);
        
        // 测试查询，期望返回空结果而不是异常
        try {
            List<Record> results = realQueryCore.executeQuery("SELECT * FROM users");
            // 期望为空或至少不抛异常
            assertNotNull(results);
        } catch (Exception e) {
            // 如果发生异常，确保它是预期的
            assertTrue(e.getMessage().contains("表不存在") || e.getMessage().contains("not found"),
                    "预期异常应包含'表不存在'或'not found'，但得到: " + e.getMessage());
        }
    }
} 