package DB;

import DB.query.impl.QueryParserImpl;
import DB.query.interfaces.QueryComponents;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 查询解析器测试类
 */
public class QueryParserTest {
    private QueryComponents.QueryParser queryParser;

    @BeforeEach
    public void setUp() {
        queryParser = new QueryParserImpl();
    }

    @Test
    public void testDetermineQueryType() {
        assertEquals(QueryComponents.QueryType.SELECT, queryParser.determineQueryType("SELECT * FROM users"));
        assertEquals(QueryComponents.QueryType.INSERT, queryParser.determineQueryType("INSERT INTO users (id, name) VALUES (1, 'Tom')"));
        assertEquals(QueryComponents.QueryType.UPDATE, queryParser.determineQueryType("UPDATE users SET name = 'Jerry' WHERE id = 1"));
        assertEquals(QueryComponents.QueryType.DELETE, queryParser.determineQueryType("DELETE FROM users WHERE id = 1"));
        
        // 测试大小写不敏感
        assertEquals(QueryComponents.QueryType.SELECT, queryParser.determineQueryType("select * from users"));
        
        // 测试异常情况
        Exception exception = assertThrows(IllegalArgumentException.class, () -> {
            queryParser.determineQueryType("INVALID QUERY");
        });
        assertTrue(exception.getMessage().contains("不支持的查询类型"));
    }

    @Test
    public void testParseSelectQuery() {
        String query = "SELECT id, name, age FROM users WHERE age > 18";
        QueryComponents.QueryParser.SelectQueryData data = queryParser.parseSelectQuery(query);
        
        assertEquals("users", data.getTableName());
        assertEquals(3, data.getColumns().size());
        assertTrue(data.getColumns().contains("id"));
        assertTrue(data.getColumns().contains("name"));
        assertTrue(data.getColumns().contains("age"));
        assertEquals("age > 18", data.getCondition());
        
        // 测试没有条件的查询
        query = "SELECT * FROM users";
        data = queryParser.parseSelectQuery(query);
        
        assertEquals("users", data.getTableName());
        assertEquals(1, data.getColumns().size());
        assertEquals("*", data.getColumns().get(0));
        assertNull(data.getCondition());
    }

    @Test
    public void testParseInsertQuery() {
        String query = "INSERT INTO users (id, name, age) VALUES (1, 'Tom', 25)";
        QueryComponents.QueryParser.InsertQueryData data = queryParser.parseInsertQuery(query);
        
        assertEquals("users", data.getTableName());
        assertEquals(3, data.getColumns().size());
        assertEquals("id", data.getColumns().get(0));
        assertEquals("name", data.getColumns().get(1));
        assertEquals("age", data.getColumns().get(2));
        
        assertEquals(3, data.getValues().size());
        assertEquals("1", data.getValues().get(0));
        assertEquals("'Tom'", data.getValues().get(1));
        assertEquals("25", data.getValues().get(2));
    }

    @Test
    public void testParseUpdateQuery() {
        String query = "UPDATE users SET name = 'Jerry', age = 30 WHERE id = 1";
        QueryComponents.QueryParser.UpdateQueryData data = queryParser.parseUpdateQuery(query);
        
        assertEquals("users", data.getTableName());
        Map<String, String> setValues = data.getSetValues();
        assertEquals(2, setValues.size());
        assertEquals("'Jerry'", setValues.get("name"));
        assertEquals("30", setValues.get("age"));
        assertEquals("id = 1", data.getCondition());
        
        // 测试没有条件的更新
        query = "UPDATE users SET status = 'inactive'";
        data = queryParser.parseUpdateQuery(query);
        
        assertEquals("users", data.getTableName());
        setValues = data.getSetValues();
        assertEquals(1, setValues.size());
        assertEquals("'inactive'", setValues.get("status"));
        assertNull(data.getCondition());
    }

    @Test
    public void testParseDeleteQuery() {
        String query = "DELETE FROM users WHERE id = 1";
        QueryComponents.QueryParser.DeleteQueryData data = queryParser.parseDeleteQuery(query);
        
        assertEquals("users", data.getTableName());
        assertEquals("id = 1", data.getCondition());
        
        // 测试没有条件的删除
        query = "DELETE FROM users";
        data = queryParser.parseDeleteQuery(query);
        
        assertEquals("users", data.getTableName());
        assertNull(data.getCondition());
    }

    @Test
    public void testParseCondition() {
        // 测试等值条件
        QueryComponents.QueryParser.ConditionData condition = queryParser.parseCondition("id = 1");
        assertEquals("id", condition.getColumn());
        assertEquals("=", condition.getOperator());
        assertEquals("1", condition.getValue());
        
        // 测试字符串条件
        condition = queryParser.parseCondition("name = 'Tom'");
        assertEquals("name", condition.getColumn());
        assertEquals("=", condition.getOperator());
        assertEquals("Tom", condition.getValue()); // 注意：值已去除引号
        
        // 测试比较运算符
        condition = queryParser.parseCondition("age > 18");
        assertEquals("age", condition.getColumn());
        assertEquals(">", condition.getOperator());
        assertEquals("18", condition.getValue());
        
        // 测试空条件
        assertNull(queryParser.parseCondition(null));
        assertNull(queryParser.parseCondition(""));
        
        // 测试无效条件
        Exception exception = assertThrows(IllegalArgumentException.class, () -> {
            queryParser.parseCondition("invalid condition");
        });
        assertTrue(exception.getMessage().contains("无效的条件表达式"));
    }
} 