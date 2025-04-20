package DB.query.interfaces;

import DB.record.models.Record;
import java.util.List;
import java.util.Map;

/**
 * 查询执行器接口
 */
public interface QueryExecutor {
    /**
     * 执行SQL查询
     * @param sql SQL语句
     * @return 结果记录列表
     */
    List<Record> executeQuery(String sql);

    /**
     * 执行SELECT查询
     * @param tableName 表名
     * @param condition 查询条件
     * @param selectedColumns 选择的列
     * @return 结果记录列表
     */
    List<Record> executeSelect(String tableName, String condition, List<String> selectedColumns);

    /**
     * 执行INSERT语句
     * @param tableName 表名
     * @param columns 列名
     * @param values 值
     * @return 影响的行数
     */
    int executeInsert(String tableName, List<String> columns, List<Object> values);

    /**
     * 执行UPDATE语句
     * @param tableName 表名
     * @param setValues 设置的值
     * @param condition 条件
     * @return 影响的行数
     */
    int executeUpdate(String tableName, Map<String, Object> setValues, String condition);

    /**
     * 执行DELETE语句
     * @param tableName 表名
     * @param condition 条件
     * @return 影响的行数
     */
    int executeDelete(String tableName, String condition);

    /**
     * 预编译SQL语句
     * @param sql SQL语句
     * @return 预编译查询对象
     */
    PreparedQuery prepare(String sql);
} 