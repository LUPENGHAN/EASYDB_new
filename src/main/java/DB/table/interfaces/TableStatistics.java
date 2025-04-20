package DB.table.interfaces;

/**
 * 表统计信息接口
 * 提供表的各种统计数据，用于查询优化
 */
public interface TableStatistics {
    
    /**
     * 获取表的行数
     * @return 行数
     */
    long getRowCount();
    
    /**
     * 获取表占用的页数
     * @return 页数
     */
    int getPageCount();
    
    /**
     * 获取表大小（字节）
     * @return 表大小
     */
    long getSize();
    
    /**
     * 获取列的基数（不同值的数量）
     * @param columnName 列名
     * @return 列基数
     */
    long getColumnCardinality(String columnName);
    
    /**
     * 获取列的最小值
     * @param columnName 列名
     * @return 最小值
     */
    Object getColumnMinValue(String columnName);
    
    /**
     * 获取列的最大值
     * @param columnName 列名
     * @return 最大值
     */
    Object getColumnMaxValue(String columnName);
} 