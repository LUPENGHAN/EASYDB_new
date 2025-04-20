package DB.stats.interfaces;

import java.util.Map;
import java.util.List;

/**
 * 统计信息管理器接口
 */
public interface StatisticsManager {
    /**
     * 收集表统计信息
     * @param tableName 表名
     */
    void collectTableStats(String tableName);

    /**
     * 收集索引统计信息
     * @param indexName 索引名
     */
    void collectIndexStats(String indexName);

    /**
     * 获取表的行数估计
     * @param tableName 表名
     * @return 估计的行数
     */
    long getTableCardinality(String tableName);

    /**
     * 获取列的基数估计
     * @param tableName 表名
     * @param columnName 列名
     * @return 估计的基数
     */
    long getColumnCardinality(String tableName, String columnName);

    /**
     * 获取列的最小值
     * @param tableName 表名
     * @param columnName 列名
     * @return 最小值
     */
    Object getColumnMin(String tableName, String columnName);

    /**
     * 获取列的最大值
     * @param tableName 表名
     * @param columnName 列名
     * @return 最大值
     */
    Object getColumnMax(String tableName, String columnName);

    /**
     * 获取列的直方图
     * @param tableName 表名
     * @param columnName 列名
     * @return 直方图数据
     */
    Histogram getColumnHistogram(String tableName, String columnName);

    /**
     * 更新统计信息
     * @param tableName 表名
     */
    void updateStats(String tableName);

    /**
     * 获取表的统计信息
     * @param tableName 表名
     * @return 统计信息
     */
    TableStats getTableStats(String tableName);

    /**
     * 表统计信息类
     */
    class TableStats {
        private String tableName;
        private long rowCount;
        private long pageCount;
        private Map<String, ColumnStats> columnStats;
        private Map<String, IndexStats> indexStats;

        // Getters and setters
    }

    /**
     * 列统计信息类
     */
    class ColumnStats {
        private String columnName;
        private long cardinality;
        private Object minValue;
        private Object maxValue;
        private Histogram histogram;

        // Getters and setters
    }

    /**
     * 直方图类
     */
    class Histogram {
        private List<Bucket> buckets;
        private int bucketCount;

        // Getters and setters
    }

    /**
     * 直方图桶类
     */
    class Bucket {
        private Object lowerBound;
        private Object upperBound;
        private long count;
        private long distinctCount;

        // Getters and setters
    }
} 