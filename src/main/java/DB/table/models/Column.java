package DB.table.models;

import lombok.Data;
import lombok.Builder;

/**
 * 列定义类
 */
@Data
@Builder
public class Column {
    private String name;                // 列名
    private DataType dataType;          // 数据类型
    private int length;                 // 长度（对于变长类型）
    private boolean nullable;           // 是否允许为空
    private boolean primaryKey;         // 是否为主键
    private boolean unique;             // 是否唯一
    private Object defaultValue;        // 默认值
    private String checkConstraint;     // 检查约束

    /**
     * 数据类型枚举
     */
    public enum DataType {
        INT(4),         // 整数
        BIGINT(8),      // 长整数
        FLOAT(4),       // 单精度浮点数
        DOUBLE(8),      // 双精度浮点数
        CHAR(1),        // 定长字符串
        VARCHAR(0),     // 变长字符串
        DATE(8),        // 日期
        TIME(8),        // 时间
        TIMESTAMP(8),   // 时间戳
        BOOLEAN(1);     // 布尔值

        private final int defaultLength;

        DataType(int defaultLength) {
            this.defaultLength = defaultLength;
        }

        public int getDefaultLength() {
            return defaultLength;
        }
    }
} 