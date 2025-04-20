package DB.table.models;

import lombok.Data;
import lombok.Builder;
import lombok.NoArgsConstructor;
import lombok.AllArgsConstructor;

import java.util.ArrayList;
import java.util.List;

/**
 * 表定义类
 */
@Data
@Builder
public class Table {
    private String name;                    // 表名
    private List<Column> columns;           // 列定义
    private List<String> primaryKeys;       // 主键列名列表
    private List<ForeignKey> foreignKeys;   // 外键定义
    private List<String> uniqueKeys;        // 唯一键列名列表
    private List<String> checkConstraints;  // 检查约束
    private int pageCount;                  // 页面数量
    private long rowCount;                  // 行数
    private long createTime;                // 创建时间
    private long lastModifiedTime;          // 最后修改时间

    /**
     * 外键定义类
     */
    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class ForeignKey {
        private String name;                // 外键名
        private List<String> columns;       // 外键列名列表
        private String refTable;            // 引用表名
        private List<String> refColumns;    // 引用列名列表
        private String onDelete;            // 删除时的动作
        private String onUpdate;            // 更新时的动作
    }

    /**
     * 创建表构建器
     */
    public static class TableBuilder {
        private List<Column> columns = new ArrayList<>();
        private List<String> primaryKeys = new ArrayList<>();
        private List<ForeignKey> foreignKeys = new ArrayList<>();
        private List<String> uniqueKeys = new ArrayList<>();
        private List<String> checkConstraints = new ArrayList<>();

        public TableBuilder column(Column column) {
            this.columns.add(column);
            if (column.isPrimaryKey()) {
                this.primaryKeys.add(column.getName());
            }
            if (column.isUnique()) {
                this.uniqueKeys.add(column.getName());
            }
            return this;
        }

        public TableBuilder foreignKey(ForeignKey foreignKey) {
            this.foreignKeys.add(foreignKey);
            return this;
        }

        public TableBuilder checkConstraint(String constraint) {
            this.checkConstraints.add(constraint);
            return this;
        }
    }
} 