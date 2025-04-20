package DB.table.impl;

import DB.page.interfaces.PageManager;
import DB.table.interfaces.TableManager;
import DB.table.models.Table;
import DB.table.models.Column;
import DB.transaction.interfaces.TransactionManager;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 表管理器实现类
 */
@Slf4j
@RequiredArgsConstructor
public class TableManagerImpl implements TableManager {
    @Getter
    private final PageManager pageManager;
    private final Map<String, Table> tables = new ConcurrentHashMap<>();
    private final Map<String, Integer> tablePageCounts = new ConcurrentHashMap<>();

    @Override
    public void createTable(Table table, TransactionManager transactionManager) {
        long xid = transactionManager.beginTransaction();
        try {
            // 检查表是否已存在
            if (tables.containsKey(table.getName())) {
                throw new IllegalArgumentException("Table " + table.getName() + " already exists");
            }

            // 验证表定义
            validateTableDefinition(table);

            // 创建初始页面
            int initialPageId = pageManager.createPage().getHeader().getPageId();
            tablePageCounts.put(table.getName(), 1);

            // 设置表的创建时间
            table.setCreateTime(System.currentTimeMillis());
            table.setLastModifiedTime(System.currentTimeMillis());
            table.setPageCount(1);

            // 保存表定义
            tables.put(table.getName(), table);

            transactionManager.commitTransaction(xid);
            log.info("Created table: {}", table.getName());
        } catch (Exception e) {
            transactionManager.rollbackTransaction(xid);
            throw new RuntimeException("Failed to create table: " + table.getName(), e);
        }
    }

    @Override
    public void dropTable(String tableName, TransactionManager transactionManager) {
        long xid = transactionManager.beginTransaction();
        try {
            // 检查表是否存在
            if (!tables.containsKey(tableName)) {
                throw new IllegalArgumentException("Table " + tableName + " does not exist");
            }

            // 删除表定义
            tables.remove(tableName);
            tablePageCounts.remove(tableName);

            // TODO: 删除表的所有页面
            // TODO: 删除相关的索引

            transactionManager.commitTransaction(xid);
            log.info("Dropped table: {}", tableName);
        } catch (Exception e) {
            transactionManager.rollbackTransaction(xid);
            throw new RuntimeException("Failed to drop table: " + tableName, e);
        }
    }

    @Override
    public void alterTable(String tableName, String alterType, String alterDefinition, TransactionManager transactionManager) {
        long xid = transactionManager.beginTransaction();
        try {
            // 检查表是否存在
            Table table = tables.get(tableName);
            if (table == null) {
                throw new IllegalArgumentException("Table " + tableName + " does not exist");
            }

            // 根据修改类型执行不同的操作
            switch (alterType.toUpperCase()) {
                case "ADD COLUMN":
                    addColumn(table, alterDefinition);
                    break;
                case "DROP COLUMN":
                    dropColumn(table, alterDefinition);
                    break;
                case "MODIFY COLUMN":
                    modifyColumn(table, alterDefinition);
                    break;
                case "ADD CONSTRAINT":
                    addConstraint(table, alterDefinition);
                    break;
                case "DROP CONSTRAINT":
                    dropConstraint(table, alterDefinition);
                    break;
                default:
                    throw new IllegalArgumentException("Unsupported alter type: " + alterType);
            }

            // 更新表的最后修改时间
            table.setLastModifiedTime(System.currentTimeMillis());

            transactionManager.commitTransaction(xid);
            log.info("Altered table: {}", tableName);
        } catch (Exception e) {
            transactionManager.rollbackTransaction(xid);
            throw new RuntimeException("Failed to alter table: " + tableName, e);
        }
    }

    @Override
    public Table getTable(String tableName) {
        return tables.get(tableName);
    }

    @Override
    public List<String> getAllTables() {
        return new ArrayList<>(tables.keySet());
    }

    /**
     * 添加列
     */
    private void addColumn(Table table, String columnDefinition) {
        // 解析列定义
        Column column = parseColumnDefinition(columnDefinition);
        
        // 检查列名是否已存在
        for (Column existingColumn : table.getColumns()) {
            if (existingColumn.getName().equals(column.getName())) {
                throw new IllegalArgumentException("Column " + column.getName() + " already exists");
            }
        }

        // 添加列
        table.getColumns().add(column);
        
        // 如果是主键，添加到主键列表
        if (column.isPrimaryKey()) {
            table.getPrimaryKeys().add(column.getName());
        }
        
        // 如果是唯一键，添加到唯一键列表
        if (column.isUnique()) {
            table.getUniqueKeys().add(column.getName());
        }
    }

    /**
     * 删除列
     */
    private void dropColumn(Table table, String columnName) {
        // 检查列是否存在
        Column columnToRemove = null;
        for (Column column : table.getColumns()) {
            if (column.getName().equals(columnName)) {
                columnToRemove = column;
                break;
            }
        }
        
        if (columnToRemove == null) {
            throw new IllegalArgumentException("Column " + columnName + " does not exist");
        }

        // 检查是否是主键或外键
        if (table.getPrimaryKeys().contains(columnName)) {
            throw new IllegalArgumentException("Cannot drop primary key column: " + columnName);
        }
        
        for (Table.ForeignKey fk : table.getForeignKeys()) {
            if (fk.getColumns().contains(columnName)) {
                throw new IllegalArgumentException("Cannot drop foreign key column: " + columnName);
            }
        }

        // 删除列
        table.getColumns().remove(columnToRemove);
        table.getPrimaryKeys().remove(columnName);
        table.getUniqueKeys().remove(columnName);
    }

    /**
     * 修改列
     */
    private void modifyColumn(Table table, String columnDefinition) {
        // 解析列定义
        Column newColumn = parseColumnDefinition(columnDefinition);
        
        // 查找要修改的列
        int index = -1;
        for (int i = 0; i < table.getColumns().size(); i++) {
            if (table.getColumns().get(i).getName().equals(newColumn.getName())) {
                index = i;
                break;
            }
        }
        
        if (index == -1) {
            throw new IllegalArgumentException("Column " + newColumn.getName() + " does not exist");
        }

        // 检查修改是否合法
        Column oldColumn = table.getColumns().get(index);
        validateColumnModification(oldColumn, newColumn);

        // 更新列定义
        table.getColumns().set(index, newColumn);
        
        // 更新主键和唯一键
        if (newColumn.isPrimaryKey() && !table.getPrimaryKeys().contains(newColumn.getName())) {
            table.getPrimaryKeys().add(newColumn.getName());
        } else if (!newColumn.isPrimaryKey()) {
            table.getPrimaryKeys().remove(newColumn.getName());
        }
        
        if (newColumn.isUnique() && !table.getUniqueKeys().contains(newColumn.getName())) {
            table.getUniqueKeys().add(newColumn.getName());
        } else if (!newColumn.isUnique()) {
            table.getUniqueKeys().remove(newColumn.getName());
        }
    }

    /**
     * 添加约束
     */
    private void addConstraint(Table table, String constraintDefinition) {
        // 解析约束定义
        String[] parts = constraintDefinition.split("\\s+");
        String constraintType = parts[0].toUpperCase();
        
        switch (constraintType) {
            case "PRIMARY":
                addPrimaryKey(table, parts);
                break;
            case "FOREIGN":
                addForeignKey(table, parts);
                break;
            case "UNIQUE":
                addUniqueKey(table, parts);
                break;
            case "CHECK":
                addCheckConstraint(table, constraintDefinition);
                break;
            default:
                throw new IllegalArgumentException("Unsupported constraint type: " + constraintType);
        }
    }

    /**
     * 删除约束
     */
    private void dropConstraint(Table table, String constraintName) {
        // 检查约束是否存在
        boolean found = false;
        
        // 检查主键
        if (table.getPrimaryKeys().contains(constraintName)) {
            table.getPrimaryKeys().remove(constraintName);
            found = true;
        }
        
        // 检查外键
        for (Table.ForeignKey fk : table.getForeignKeys()) {
            if (fk.getName().equals(constraintName)) {
                table.getForeignKeys().remove(fk);
                found = true;
                break;
            }
        }
        
        // 检查唯一键
        if (table.getUniqueKeys().contains(constraintName)) {
            table.getUniqueKeys().remove(constraintName);
            found = true;
        }
        
        // 检查检查约束
        if (table.getCheckConstraints().contains(constraintName)) {
            table.getCheckConstraints().remove(constraintName);
            found = true;
        }
        
        if (!found) {
            throw new IllegalArgumentException("Constraint " + constraintName + " does not exist");
        }
    }

    /**
     * 解析列定义
     */
    private Column parseColumnDefinition(String definition) {
        // 解析列定义，格式：column_name data_type [length] [NOT NULL] [PRIMARY KEY] [UNIQUE] [DEFAULT value] [CHECK (condition)]
        String[] parts = definition.trim().split("\\s+");
        if (parts.length < 2) {
            throw new IllegalArgumentException("Invalid column definition: " + definition);
        }

        Column.ColumnBuilder builder = Column.builder();
        int index = 0;

        // 解析列名
        builder.name(parts[index++]);

        // 解析数据类型
        String typeStr = parts[index++].toUpperCase();
        Column.DataType dataType;
        int length = 0;
        try {
            dataType = Column.DataType.valueOf(typeStr);
            length = dataType.getDefaultLength();
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Invalid data type: " + typeStr);
        }

        // 解析长度（如果有）
        if (index < parts.length && parts[index].startsWith("(")) {
            String lengthStr = parts[index].substring(1, parts[index].length() - 1);
            try {
                length = Integer.parseInt(lengthStr);
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("Invalid length: " + lengthStr);
            }
            index++;
        }

        builder.dataType(dataType);
        builder.length(length);

        // 解析其他属性
        while (index < parts.length) {
            String token = parts[index++].toUpperCase();
            switch (token) {
                case "NOT":
                    if (index < parts.length && parts[index].equalsIgnoreCase("NULL")) {
                        builder.nullable(false);
                        index++;
                    }
                    break;
                case "PRIMARY":
                    if (index < parts.length && parts[index].equalsIgnoreCase("KEY")) {
                        builder.primaryKey(true);
                        index++;
                    }
                    break;
                case "UNIQUE":
                    builder.unique(true);
                    break;
                case "DEFAULT":
                    if (index < parts.length) {
                        String defaultValue = parts[index++];
                        // 处理带引号的字符串
                        if (defaultValue.startsWith("'") && defaultValue.endsWith("'")) {
                            defaultValue = defaultValue.substring(1, defaultValue.length() - 1);
                        }
                        builder.defaultValue(defaultValue);
                    }
                    break;
                case "CHECK":
                    if (index < parts.length && parts[index].equals("(")) {
                        StringBuilder checkCondition = new StringBuilder();
                        int parenCount = 1;
                        index++;
                        while (index < parts.length && parenCount > 0) {
                            if (parts[index].equals("(")) parenCount++;
                            if (parts[index].equals(")")) parenCount--;
                            if (parenCount > 0) {
                                checkCondition.append(parts[index]).append(" ");
                            }
                            index++;
                        }
                        builder.checkConstraint(checkCondition.toString().trim());
                    }
                    break;
            }
        }

        return builder.build();
    }

    /**
     * 验证列修改
     */
    private void validateColumnModification(Column oldColumn, Column newColumn) {
        // 检查数据类型修改
        if (oldColumn.getDataType() != newColumn.getDataType()) {
            throw new IllegalArgumentException("Cannot change column data type");
        }
        
        // 检查长度修改
        if (oldColumn.getLength() != newColumn.getLength()) {
            throw new IllegalArgumentException("Cannot change column length");
        }
        
        // 检查主键修改
        if (oldColumn.isPrimaryKey() != newColumn.isPrimaryKey()) {
            throw new IllegalArgumentException("Cannot change primary key status");
        }
    }

    /**
     * 添加主键
     */
    private void addPrimaryKey(Table table, String[] parts) {
        if (parts.length < 4 || !parts[1].equalsIgnoreCase("KEY") || !parts[2].equals("(")) {
            throw new IllegalArgumentException("Invalid primary key definition");
        }

        List<String> pkColumns = new ArrayList<>();
        int i = 3;
        while (i < parts.length && !parts[i].equals(")")) {
            String columnName = parts[i].replace(",", "").trim();
            // 验证列是否存在
            boolean found = false;
            for (Column column : table.getColumns()) {
                if (column.getName().equals(columnName)) {
                    found = true;
                    break;
                }
            }
            if (!found) {
                throw new IllegalArgumentException("Column " + columnName + " does not exist");
            }
            pkColumns.add(columnName);
            i++;
        }

        // 更新主键
        table.getPrimaryKeys().clear();
        table.getPrimaryKeys().addAll(pkColumns);

        // 更新列的primaryKey属性
        for (Column column : table.getColumns()) {
            column.setPrimaryKey(pkColumns.contains(column.getName()));
        }
    }

    /**
     * 添加外键
     */
    private void addForeignKey(Table table, String[] parts) {
        if (parts.length < 8 || !parts[1].equalsIgnoreCase("KEY") || !parts[2].equals("(")) {
            throw new IllegalArgumentException("Invalid foreign key definition");
        }

        Table.ForeignKey fk = new Table.ForeignKey();
        List<String> columns = new ArrayList<>();
        List<String> refColumns = new ArrayList<>();

        // 解析外键列
        int i = 3;
        while (i < parts.length && !parts[i].equals(")")) {
            String columnName = parts[i].replace(",", "").trim();
            // 验证列是否存在
            boolean found = false;
            for (Column column : table.getColumns()) {
                if (column.getName().equals(columnName)) {
                    found = true;
                    break;
                }
            }
            if (!found) {
                throw new IllegalArgumentException("Column " + columnName + " does not exist");
            }
            columns.add(columnName);
            i++;
        }

        // 解析引用表
        i += 2; // 跳过 ) REFERENCES
        String refTable = parts[i++];

        // 解析引用列
        i += 1; // 跳过 (
        while (i < parts.length && !parts[i].equals(")")) {
            String columnName = parts[i].replace(",", "").trim();
            refColumns.add(columnName);
            i++;
        }

        // 解析ON DELETE和ON UPDATE
        String onDelete = "NO ACTION";
        String onUpdate = "NO ACTION";
        while (i < parts.length) {
            if (parts[i].equalsIgnoreCase("ON")) {
                i++;
                if (parts[i].equalsIgnoreCase("DELETE")) {
                    i++;
                    onDelete = parts[i];
                } else if (parts[i].equalsIgnoreCase("UPDATE")) {
                    i++;
                    onUpdate = parts[i];
                }
            }
            i++;
        }

        // 设置外键属性
        fk.setName(table.getName() + "_" + String.join("_", columns) + "_fk");
        fk.setColumns(columns);
        fk.setRefTable(refTable);
        fk.setRefColumns(refColumns);
        fk.setOnDelete(onDelete);
        fk.setOnUpdate(onUpdate);

        // 添加外键
        table.getForeignKeys().add(fk);
    }

    /**
     * 添加唯一键
     */
    private void addUniqueKey(Table table, String[] parts) {
        if (parts.length < 3 || !parts[1].equals("(")) {
            throw new IllegalArgumentException("Invalid unique key definition");
        }

        List<String> uniqueColumns = new ArrayList<>();
        int i = 2;
        while (i < parts.length && !parts[i].equals(")")) {
            String columnName = parts[i].replace(",", "").trim();
            // 验证列是否存在
            boolean found = false;
            for (Column column : table.getColumns()) {
                if (column.getName().equals(columnName)) {
                    found = true;
                    break;
                }
            }
            if (!found) {
                throw new IllegalArgumentException("Column " + columnName + " does not exist");
            }
            uniqueColumns.add(columnName);
            i++;
        }

        // 更新唯一键
        table.getUniqueKeys().addAll(uniqueColumns);

        // 更新列的unique属性
        for (Column column : table.getColumns()) {
            column.setUnique(uniqueColumns.contains(column.getName()));
        }
    }

    /**
     * 添加检查约束
     */
    private void addCheckConstraint(Table table, String definition) {
        // 解析检查约束名称和条件
        String[] parts = definition.split("\\s+", 3);
        if (parts.length < 3 || !parts[1].equals("(")) {
            throw new IllegalArgumentException("Invalid check constraint definition");
        }

        String constraintName = table.getName() + "_check_" + System.currentTimeMillis();
        String condition = parts[2].substring(0, parts[2].length() - 1); // 去掉最后的 )

        // 验证条件中引用的列是否存在
        String[] tokens = condition.split("\\s+");
        for (String token : tokens) {
            if (token.matches("[a-zA-Z_][a-zA-Z0-9_]*")) { // 可能是列名
                boolean found = false;
                for (Column column : table.getColumns()) {
                    if (column.getName().equals(token)) {
                        found = true;
                        break;
                    }
                }
                if (!found) {
                    throw new IllegalArgumentException("Column " + token + " does not exist in check constraint");
                }
            }
        }

        // 添加检查约束
        table.getCheckConstraints().add(constraintName + ":" + condition);
    }

    /**
     * 验证表定义
     */
    private void validateTableDefinition(Table table) {
        // 检查表名
        if (table.getName() == null || table.getName().isEmpty()) {
            throw new IllegalArgumentException("Table name cannot be empty");
        }

        // 检查列定义
        if (table.getColumns() == null || table.getColumns().isEmpty()) {
            throw new IllegalArgumentException("Table must have at least one column");
        }

        // 检查主键
        if (table.getPrimaryKeys() != null && !table.getPrimaryKeys().isEmpty()) {
            for (String pk : table.getPrimaryKeys()) {
                boolean found = false;
                for (Column column : table.getColumns()) {
                    if (column.getName().equals(pk)) {
                        found = true;
                        break;
                    }
                }
                if (!found) {
                    throw new IllegalArgumentException("Primary key column " + pk + " does not exist");
                }
            }
        }

        // 检查外键
        if (table.getForeignKeys() != null) {
            for (Table.ForeignKey fk : table.getForeignKeys()) {
                // 检查外键列是否存在
                for (String column : fk.getColumns()) {
                    boolean found = false;
                    for (Column tableColumn : table.getColumns()) {
                        if (tableColumn.getName().equals(column)) {
                            found = true;
                            break;
                        }
                    }
                    if (!found) {
                        throw new IllegalArgumentException("Foreign key column " + column + " does not exist");
                    }
                }

                // 检查引用表是否存在
                if (!tables.containsKey(fk.getRefTable())) {
                    throw new IllegalArgumentException("Referenced table " + fk.getRefTable() + " does not exist");
                }

                // 检查引用列是否存在
                Table refTable = tables.get(fk.getRefTable());
                for (String refColumn : fk.getRefColumns()) {
                    boolean found = false;
                    for (Column column : refTable.getColumns()) {
                        if (column.getName().equals(refColumn)) {
                            found = true;
                            break;
                        }
                    }
                    if (!found) {
                        throw new IllegalArgumentException("Referenced column " + refColumn + " does not exist");
                    }
                }
            }
        }
    }
} 