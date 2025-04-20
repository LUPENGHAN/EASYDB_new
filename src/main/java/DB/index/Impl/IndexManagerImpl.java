package DB.index.Impl;

import DB.index.interfaces.IndexManager;
import DB.page.models.Page;
import DB.record.models.Record;
import DB.table.interfaces.TableManager;
import DB.transaction.interfaces.TransactionManager;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.ArrayList;

/**
 * 索引管理器实现类
 */
@Slf4j
public class IndexManagerImpl implements IndexManager {
    // 底层索引实现
    private final BPlusTreeIndex bPlusTreeIndex;
    
    // 表与列索引的映射关系
    private final Map<String, Map<String, String>> tableColumnIndices;
    
    // 索引信息映射
    private final Map<String, IndexInfo> indexInfoMap;
    
    // 表管理器
    private final TableManager tableManager;
    
    /**
     * 索引信息类
     */
    private static class IndexInfo {
        String tableName;      // 表名
        String columnName;     // 列名
        String indexName;      // 索引名
        boolean isUnique;      // 是否唯一索引
        boolean isPrimary;     // 是否主键索引
        
        public IndexInfo(String tableName, String columnName, String indexName, boolean isUnique, boolean isPrimary) {
            this.tableName = tableName;
            this.columnName = columnName;
            this.indexName = indexName;
            this.isUnique = isUnique;
            this.isPrimary = isPrimary;
        }
    }
    
    /**
     * 构造函数
     * @param tableManager 表管理器
     */
    public IndexManagerImpl(TableManager tableManager) {
        this.bPlusTreeIndex = new BPlusTreeIndex(10); // 默认阶数为10
        this.tableColumnIndices = new ConcurrentHashMap<>();
        this.indexInfoMap = new ConcurrentHashMap<>();
        this.tableManager = tableManager;
    }
    
    /**
     * 构造函数
     * @param tableManager 表管理器
     * @param order B+树阶数
     */
    public IndexManagerImpl(TableManager tableManager, int order) {
        this.bPlusTreeIndex = new BPlusTreeIndex(order);
        this.tableColumnIndices = new ConcurrentHashMap<>();
        this.indexInfoMap = new ConcurrentHashMap<>();
        this.tableManager = tableManager;
    }

    @Override
    public void createIndex(String tableName, String columnName, String indexName, TransactionManager transactionManager) {
        // 1. 检查表是否存在
        if (tableManager.getTable(tableName) == null) {
            throw new IllegalArgumentException("表不存在: " + tableName);
        }
        
        // 2. 检查索引是否已存在
        if (indexInfoMap.containsKey(indexName)) {
            throw new IllegalArgumentException("索引已存在: " + indexName);
        }
        
        // 3. 创建B+树索引
        bPlusTreeIndex.createIndex(tableName, columnName, indexName, transactionManager);
        
        // 4. 更新索引映射
        tableColumnIndices.computeIfAbsent(tableName, k -> new ConcurrentHashMap<>())
                         .put(columnName, indexName);
        
        // 5. 存储索引信息
        boolean isUnique = false;
        boolean isPrimary = false;
        
        // 检查是否为唯一索引或主键索引
        for (DB.table.models.Column column : tableManager.getTable(tableName).getColumns()) {
            if (column.getName().equals(columnName)) {
                isUnique = column.isUnique();
                isPrimary = column.isPrimaryKey();
                break;
            }
        }
        
        indexInfoMap.put(indexName, new IndexInfo(tableName, columnName, indexName, isUnique, isPrimary));
        
        // 6. 建立索引数据
        // 获取表中所有记录并建立索引
        buildIndexData(tableName, columnName, indexName, transactionManager);
        
        log.info("创建索引: {} 表: {} 列: {}", indexName, tableName, columnName);
    }

    @Override
    public void dropIndex(String indexName, TransactionManager transactionManager) {
        // 1. 检查索引是否存在
        IndexInfo indexInfo = indexInfoMap.get(indexName);
        if (indexInfo == null) {
            throw new IllegalArgumentException("索引不存在: " + indexName);
        }
        
        // 2. 删除底层索引
        bPlusTreeIndex.dropIndex(indexName, transactionManager);
        
        // 3. 更新索引映射
        Map<String, String> columnIndices = tableColumnIndices.get(indexInfo.tableName);
        if (columnIndices != null) {
            columnIndices.remove(indexInfo.columnName);
            
            // 如果表没有任何索引了，移除表索引映射
            if (columnIndices.isEmpty()) {
                tableColumnIndices.remove(indexInfo.tableName);
            }
        }
        
        // 4. 移除索引信息
        indexInfoMap.remove(indexName);
        
        log.info("删除索引: {}", indexName);
    }

    @Override
    public void insertIndex(String indexName, Object key, Record record, TransactionManager transactionManager) {
        // 检查索引是否存在
        if (!indexInfoMap.containsKey(indexName)) {
            throw new IllegalArgumentException("索引不存在: " + indexName);
        }
        
        // 检查唯一性约束
        IndexInfo indexInfo = indexInfoMap.get(indexName);
        if (indexInfo.isUnique || indexInfo.isPrimary) {
            List<Record> existingRecords = exactQuery(indexName, key, transactionManager);
            if (!existingRecords.isEmpty()) {
                throw new IllegalStateException("违反唯一性约束: 索引 " + indexName + " 键值 " + key + " 已存在");
            }
        }
        
        // 插入索引
        bPlusTreeIndex.insertIndex(indexName, key, record, transactionManager);
    }

    @Override
    public void deleteIndex(String indexName, Object key, Record record, TransactionManager transactionManager) {
        // 检查索引是否存在
        if (!indexInfoMap.containsKey(indexName)) {
            throw new IllegalArgumentException("索引不存在: " + indexName);
        }
        
        // 删除索引
        bPlusTreeIndex.deleteIndex(indexName, key, record, transactionManager);
    }

    @Override
    public List<Record> rangeQuery(String indexName, Object minKey, Object maxKey, TransactionManager transactionManager) {
        // 检查索引是否存在
        if (!indexInfoMap.containsKey(indexName)) {
            throw new IllegalArgumentException("索引不存在: " + indexName);
        }
        
        // 范围查询
        return bPlusTreeIndex.rangeQuery(indexName, minKey, maxKey, transactionManager);
    }

    @Override
    public List<Record> exactQuery(String indexName, Object key, TransactionManager transactionManager) {
        // 检查索引是否存在
        if (!indexInfoMap.containsKey(indexName)) {
            throw new IllegalArgumentException("索引不存在: " + indexName);
            
        }
        
        // 精确查询
        return bPlusTreeIndex.exactQuery(indexName, key, transactionManager);
    }

    @Override
    public Page getIndexRoot(String indexName) {
        // 检查索引是否存在
        if (!indexInfoMap.containsKey(indexName)) {
            throw new IllegalArgumentException("索引不存在: " + indexName);
        }
        
        return bPlusTreeIndex.getIndexRoot(indexName);
    }

    @Override
    public void updateIndex(String indexName, Object oldKey, Object newKey, Record record, TransactionManager transactionManager) {
        // 检查索引是否存在
        if (!indexInfoMap.containsKey(indexName)) {
            throw new IllegalArgumentException("索引不存在: " + indexName);
        }
        
        // 检查唯一性约束
        IndexInfo indexInfo = indexInfoMap.get(indexName);
        if ((indexInfo.isUnique || indexInfo.isPrimary) && !oldKey.equals(newKey)) {
            List<Record> existingRecords = exactQuery(indexName, newKey, transactionManager);
            if (!existingRecords.isEmpty()) {
                throw new IllegalStateException("违反唯一性约束: 索引 " + indexName + " 键值 " + newKey + " 已存在");
            }
        }
        
        // 更新索引
        bPlusTreeIndex.updateIndex(indexName, oldKey, newKey, record, transactionManager);
    }
    
    /**
     * 为表的列创建主键索引
     * @param tableName 表名
     * @param columnName 列名
     * @param transactionManager 事务管理器
     */
    public void createPrimaryKeyIndex(String tableName, String columnName, TransactionManager transactionManager) {
        String indexName = tableName + "_" + columnName + "_pk";
        createIndex(tableName, columnName, indexName, transactionManager);
        
        // 更新索引信息为主键索引
        IndexInfo indexInfo = indexInfoMap.get(indexName);
        if (indexInfo != null) {
            indexInfo.isPrimary = true;
            indexInfo.isUnique = true; // 主键必须唯一
        }
    }
    
    /**
     * 为表的列创建唯一索引
     * @param tableName 表名
     * @param columnName 列名
     * @param transactionManager 事务管理器
     */
    public void createUniqueIndex(String tableName, String columnName, TransactionManager transactionManager) {
        String indexName = tableName + "_" + columnName + "_uk";
        createIndex(tableName, columnName, indexName, transactionManager);
        
        // 更新索引信息为唯一索引
        IndexInfo indexInfo = indexInfoMap.get(indexName);
        if (indexInfo != null) {
            indexInfo.isUnique = true;
        }
    }
    
    /**
     * 获取表的所有索引名称
     * @param tableName 表名
     * @return 索引名称列表
     */
    public List<String> getTableIndexes(String tableName) {
        Map<String, String> columnIndices = tableColumnIndices.get(tableName);
        if (columnIndices == null) {
            return new ArrayList<>();
        }
        
        return new ArrayList<>(columnIndices.values());
    }
    
    /**
     * 获取列的索引名称
     * @param tableName 表名
     * @param columnName 列名
     * @return 索引名称，如果不存在返回null
     */
    public String getColumnIndex(String tableName, String columnName) {
        Map<String, String> columnIndices = tableColumnIndices.get(tableName);
        if (columnIndices == null) {
            return null;
        }
        
        return columnIndices.get(columnName);
    }
    
    /**
     * 判断索引是否存在
     * @param indexName 索引名称
     * @return 是否存在
     */
    public boolean indexExists(String indexName) {
        return indexInfoMap.containsKey(indexName);
    }
    
    /**
     * 构建索引数据
     * @param tableName 表名
     * @param columnName 列名
     * @param indexName 索引名
     * @param transactionManager 事务管理器
     */
    private void buildIndexData(String tableName, String columnName, String indexName, TransactionManager transactionManager) {
        // 使用TableManager获取表的所有记录
        if (tableManager instanceof DB.table.impl.TableManagerImpl) {
            DB.table.impl.TableManagerImpl tableManagerImpl = (DB.table.impl.TableManagerImpl) tableManager;
            try {
                // 获取表的所有记录
                List<Record> records = tableManagerImpl.findRecords(
                    tableName, 
                    null, // 无条件，获取所有记录
                    null, // 这里需要适配实际的记录管理器
                    transactionManager
                );
                
                // 为每条记录建立索引
                for (Record record : records) {
                    Object key = record.getFieldValue(columnName);
                    if (key != null) {
                        bPlusTreeIndex.insertIndex(indexName, key, record, transactionManager);
                    }
                }
                
                log.info("索引 {} 数据构建完成，处理了 {} 条记录", indexName, records.size());
            } catch (Exception e) {
                log.error("构建索引数据失败: " + indexName, e);
                // 出错时删除索引
                bPlusTreeIndex.dropIndex(indexName, transactionManager);
                throw new RuntimeException("构建索引数据失败: " + e.getMessage(), e);
            }
        } else {
            log.warn("TableManager类型不支持，无法自动构建索引数据");
        }
    }
} 