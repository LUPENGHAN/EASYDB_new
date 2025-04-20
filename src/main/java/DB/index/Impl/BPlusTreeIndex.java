package DB.index.Impl;

import DB.concurrency.models.LockType;
import DB.index.interfaces.IndexManager;
import DB.page.models.Page;
import DB.record.models.Record;
import DB.transaction.interfaces.TransactionManager;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * B+树索引实现
 */
@Slf4j
public class BPlusTreeIndex implements IndexManager {
    private final Map<String, Page> indexRoots;
    private final int order;

    public BPlusTreeIndex(int order) {
        this.order = order;
        this.indexRoots = new ConcurrentHashMap<>();
    }

    @Override
    public void createIndex(String tableName, String columnName, String indexName, TransactionManager transactionManager) {
        Page rootPage = new Page(0);
        // 初始化根页面
        rootPage.getHeader().setPageType(Page.PageType.INDEX.getValue());
        rootPage.getHeader().setIsLeaf(true);
        rootPage.getHeader().setKeyCount(0);
        indexRoots.put(indexName, rootPage);
        log.info("Created B+ tree index {} on table {} column {}", indexName, tableName, columnName);
    }

    @Override
    public void dropIndex(String indexName, TransactionManager transactionManager) {
        indexRoots.remove(indexName);
        log.info("Dropped B+ tree index {}", indexName);
    }

    @Override
    public void insertIndex(String indexName, Object key, Record record, TransactionManager transactionManager) {
        Page root = indexRoots.get(indexName);
        if (root == null) {
            throw new IllegalArgumentException("Index " + indexName + " does not exist");
        }

        long xid = transactionManager.beginTransaction();
        transactionManager.acquireLock(xid, root, LockType.EXCLUSIVE);

        try {
            insert(root, key, record);
        } finally {
            transactionManager.releaseLock(xid, root);
        }
    }

    @Override
    public void deleteIndex(String indexName, Object key, Record record, TransactionManager transactionManager) {
        Page root = indexRoots.get(indexName);
        if (root == null) {
            throw new IllegalArgumentException("Index " + indexName + " does not exist");
        }

        long xid = transactionManager.beginTransaction();
        transactionManager.acquireLock(xid, root, LockType.EXCLUSIVE);

        try {
            delete(root, key, record);
        } finally {
            transactionManager.releaseLock(xid, root);
        }
    }

    @Override
    public List<Record> rangeQuery(String indexName, Object minKey, Object maxKey, TransactionManager transactionManager) {
        Page root = indexRoots.get(indexName);
        if (root == null) {
            throw new IllegalArgumentException("Index " + indexName + " does not exist");
        }

        long xid = transactionManager.beginTransaction();
        transactionManager.acquireLock(xid, root, LockType.SHARED);

        try {
            return rangeSearch(root, minKey, maxKey);
        } finally {
            transactionManager.releaseLock(xid, root);
        }
    }

    @Override
    public List<Record> exactQuery(String indexName, Object key, TransactionManager transactionManager) {
        Page root = indexRoots.get(indexName);
        if (root == null) {
            throw new IllegalArgumentException("Index " + indexName + " does not exist");
        }

        long xid = transactionManager.beginTransaction();
        transactionManager.acquireLock(xid, root, LockType.SHARED);

        try {
            return search(root, key);
        } finally {
            transactionManager.releaseLock(xid, root);
        }
    }

    @Override
    public Page getIndexRoot(String indexName) {
        return indexRoots.get(indexName);
    }

    @Override
    public void updateIndex(String indexName, Object oldKey, Object newKey, Record record, TransactionManager transactionManager) {
        deleteIndex(indexName, oldKey, record, transactionManager);
        insertIndex(indexName, newKey, record, transactionManager);
    }

    private void insert(Page page, Object key, Record record) {
        if (page.getHeader().isLeaf()) {
            // 叶子节点插入
            int keyCount = page.getHeader().getKeyCount();
            if (keyCount < order - 1) {
                // 直接插入
                insertIntoLeaf(page, key, record);
            } else {
                // 分裂
                splitLeaf(page, key, record);
            }
        } else {
            // 内部节点插入
            Page child = findChild(page, key);
            insert(child, key, record);
            
            // 检查是否需要分裂
            if (child.getHeader().getKeyCount() >= order) {
                splitInternal(page, child);
            }
        }
    }

    private void delete(Page page, Object key, Record record) {
        if (page.getHeader().isLeaf()) {
            // 叶子节点删除
            deleteFromLeaf(page, key, record);
            
            // 检查是否需要合并
            if (page.getHeader().getKeyCount() < (order - 1) / 2) {
                mergeLeaf(page);
            }
        } else {
            // 内部节点删除
            Page child = findChild(page, key);
            delete(child, key, record);
            
            // 检查是否需要合并
            if (child.getHeader().getKeyCount() < (order - 1) / 2) {
                mergeInternal(page, child);
            }
        }
    }

    private List<Record> rangeSearch(Page page, Object minKey, Object maxKey) {
        List<Record> result = new ArrayList<>();
        
        if (page.getHeader().isLeaf()) {
            // 叶子节点范围查询
            int keyCount = page.getHeader().getKeyCount();
            for (int i = 0; i < keyCount; i++) {
                Object key = page.getKey(i);
                if (compare(key, minKey) >= 0 && compare(key, maxKey) <= 0) {
                    result.add(page.getRecord(i));
                }
            }
        } else {
            // 内部节点范围查询
            int keyCount = page.getHeader().getKeyCount();
            for (int i = 0; i < keyCount; i++) {
                Object key = page.getKey(i);
                if (compare(key, minKey) >= 0) {
                    Page child = page.getChild(i);
                    result.addAll(rangeSearch(child, minKey, maxKey));
                }
            }
        }
        
        return result;
    }

    private List<Record> search(Page page, Object key) {
        if (page.getHeader().isLeaf()) {
            // 叶子节点精确查询
            int keyCount = page.getHeader().getKeyCount();
            for (int i = 0; i < keyCount; i++) {
                if (compare(page.getKey(i), key) == 0) {
                    List<Record> result = new ArrayList<>();
                    result.add(page.getRecord(i));
                    return result;
                }
            }
            return new ArrayList<>();
        } else {
            // 内部节点精确查询
            Page child = findChild(page, key);
            return search(child, key);
        }
    }

    private void insertIntoLeaf(Page page, Object key, Record record) {
        int keyCount = page.getHeader().getKeyCount();
        int insertPos = 0;
        
        // 找到插入位置
        while (insertPos < keyCount && compare(page.getKey(insertPos), key) < 0) {
            insertPos++;
        }
        
        // 移动后面的键和记录
        for (int i = keyCount; i > insertPos; i--) {
            page.setKey(i, page.getKey(i - 1));
            page.setRecord(i, page.getRecord(i - 1));
        }
        
        // 插入新键和记录
        page.setKey(insertPos, key);
        page.setRecord(insertPos, record);
        page.getHeader().setKeyCount(keyCount + 1);
    }

    private void splitLeaf(Page page, Object key, Record record) {
        // 创建新页面
        Page newPage = new Page(0);
        newPage.getHeader().setPageType(Page.PageType.INDEX.getValue());
        newPage.getHeader().setIsLeaf(true);
        
        // 计算分裂点
        int splitPoint = order / 2;
        
        // 移动后半部分到新页面
        int keyCount = page.getHeader().getKeyCount();
        for (int i = splitPoint; i < keyCount; i++) {
            newPage.addKey(page.getKey(i));
            newPage.addRecord(page.getRecord(i));
        }
        
        // 更新键数量
        page.getHeader().setKeyCount(splitPoint);
        newPage.getHeader().setKeyCount(keyCount - splitPoint);
        
        // 插入新键值
        if (compare(key, page.getKey(splitPoint - 1)) < 0) {
            insertIntoLeaf(page, key, record);
        } else {
            insertIntoLeaf(newPage, key, record);
        }
        
        // 更新父节点
        updateParent(page, newPage, newPage.getKey(0));
    }

    private void splitInternal(Page page, Page child) {
        // 创建新页面
        Page newPage = new Page(0);
        newPage.getHeader().setPageType(Page.PageType.INDEX.getValue());
        newPage.getHeader().setIsLeaf(false);
        
        // 计算分裂点
        int splitPoint = order / 2;
        
        // 移动后半部分到新页面
        int keyCount = child.getHeader().getKeyCount();
        for (int i = splitPoint + 1; i < keyCount; i++) {
            newPage.addKey(child.getKey(i));
            newPage.addChild(child.getChild(i));
        }
        newPage.addChild(child.getChild(keyCount));
        
        // 更新键数量
        child.getHeader().setKeyCount(splitPoint);
        newPage.getHeader().setKeyCount(keyCount - splitPoint - 1);
        
        // 更新父节点
        updateParent(page, newPage, child.getKey(splitPoint));
    }

    private void deleteFromLeaf(Page page, Object key, Record record) {
        int keyCount = page.getHeader().getKeyCount();
        int deletePos = -1;
        
        // 找到删除位置
        for (int i = 0; i < keyCount; i++) {
            if (compare(page.getKey(i), key) == 0 && page.getRecord(i).equals(record)) {
                deletePos = i;
                break;
            }
        }
        
        if (deletePos == -1) {
            return;
        }
        
        // 移动后面的键和记录
        for (int i = deletePos; i < keyCount - 1; i++) {
            page.setKey(i, page.getKey(i + 1));
            page.setRecord(i, page.getRecord(i + 1));
        }
        
        page.getHeader().setKeyCount(keyCount - 1);
    }

    private void mergeLeaf(Page page) {
        // 获取父节点
        Page parent = findParent(page);
        if (parent == null) {
            return; // 根节点不需要合并
        }

        // 找到当前节点在父节点中的位置
        int index = findChildIndex(parent, page);
        if (index == -1) {
            return;
        }

        // 尝试与左兄弟节点合并
        if (index > 0) {
            Page leftSibling = parent.getChild(index - 1);
            if (leftSibling.getHeader().getKeyCount() + page.getHeader().getKeyCount() <= order - 1) {
                // 合并到左兄弟节点
                mergeToLeftSibling(leftSibling, page);
                // 从父节点中删除当前节点
                parent.removeKey(index - 1);
                parent.removeChild(index);
                return;
            }
        }

        // 尝试与右兄弟节点合并
        if (index < parent.getHeader().getKeyCount()) {
            Page rightSibling = parent.getChild(index + 1);
            if (rightSibling.getHeader().getKeyCount() + page.getHeader().getKeyCount() <= order - 1) {
                // 合并到右兄弟节点
                mergeToRightSibling(page, rightSibling);
                // 从父节点中删除当前节点
                parent.removeKey(index);
                parent.removeChild(index + 1);
                return;
            }
        }
    }

    private void mergeInternal(Page page, Page child) {
        // 获取父节点
        Page parent = findParent(page);
        if (parent == null) {
            return; // 根节点不需要合并
        }

        // 找到当前节点在父节点中的位置
        int index = findChildIndex(parent, page);
        if (index == -1) {
            return;
        }

        // 尝试与左兄弟节点合并
        if (index > 0) {
            Page leftSibling = parent.getChild(index - 1);
            if (leftSibling.getHeader().getKeyCount() + page.getHeader().getKeyCount() + 1 <= order - 1) {
                // 合并到左兄弟节点
                mergeInternalToLeftSibling(leftSibling, page, parent.getKey(index - 1));
                // 从父节点中删除当前节点
                parent.removeKey(index - 1);
                parent.removeChild(index);
                return;
            }
        }

        // 尝试与右兄弟节点合并
        if (index < parent.getHeader().getKeyCount()) {
            Page rightSibling = parent.getChild(index + 1);
            if (rightSibling.getHeader().getKeyCount() + page.getHeader().getKeyCount() + 1 <= order - 1) {
                // 合并到右兄弟节点
                mergeInternalToRightSibling(page, rightSibling, parent.getKey(index));
                // 从父节点中删除当前节点
                parent.removeKey(index);
                parent.removeChild(index + 1);
                return;
            }
        }
    }

    private void updateParent(Page page, Page newPage, Object key) {
        // 获取父节点
        Page parent = findParent(page);
        if (parent == null) {
            // 当前节点是根节点，需要创建新的根节点
            Page newRoot = new Page(0);
            newRoot.getHeader().setPageType(Page.PageType.INDEX.getValue());
            newRoot.getHeader().setIsLeaf(false);
            newRoot.addKey(key);
            newRoot.addChild(page);
            newRoot.addChild(newPage);
            // 更新索引根节点
            for (Map.Entry<String, Page> entry : indexRoots.entrySet()) {
                if (entry.getValue() == page) {
                    entry.setValue(newRoot);
                    break;
                }
            }
            return;
        }

        // 找到当前节点在父节点中的位置
        int index = findChildIndex(parent, page);
        if (index == -1) {
            return;
        }

        // 在父节点中插入新的键和子节点
        parent.addKey(index, key);
        parent.addChild(index + 1, newPage);
    }

    private Page findParent(Page page) {
        for (Page root : indexRoots.values()) {
            Page parent = findParentHelper(root, page);
            if (parent != null) {
                return parent;
            }
        }
        return null;
    }

    private Page findParentHelper(Page current, Page target) {
        if (current.getHeader().isLeaf()) {
            return null;
        }

        for (int i = 0; i <= current.getHeader().getKeyCount(); i++) {
            Page child = current.getChild(i);
            if (child == target) {
                return current;
            }
            Page parent = findParentHelper(child, target);
            if (parent != null) {
                return parent;
            }
        }
        return null;
    }

    private int findChildIndex(Page parent, Page child) {
        for (int i = 0; i <= parent.getHeader().getKeyCount(); i++) {
            if (parent.getChild(i) == child) {
                return i;
            }
        }
        return -1;
    }

    private void mergeToLeftSibling(Page leftSibling, Page page) {
        int keyCount = page.getHeader().getKeyCount();
        for (int i = 0; i < keyCount; i++) {
            leftSibling.addKey(page.getKey(i));
            leftSibling.addRecord(page.getRecord(i));
        }
    }

    private void mergeToRightSibling(Page page, Page rightSibling) {
        int keyCount = page.getHeader().getKeyCount();
        for (int i = 0; i < keyCount; i++) {
            rightSibling.addKey(0, page.getKey(i));
            rightSibling.addRecord(0, page.getRecord(i));
        }
    }

    private void mergeInternalToLeftSibling(Page leftSibling, Page page, Object key) {
        leftSibling.addKey(key);
        int keyCount = page.getHeader().getKeyCount();
        for (int i = 0; i < keyCount; i++) {
            leftSibling.addKey(page.getKey(i));
            leftSibling.addChild(page.getChild(i));
        }
        leftSibling.addChild(page.getChild(keyCount));
    }

    private void mergeInternalToRightSibling(Page page, Page rightSibling, Object key) {
        rightSibling.addKey(0, key);
        int keyCount = page.getHeader().getKeyCount();
        for (int i = keyCount - 1; i >= 0; i--) {
            rightSibling.addKey(0, page.getKey(i));
            rightSibling.addChild(0, page.getChild(i + 1));
        }
        rightSibling.addChild(0, page.getChild(0));
    }

    private Page findChild(Page page, Object key) {
        int keyCount = page.getHeader().getKeyCount();
        for (int i = 0; i < keyCount; i++) {
            if (compare(key, page.getKey(i)) < 0) {
                return page.getChild(i);
            }
        }
        return page.getChild(keyCount);
    }

    private int compare(Object key1, Object key2) {
        if (key1 == null && key2 == null) return 0;
        if (key1 == null) return -1;
        if (key2 == null) return 1;
        return ((Comparable)key1).compareTo(key2);
    }
} 