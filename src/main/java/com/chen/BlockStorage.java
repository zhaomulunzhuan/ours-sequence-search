package com.chen;


import java.io.Serializable;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.List;

interface BloomFilterStorage {
    void insertElement(String element);
    boolean test(String element);
    int countOnes();
}

class RowBloomFilterStorage implements BloomFilterStorage {//用于查询
    private BitSet[] bitSets;

    public RowBloomFilterStorage(int size, int capacity) {
        bitSets = new BitSet[capacity];
        for (int i = 0; i < capacity; i++) {
            bitSets[i] = new BitSet(size);
        }
    }

    @Override
    public void insertElement(String element) {
        // Implement insertElement for Row storage
    }

    @Override
    public boolean test(String element) {
        // Implement test for Row storage
        return false;
    }

    @Override
    public int countOnes() {
        // Implement countOnes for Row storage
        return 0;
    }
}

class ColumnBloomFilterStorage implements BloomFilterStorage , Serializable{//用于构建，插入，删除
    private static final long serialVersionUID = 1L;
    private BitSet[] bitSets;

    public ColumnBloomFilterStorage(int size, int capacity) {
        bitSets = new BitSet[size];
        for (int i = 0; i < size; i++) {
            bitSets[i] = new BitSet(capacity);
        }
    }
    @Override
    public void insertElement(String element) {
        // Implement insertElement for Row storage
    }

    @Override
    public boolean test(String element) {
        // Implement test for Row storage
        return false;
    }

    @Override
    public int countOnes() {
        // Implement countOnes for Row storage
        return 0;
    }

}


