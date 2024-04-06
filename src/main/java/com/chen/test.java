package com.chen;

import java.io.*;
import java.sql.SQLOutput;
import java.util.*;

public class test {
    public static void main(String[] args) throws IOException {
//        初次构建
//        DataPreProcessing.dataPreprocessing();
//        Build.buildIndex();
        //不是初次构建，从序列化文件中加载
        Build.buildIndexFromSER();
        MetaData.outputMetadata();

        index.printIndex();

//        Delete.deleteDataset("GCF_000005845.2_ASM584v2_genomic.fna");

//        Insert.insertDatasets("Insert_3.txt");

//        MetaData.outputMetadata();
//        index.printIndex();
//        System.out.println(index.getBlock(0).queryExistence("CTGAGGATACTCTTCTAAAGACATTCTTGTA"));
//
//        System.out.println(index.getBlock(11).getNumsBloomFilter());
//
//        MetaData.outputMetadata();
//
//        index.printIndex();
//        Block block=index.getBlock(11);
//        List<BloomFilter> bflist=block.getBloomFilterList();
//        System.out.println(bflist);
//        System.out.println(block.getRowStorageBlock().size());
//        block.convertToColumnStorage();
//        System.out.println(block.getBloomFilterList().get(0).getBitarray().size()*block.getNumsBloomFilter());
//        MetaData.outputMetadata();
//        index.printIndex();
//
        long startTime = System.currentTimeMillis();

        Query.querykmer2("TTATTACACCTCCCTGAGGATACTCTTCTAA");

        long endTime = System.currentTimeMillis();
        // 计算代码的运行时间
        long elapsedTime = endTime - startTime;
        System.out.println("代码运行时间：" + elapsedTime + " 毫秒");


        long startTime2 = System.currentTimeMillis();

        Query.querykmer("TTATTACACCTCCCTGAGGATACTCTTCTAA");

        long endTime2 = System.currentTimeMillis();
        // 计算代码的运行时间
        long elapsedTime2 = endTime2 - startTime2;
        System.out.println("代码运行时间：" + elapsedTime2 + " 毫秒");


    }


}
