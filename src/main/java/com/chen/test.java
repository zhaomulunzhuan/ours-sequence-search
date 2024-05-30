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
        long startBuild = System.currentTimeMillis();

        Build.buildIndexFromSER();

        long endBuild = System.currentTimeMillis();
        long BuildTime = endBuild - startBuild;
        System.out.println("反序列化构建时间" + BuildTime + "毫秒");


        //测试插入
//        long startInsert=System.currentTimeMillis();
//
//        Insert.insertDatasets("Insert_1.txt");
//        Insert.insertDatasets("Insert_2.txt");
//        long endInsert=System.currentTimeMillis();
//        long InsertTime=endInsert-startInsert;
//        System.out.println("插入时间"+InsertTime+"毫秒");
//
//        MetaData.outputMetadata();


        //测试查询
        long startQuery = System.currentTimeMillis();


        Query.queryFile("D:\\SequenceSearch_2\\query.txt");

        long endQuery = System.currentTimeMillis();
        long QueryTime = endQuery - startQuery;
        System.out.println("按行查询时间" + QueryTime + "毫秒");

        //按列查询 需要行存储到列存储转换
//        long startQuery=System.currentTimeMillis();
//        Query.queryFileAScol("D:\\SequenceSearch_2\\query.txt");
//        long endQuery=System.currentTimeMillis();
//        long QueryTime=endQuery-startQuery;
//        System.out.println("按列查询时间"+QueryTime+"毫秒");

        //测试删除 删除
//        MetaData.outputMetadata();
//        Delete.deleteDatasets("D:\\SequenceSearch_2\\deleteDatasets.txt");
//        MetaData.outputMetadata();
//        Insert.insertDatasets("Insert_1.txt");
//        MetaData.outputMetadata();
//        Query.queryFile("D:\\SequenceSearch_2\\query.txt");

    }

}