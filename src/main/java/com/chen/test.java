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
        long startBuild=System.currentTimeMillis();

        Build.buildIndexFromSER();

        long endBuild=System.currentTimeMillis();
        long BuildTime=endBuild-startBuild;
        System.out.println("构建时间"+BuildTime+"毫秒");

        long startQuery=System.currentTimeMillis();

//        Query.Exactquerykmers2("D:\\SequenceSearch_2\\query.txt");
        Query.queryFile("D:\\SequenceSearch_2\\query.txt");

        long endQuery=System.currentTimeMillis();
        long QueryTime=endQuery-startQuery;
        System.out.println("查询时间"+QueryTime+"毫秒");


    }


}
