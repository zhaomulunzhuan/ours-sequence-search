package com.chen;

import java.io.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.stream.Collectors;

public class Query {

    private static long query_time_row_bitset=0;//按行查询，列转行时使用bitset

    private static long query_time_row_bitset_parallel=0;////按行查询，列转行时使用bitset并且使用多线程进行查询

    private static long query_time_row_longarray=0;//按行查询，将bitset转换为long数组再查询

    public static long getQuery_time_row_bitset() {
        return query_time_row_bitset;
    }

    public static long getQuery_time_row_bitset_parallel(){
        return query_time_row_bitset_parallel;
    }

    public static long getQuery_time_row_longarray() {
        return query_time_row_longarray;
    }

    private static final ExecutorService executorService = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());

    //按行查 转置为longbitset查
    //第一种：对于查询kmer，一个段一个段的查询 与第二种并行查询不同的是querykmer函数
    public static void queryFile_bitset_row(String filePath){
        String rootDirectory = ConfigReader.getProperty("project-root-directory");
        String queryresultFile = rootDirectory+"/"+"query_result(row_bitset).txt";
        try(
                BufferedReader reader=new BufferedReader(new FileReader(filePath));
                BufferedWriter writer=new BufferedWriter(new FileWriter(queryresultFile))){
            String line;
            String sequence="";
            while ((line=reader.readLine())!=null){
                if(line.startsWith(">")){
                    //查询
                    if (!sequence.isEmpty()){
                        writer.write(sequence+"\n");
                        ExactquerySequenceAsRow(sequence,writer);
                        writer.write(line+"\n");
                    }else {
                        writer.write(line+"\n");
                    }
                    sequence="";
                }else {
                    sequence+=line.trim().toUpperCase();
                }
            }
            if(!sequence.isEmpty()){
                writer.write(sequence + "\n");
                //查询最后一段序列
                ExactquerySequenceAsRow(sequence,writer);
            }
        }catch (IOException e){
            System.err.println(e);
        }
    }

    public static void ExactquerySequenceAsRow(String sequence, BufferedWriter writer) throws IOException {
        long startquery=System.currentTimeMillis();

        int kmersize= Integer.parseInt(ConfigReader.getProperty("kmer-size"));
        List<String> kmerList=new ArrayList<>();
        // 切割sequence并将长度为kmersize的子字符串加入kmerList
        for (int i = 0; i <= sequence.length() - kmersize; i++) {
            String kmer = sequence.substring(i, i + kmersize);
            kmerList.add(kmer);
        }
//        for (String kmer : kmerList) {
//            System.out.println(kmer);
//        }
        List<String> result=new ArrayList<>(querykmerAsRow(kmerList.get(0)));
        for(String kmer:kmerList){
            result.retainAll(querykmerAsRow(kmer));
        }

        long endquery = System.currentTimeMillis();
        query_time_row_bitset+=(endquery-startquery);


//        System.out.println("包含输入文件中查询序列的数据集");
//        for (String datasetName:result){
//            System.out.println(datasetName);
//        }
        writer.write("查询结果\n");
        // 将查询结果写入到结果文件
        if (!result.isEmpty()){
            for (String datasetName : result) {
                writer.write(datasetName + "\n");
            }
        }else {
            writer.write("未查询到包含查询序列的数据集"+"\n");
        }

    }

    public static List<String> querykmerAsRow(String kmer){
        List<String> results=new ArrayList<>();

        for(Map.Entry<Integer,List<Integer>> entry:MetaData.getGroupNum_to_samples().entrySet()) {
            int group_nums = entry.getKey();
            List<Integer> datasetIdxs = entry.getValue();
            //得到段内组索引
//            int group_idx = Math.abs(kmer.hashCode()) % group_nums;
            int group_idx =(kmer.hashCode()& 0x7FFFFFFF) % group_nums;

            List<Integer> blockList = MetaData.getBlocksByGroupNumAndGroupIdx(group_nums, group_idx);
            //k个哈希索引
            List<Long> rowIdxs = new ArrayList<>();
            int k = Integer.parseInt(ConfigReader.getProperty("k"));
            int m = Integer.parseInt(ConfigReader.getProperty("BF-size"));
            rowIdxs = Utils.myHash(kmer, k, m);
            List<String> cur_datasetResult = index.searchBlocksAsRow_bitset_row(blockList, datasetIdxs, rowIdxs);
            results.addAll(cur_datasetResult);
        }
//        System.out.println(kmer+"查询到数据集：");
//        if (results.size()==0){
//            System.out.println("未查询到包含查询元素的数据集");
//        }else {
//            for(String dataset:results){
//                System.out.println(dataset);
//            }
//        }
        return results;
    }

     //第二种：对于查询kmer，多线程多个段并行查询
    public static void queryFile_bitset_row_parallel(String filePath){
        String rootDirectory = ConfigReader.getProperty("project-root-directory");
        String queryresultFile = rootDirectory+"/"+"query_result(row_bitset_parallel).txt";
        try(
                BufferedReader reader=new BufferedReader(new FileReader(filePath));
                BufferedWriter writer=new BufferedWriter(new FileWriter(queryresultFile))){
            String line;
            String sequence="";
            while ((line=reader.readLine())!=null){
                if(line.startsWith(">")){
                    //查询
                    if (!sequence.isEmpty()){
                        writer.write(sequence+"\n");
                        ExactquerySequenceAsRow_parallel(sequence,writer);
                        writer.write(line+"\n");
                    }else {
                        writer.write(line+"\n");
                    }
                    sequence="";
                }else {
                    sequence+=line.trim().toUpperCase();
                }
            }
            if(!sequence.isEmpty()){
                writer.write(sequence + "\n");
                //查询最后一段序列
                ExactquerySequenceAsRow_parallel(sequence,writer);
            }
        }catch (IOException | ExecutionException | InterruptedException e){
            System.err.println(e);
        }

        //关闭线程池
        executorService.shutdown();
        try {
            if (!executorService.awaitTermination(60, TimeUnit.SECONDS)) {
                executorService.shutdownNow();
                if (!executorService.awaitTermination(60, TimeUnit.SECONDS))
                    System.err.println("线程池未能正常终止");
            }
        } catch (InterruptedException ie) {
            executorService.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    public static void ExactquerySequenceAsRow_parallel(String sequence, BufferedWriter writer) throws IOException, ExecutionException, InterruptedException {
        long startquery=System.currentTimeMillis();

        int kmersize= Integer.parseInt(ConfigReader.getProperty("kmer-size"));
        List<String> kmerList=new ArrayList<>();
        // 切割sequence并将长度为kmersize的子字符串加入kmerList
        for (int i = 0; i <= sequence.length() - kmersize; i++) {
            String kmer = sequence.substring(i, i + kmersize);
            kmerList.add(kmer);
        }
//        for (String kmer : kmerList) {
//            System.out.println(kmer);
//        }
        List<String> result=new ArrayList<>(querykmerAsRow_parallel(kmerList.get(0)));
        for(String kmer:kmerList){
            result.retainAll(querykmerAsRow_parallel(kmer));
        }

        long endquery = System.currentTimeMillis();
        query_time_row_bitset_parallel+=(endquery-startquery);


//        System.out.println("包含输入文件中查询序列的数据集");
//        for (String datasetName:result){
//            System.out.println(datasetName);
//        }
        writer.write("查询结果\n");
        // 将查询结果写入到结果文件
        if (!result.isEmpty()){
            for (String datasetName : result) {
                writer.write(datasetName + "\n");
            }
        }else {
            writer.write("未查询到包含查询序列的数据集"+"\n");
        }

    }

    //并行查询
//    public static List<String> querykmerAsRow(String kmer) throws InterruptedException, ExecutionException {
//        List<String> results = new ArrayList<>();
//        Map<Integer, List<Integer>> groupNumToSamples = MetaData.getGroupNum_to_samples();
//        int k = Integer.parseInt(ConfigReader.getProperty("k"));
//        int m = Integer.parseInt(ConfigReader.getProperty("BF-size"));
//
//        // 创建一个线程池
//        List<Future<List<String>>> futures = new ArrayList<>();
//
//        // 提交任务到线程池
//        for (Map.Entry<Integer, List<Integer>> entry : groupNumToSamples.entrySet()) {
//            Future<List<String>> future = executorService.submit(() -> {
//                int group_nums = entry.getKey();
//                List<Integer> datasetIdxs = entry.getValue();
//
//                // 计算段内组索引
//                int group_idx = (kmer.hashCode() & 0x7FFFFFFF) % group_nums;
//
//                List<Integer> blockList = MetaData.getBlocksByGroupNumAndGroupIdx(group_nums, group_idx);
//
//                // 计算k个哈希索引
//                List<Long> rowIdxs = Utils.myHash(kmer, k, m);
//
//                List<String> cur_result = index.searchBlocksAsRow_bitset_row(blockList, datasetIdxs, rowIdxs);
//
//                // 搜索块并返回结果
//                return cur_result;
//            });
//            futures.add(future);
//        }
//
//        // 等待所有任务完成并收集结果
//        for (Future<List<String>> future : futures) {
//            results.addAll(future.get());
//        }
//
//        return results;
//    }

    public static List<String> querykmerAsRow_parallel(String kmer) throws InterruptedException, ExecutionException {
        List<String> results = Collections.synchronizedList(new ArrayList<>());
        Map<Integer, List<Integer>> groupNumToSamples = MetaData.getGroupNum_to_samples();
        int k = Integer.parseInt(ConfigReader.getProperty("k"));
        int m = Integer.parseInt(ConfigReader.getProperty("BF-size"));

        // 提交任务到线程池
        List<Callable<Void>> tasks = groupNumToSamples.entrySet().stream()
                .map(entry -> (Callable<Void>) () -> {
                    int group_nums = entry.getKey();
                    List<Integer> datasetIdxs = entry.getValue();

                    // 计算段内组索引
                    int group_idx = (kmer.hashCode() & 0x7FFFFFFF) % group_nums;
                    List<Integer> blockList = MetaData.getBlocksByGroupNumAndGroupIdx(group_nums, group_idx);

                    // 计算k个哈希索引
                    List<Long> rowIdxs = Utils.myHash(kmer, k, m);
                    List<String> cur_result = index.searchBlocksAsRow_bitset_row(blockList, datasetIdxs, rowIdxs);

                    results.addAll(cur_result);
                    return null;
                })
                .collect(Collectors.toList());

        // 执行所有任务
        executorService.invokeAll(tasks);

        return results;
    }



    //按列查
    public static void queryFileAScol(String filePath){
        String rootDirectory = ConfigReader.getProperty("project-root-directory");
        String queryresultFile = rootDirectory+"/"+"query_result(col).txt";
        try(
                BufferedReader reader=new BufferedReader(new FileReader(filePath));
                BufferedWriter writer=new BufferedWriter(new FileWriter(queryresultFile))){
            String line;
            String sequence="";
            while ((line=reader.readLine())!=null){
                if(line.startsWith(">")){
                    //查询
                    if (!sequence.isEmpty()){
                        writer.write(sequence+"\n");
                        ExactquerySequenceAScol(sequence,writer);
                        writer.write(line+"\n");
                    }else {
                        writer.write(line+"\n");
                    }
                    sequence="";
                }else {
                    sequence+=line.trim().toUpperCase();
                }
            }
            if(!sequence.isEmpty()){
                writer.write(sequence + "\n");
                //查询最后一段序列
                ExactquerySequenceAScol(sequence,writer);
            }
        }catch (IOException e){
            System.err.println(e);
        }
    }

    public static void ExactquerySequenceAScol(String sequence, BufferedWriter writer) throws IOException {
        int kmersize= Integer.parseInt(ConfigReader.getProperty("kmer-size"));
        List<String> kmerList=new ArrayList<>();
        // 切割sequence并将长度为kmersize的子字符串加入kmerList
        for (int i = 0; i <= sequence.length() - kmersize; i++) {
            String kmer = sequence.substring(i, i + kmersize);
            kmerList.add(kmer);
        }
//        for (String kmer : kmerList) {
//            System.out.println(kmer);
//        }
        List<String> result=new ArrayList<>(querykmerAScol(kmerList.get(0)));
        for(String kmer:kmerList){
            result.retainAll(querykmerAScol(kmer));
        }
//        System.out.println("包含输入文件中查询序列的数据集");
//        for (String datasetName:result){
//            System.out.println(datasetName);
//        }
        writer.write("查询结果\n");
        // 将查询结果写入到结果文件
        if (!result.isEmpty()){
            for (String datasetName : result) {
                writer.write(datasetName + "\n");
            }
        }else {
            writer.write("未查询到包含查询序列的数据集"+"\n");
        }

    }
    public static List<String> querykmerAScol(String kmer){
        List<String> results=new ArrayList<>();

        for(Map.Entry<Integer,List<Integer>> entry:MetaData.getGroupNum_to_samples().entrySet()) {
            int group_nums = entry.getKey();
            List<Integer> datasetIdxs = entry.getValue();
            //得到段内组索引
//            int group_idx = Math.abs(kmer.hashCode()) % group_nums;
            int group_idx =(kmer.hashCode()& 0x7FFFFFFF) % group_nums;
            List<Integer> blockList = MetaData.getBlocksByGroupNumAndGroupIdx(group_nums, group_idx);
            //k个哈希索引
//            List<Long> rowIdxs = new ArrayList<>();
            int k = Integer.parseInt(ConfigReader.getProperty("k"));
            int m = Integer.parseInt(ConfigReader.getProperty("BF-size"));
//            rowIdxs = Utils.myHash(kmer, k, m);
            List<String> cur_datasetResult = index.searchBlocksAScol(blockList, datasetIdxs, kmer);
            results.addAll(cur_datasetResult);
        }
//        System.out.println(kmer+"查询到数据集：");
//        if (results.size()==0){
//            System.out.println("未查询到包含查询元素的数据集");
//        }else {
//            for(String dataset:results){
//                System.out.println(dataset);
//            }
//        }
        return results;
    }


    //按行查 longbiset转换为long数组 查
    public static void queryFile_longArray_row(String filePath){
        String rootDirectory = ConfigReader.getProperty("project-root-directory");
        String queryresultFile = rootDirectory+"/"+"query_result(row_longarray).txt";
        try(
                BufferedReader reader=new BufferedReader(new FileReader(filePath));
                BufferedWriter writer=new BufferedWriter(new FileWriter(queryresultFile))){
            String line;
            String sequence="";
            while ((line=reader.readLine())!=null){
                if(line.startsWith(">")){
                    //查询
                    if (!sequence.isEmpty()){
                        writer.write(sequence+"\n");
                        ExactquerySequenceAsRow_longArray_row(sequence,writer);
                        writer.write(line+"\n");
                    }else {
                        writer.write(line+"\n");
                    }
                    sequence="";
                }else {
                    sequence+=line.trim().toUpperCase();
                }
            }
            if(!sequence.isEmpty()){
                writer.write(sequence + "\n");
                //查询最后一段序列
                ExactquerySequenceAsRow_longArray_row(sequence,writer);
            }
        }catch (IOException e){
            System.err.println(e);
        }
    }

    public static void ExactquerySequenceAsRow_longArray_row(String sequence, BufferedWriter writer) throws IOException {
        long startquery=System.currentTimeMillis();

        int kmersize= Integer.parseInt(ConfigReader.getProperty("kmer-size"));
        List<String> kmerList=new ArrayList<>();
        // 切割sequence并将长度为kmersize的子字符串加入kmerList
        for (int i = 0; i <= sequence.length() - kmersize; i++) {
            String kmer = sequence.substring(i, i + kmersize);
            kmerList.add(kmer);
        }

        List<String> result=new ArrayList<>(querykmerAsRow_longArray_row(kmerList.get(0)));
        for(String kmer:kmerList){
            result.retainAll(querykmerAsRow_longArray_row(kmer));
        }
//        System.out.println("包含输入文件中查询序列的数据集");
//        for (String datasetName:result){
//            System.out.println(datasetName);
//        }

        long endquery = System.currentTimeMillis();
        query_time_row_longarray+=(endquery-startquery);

        writer.write("查询结果\n");
        // 将查询结果写入到结果文件
        if (!result.isEmpty()){
            for (String datasetName : result) {
                writer.write(datasetName + "\n");
            }
        }else {
            writer.write("未查询到包含查询序列的数据集"+"\n");
        }

    }

    public static List<String> querykmerAsRow_longArray_row(String kmer){
        List<String> results=new ArrayList<>();

        for(Map.Entry<Integer,List<Integer>> entry:MetaData.getGroupNum_to_samples().entrySet()) {
            int group_nums = entry.getKey();
            List<Integer> datasetIdxs = entry.getValue();
            //得到段内组索引
//            int group_idx = Math.abs(kmer.hashCode()) % group_nums;
            int group_idx =(kmer.hashCode()& 0x7FFFFFFF) % group_nums;
            List<Integer> blockList = MetaData.getBlocksByGroupNumAndGroupIdx(group_nums, group_idx);
            //k个哈希索引
            List<Long> rowIdxs = new ArrayList<>();
            int k = Integer.parseInt(ConfigReader.getProperty("k"));
            int m = Integer.parseInt(ConfigReader.getProperty("BF-size"));
            rowIdxs = Utils.myHash(kmer, k, m);
            List<String> cur_datasetResult = index.searchBlocksAsRow_longArray_row(blockList, datasetIdxs, rowIdxs);
            results.addAll(cur_datasetResult);
        }
//        System.out.println(kmer+"查询到数据集：");
//        if (results.size()==0){
//            System.out.println("未查询到包含查询元素的数据集");
//        }else {
//            for(String dataset:results){
//                System.out.println(dataset);
//            }
//        }
        return results;
    }

}
