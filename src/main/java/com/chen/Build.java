package com.chen;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Build {
    private static int cur_block_index;

    static {
        cur_block_index=-1;
    }

    public static void buildIndex() throws IOException {
        //遍历每个段，确定group_nums和这个段的数据集列表
        //获得存放分段结果的目录
        long buildstart=System.nanoTime();
        String segementedDirectory=ConfigReader.getProperty("project-root-directory")+"/"+ConfigReader.getProperty("segementedDirectory");

        int count=0;
        //遍历目录下的所有文件
        File folder=new File(segementedDirectory);
        File[] files=folder.listFiles();
        if(files!=null){
            for(File file:files){//一个file代表处理一个段  文件名 segement_1.txt
                if(file.isFile()&& file.getName().startsWith("segement_") && file.getName().endsWith(".txt")){
                    // 提取文件名中的数字 是当前段的组数
                    int group_nums = extractSegmentNumber(file.getName());
//                    System.out.println("Segment number: " + group_nums);
                    //当前段存储的数据集的kmer文件路径
                    ArrayList<String> samplesList=processTxtFile(file);
                    //当前段每个组需要的block数量
                    assert samplesList != null;
                    //按照64大小分成子列表
                    // 调用 chunks 方法并指定类型参数为 String
                    int chunk_size= Integer.parseInt(ConfigReader.getProperty("Block-max-size"));
                    Iterable<List<String>> chunkIterator = Utils.<String>chunks(samplesList, chunk_size);
                    for (List<String> chunk : chunkIterator) {//chunk为当前要处理的子列表，长度等于64，最后一个子列表的长度可能小于64
                        for(int group_index=0;group_index<group_nums;group_index++){//对于一个子列表，每个组创建一个Block
                            int block_index=cur_block_index+1;
                            Block block=new Block(block_index,chunk.size());
                            index.addBlock(block);
                            MetaData.addGroupNumAndGroupIdxToBlocks(group_nums,group_index,block_index);
                            cur_block_index++;
                        }
                        for(int j=0;j<chunk.size();j++){
                            String samplePath=chunk.get(j);//获得当前要处理的数据集的文件路径
                            count++;
//                            System.out.println("当前处理数据集数量："+count+",数据集文件:"+samplePath);
                            try (BufferedReader reader=new BufferedReader(new FileReader(samplePath))){
                                String kmer;
                                while ((kmer=reader.readLine())!=null){
                                    // 对每个 kmer 进行哈希操作，确保哈希值在 group_nums 范围内
                                    int group_idx = (kmer.hashCode()& 0x7FFFFFFF) % group_nums;//确保不是负数
                                    int global_block_idx=cur_block_index-group_nums+1+group_idx;
                                    index.getBlock(global_block_idx).addElement(j,kmer);
                                }
                            }catch (IOException e){
                                e.printStackTrace();
                            }
                        }
                    }

                }
            }
        }
        long buildend=System.nanoTime();
        long buildelapsedTime=buildend-buildstart;
        double elapsedTimeInseconds=(double) buildelapsedTime/1_000_000_000.0;
        for(Block block:index.getBlockList()){//将block转换为按行存储
            block.serialize();
        }
        System.out.println("build时间"+elapsedTimeInseconds+"秒");
        serializeAll();
//        //元数据序列化
//        String MetaDataFile=ConfigReader.getProperty("project-root-directory")+"/"+"metadata.ser";
//        MetaData.serialize(MetaDataFile);
//        //索引序列化，即将块信息和布隆过滤器信息序列化
//        String indexFile=ConfigReader.getProperty("project-root-directory")+"/"+"index.ser";
//        index.serialize(indexFile);
//        //将配置文件存储项目目录下
//        ConfigReader.saveConfigFile();
//        index.printIndex();
//        MetaData.outputMetadata();
    }

    public static void buildIndexFromSER(){
        String MetaDataFile=ConfigReader.getProperty("project-root-directory")+"/"+"serializeFile/metadata.ser";
        MetaData.deserialize(MetaDataFile);
//        MetaData.outputMetadata();
        String indexFile=ConfigReader.getProperty("project-root-directory")+"/"+"serializeFile/index.ser";
        index.deserialize(indexFile);
//        index.printIndex();
        // 构造配置文件目标文件路径
        String configFile=ConfigReader.getProperty("project-root-directory")+"/config.properties";
        ConfigReader.loadProperties(configFile);
    }


    public static int extractSegmentNumber(String fileName) {
        String[] parts = fileName.split("_");
        String numberPart = parts[1].split("\\.")[0]; // 获取_后面的数字部分，并移除.txt后缀
        return Integer.parseInt(numberPart);
    }

    public static ArrayList<String> processTxtFile(File file) {
        ArrayList<String> kmersDatasetPaths=new ArrayList<>();
        try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
            String line;
            while ((line = reader.readLine()) != null) {
                // 读取每行数据集文件路径
                kmersDatasetPaths.add(line);
            }
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
        return kmersDatasetPaths;
    }

    public static void serializeAll() throws IOException {
        //元数据序列化
        String MetaDataFile=ConfigReader.getProperty("project-root-directory")+"/"+"serializeFile/metadata.ser";
        MetaData.serialize(MetaDataFile);
        //索引序列化，即将块信息和布隆过滤器信息序列化
        String indexFile=ConfigReader.getProperty("project-root-directory")+"/"+"serializeFile/index.ser";
        index.serialize(indexFile);
        //将配置文件存储项目目录下
        ConfigReader.saveConfigFile();
    }



}
