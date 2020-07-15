package com.datasoft.compress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.*;
import org.apache.hadoop.util.ReflectionUtils;


import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;

/**
 * 按编号压缩解压缩的例子
 */

public class HadoopCompress {

    private static String BZip2Codec = "org.apache.hadoop.io.compress.BZip2Codec";
    private static String DefaultCodec = "org.apache.hadoop.io.compress.DefaultCodec";
    private static String GzipCodec = "org.apache.hadoop.io.compress.GzipCodec";


    public static void main(String[] args) throws IOException, ClassNotFoundException {
        //compress("d:/tmp/hello.txt", GzipCodec);
        decompress("d:/tmp/hello.txt.deflate");
    }



    private static void compress(String filename, String codecname) throws IOException, ClassNotFoundException {
        //创建输入流

        FileInputStream fis = new FileInputStream(filename);

        //创建输出流

        Class classCodec = Class.forName(codecname);

        CompressionCodec compressionCodec = (CompressionCodec) ReflectionUtils.newInstance(classCodec, new Configuration());

        FileOutputStream fos = new FileOutputStream(filename + compressionCodec.getDefaultExtension());

        //将输出流包装到压缩格式的输出流中
        CompressionOutputStream cos = compressionCodec.createOutputStream(fos);

        //流的对拷

        IOUtils.copyBytes(fis, cos, 1024*1024, false);

        //关闭资源
        IOUtils.closeStream(cos);
        IOUtils.closeStream(fos);
        IOUtils.closeStream(fis);
    }

    private static void decompress(String filename) throws IOException {
        //验证合法性

        CompressionCodecFactory factory = new CompressionCodecFactory(new Configuration());
        CompressionCodec codec = factory.getCodec(new Path(filename));
        if (codec == null){
            System.out.println("can not decompress, because undefined codec");
            return;
        }

        //创建输入流
        FileInputStream fis = new FileInputStream(filename);
        CompressionInputStream cis = codec.createInputStream(fis);

        //创建输出流
        FileOutputStream fos = new FileOutputStream(filename + ".decode");

        //流的对拷

        IOUtils.copyBytes(cis, fos, 1024*1024, false);


        //关闭资源
        IOUtils.closeStream(fos);
        IOUtils.closeStream(cis);
        IOUtils.closeStream(fis);
    }
}
