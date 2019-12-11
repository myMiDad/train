package com.tom.source;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDrivenSource;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.source.AbstractSource;
import org.apache.flume.source.ExecSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.charset.Charset;
import java.util.concurrent.*;

import static java.util.concurrent.Executors.*;

/**
 * ClassName: ExecSource
 * Description:
 *
 * @author Mi_dad
 * @date 2019/12/11 9:46
 */
public class ExecSourceDy extends AbstractSource implements EventDrivenSource, Configurable {
    private static final Logger logger = LoggerFactory.getLogger(ExecSource.class);

    //日志文件路径
    private String filePath;
    //偏移量文件路径
    private String offsetPath;
    //编码格式
    private String charset;
    //睡眠时间
    private Long interval;


    //线程池
    private ExecutorService executor;

    @Override
    public void configure(Context context) {
        filePath = context.getString("filePath");
        offsetPath = context.getString("offsetPath");
        charset = context.getString("charset");
        interval = context.getLong("interval");
    }

    @Override
    public synchronized void start() {
        logger.info("Exec source starting......" );
        //发送Event
        ChannelProcessor channelProcessor = getChannelProcessor();
        ThreadFactory threadFactory = new ThreadFactory(){
            @Override
            public Thread newThread(Runnable r) {
                Thread thread = new Thread(r);
                thread.setName("线程"+thread.getId());
                thread.setDaemon(true);
                return thread;
            }
        };

        this.executor = new ThreadPoolExecutor(5, 10, 0L, TimeUnit.SECONDS ,new LinkedBlockingQueue<>(),threadFactory);

        System.out.println(filePath+"-"+offsetPath+"-"+interval+"-"+charset+"-"+channelProcessor);
        executor.execute(new RunnerFile(filePath, offsetPath, interval, charset, channelProcessor));
        super.start();

        logger.debug("Exec source started");
    }

    @Override
    public synchronized void stop() {
        super.stop();
    }

    private static class RunnerFile implements Runnable{
        //编码格式
        private String charset;
        //睡眠时间
        private Long interval;
        //channelProcessor
        private ChannelProcessor channelProcessor;
        //偏移量文件的实例
        private File file;
        //偏移量
        private Long offset = 0L;
        //偏移量读取
        private RandomAccessFile raf;

        private RunnerFile(){

        }

        private RunnerFile(String filePath,String offsetPath,long interval,String charset,ChannelProcessor channelProcessor){
            //日志文件路径
            //偏移量文件路径
            this.charset = charset;
            this.interval = interval;
            this.channelProcessor = channelProcessor;

            //判断偏移量是否存在，如果存在就读取，不存在就创建
            file = new File(offsetPath);
            if (!file.exists()){
                try {
                    file.createNewFile();
                } catch (IOException e) {
                    //打印文件创建失败信息
                    e.printStackTrace();
                }
            }
            try {
                String offsetStr = FileUtils.readFileToString(file);
                if(!StringUtils.isEmpty(offsetStr)){
                    offset = Long.parseLong(offsetStr);
                }
                //跳到偏移量位置读取
                raf = new RandomAccessFile(filePath, "r");
                raf.seek(offset);
            } catch (IOException e) {
                //打印文件读取错误
                e.printStackTrace();
            }
        }
        @Override
        public void run() {
            int i = 0;
            while (true){
                try {
                    System.out.println("------------------"+i+"------------------");
                    i++;
                    String line = raf.readLine();
                    //先判断是否为空，如果为空让程序睡眠
                    if (line!=null){
                        //封装Event
                        Event event = EventBuilder.withBody(line, Charset.forName(charset));
                        //发送Event对象给Channel
                        channelProcessor.processEvent(event);
                        //更新偏移量
                        offset = raf.getFilePointer();
                        FileUtils.writeStringToFile(file, offset.toString());
                        logger.info("读取完成");
                    }else {
                        logger.info("sleep");
                        Thread.sleep(interval);
                    }

                } catch (IOException | InterruptedException e) {
                    //读取错误或者sleep异常
                    logger.error("循环出错");
                    e.printStackTrace();

                }
            }
        }
    }
}
