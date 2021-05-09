package org.flume;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDrivenSource;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.conf.Configurable;
import org.apache.flume.source.AbstractSource;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.util.ArrayList;
import java.util.List;


public class MySourceForFlume extends AbstractSource
        implements EventDrivenSource, Configurable {

    MyFlumeSource myFlumeSrc = null;

    public void configure(Context context) {
        myFlumeSrc = new MyFlumeSource(context);
    }

    public void start() {
        final ChannelProcessor channel = getChannelProcessor();

        System.out.println("TEST1234");
        URL url = null;
//        BufferedInputStream in = null;
//        ByteArrayOutputStream baostream = null;
//        byte[] bytesArray = new byte[1024];
//        int bytesRead = 0;
//        try {
//            url = new URL("http://data.dft.gov.uk.s3.amazonaws.com/road-accidents-safety-data/Stats19_Data_2005-2014.zip");
//            in = new BufferedInputStream(url.openStream());
//            baostream = new ByteArrayOutputStream(1024);
//            while ((bytesRead = in.read(bytesArray)) != -1) {
//                baostream.write(bytesArray, 0, bytesRead);
//            }
//            byte[] data = baostream.toByteArray();
//            Event event = EventBuilder.withBody(data);
//            channel.processEvent(event);
//        } catch (MalformedURLException e) {
//            e.printStackTrace();
//        } catch (IOException e) {
//            e.printStackTrace();
//        } finally {
//            if (baostream != null) {
//                try {
//                    baostream.close();
//                } catch (IOException e) {
//                    e.printStackTrace();
//                }
//            }
//            if (in != null) {
//                try {
//                    in.close();
//                } catch (IOException e) {
//                    e.printStackTrace();
//                }
//            }
//         }

//        List<Event> events = new ArrayList<Event>();
//        try {
//            url = new URL("http://data.dft.gov.uk.s3.amazonaws.com/road-accidents-safety-data/Stats19_Data_2005-2014.zip");
//            ReadableByteChannel readableByteChannel = Channels.newChannel(url.openStream());
//            FileOutputStream fileOutputStream = new FileOutputStream("temp.zip");
//            FileChannel fileChannel = fileOutputStream.getChannel();
//            fileChannel.transferFrom(readableByteChannel, 0, Long.MAX_VALUE);
//            fileChannel.close();
//            readableByteChannel.close();
//
//            UnzipUtility unzipUtility = new UnzipUtility();
//            unzipUtility.unzip("temp.zip", "unzip");
//            System.out.println("Working Directory = " + System.getProperty("user.dir"));
//
//
//
////            BufferedInputStream in = new BufferedInputStream(url.openStream());
////            byte[] dataBuffer = new byte[1024000];
////            while (in.read(dataBuffer, 0, 1024000) >= 0) {
////                events.add(EventBuilder.withBody(dataBuffer));
////            }
////            in.close();
////            channel.close();
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//        channel.processEventBatch(events);
        try{
            FileSystem fs = FileSystem.get(new Configuration());
            FileStatus[] status = fs.listStatus(new Path("hdfs://quickstart.cloudera:8020/user/cloudera"));  // you need to pass in your hdfs path

            for (int i=0;i<status.length;i++){
                System.out.println(status[i].getPath());
            }
        }catch(Exception e){
            System.out.println("ERRRRRRRRROR: " + e.getMessage());
        }
        System.out.println("END");
    }

    public void stop() {

        super.stop();
    }

}

;
