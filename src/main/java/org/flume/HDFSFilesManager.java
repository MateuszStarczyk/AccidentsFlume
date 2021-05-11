package org.flume;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.util.Arrays;
import java.util.Optional;

public class HDFSFilesManager {

    private static final String HDFS_LOCAL_DIRECTORY = "hdfs" + File.separator;
    private static final String HDFS_URL = "hdfs://quickstart.cloudera:8020/user/cloudera/data";

    private final String downloadDirectoryPath;
    private FileSystem fs;

    public HDFSFilesManager(String downloadDirectoryPath) {
        this.downloadDirectoryPath = downloadDirectoryPath;
        try {
            Configuration conf = new Configuration();
            conf.addResource(new Path("/etc/hadoop/conf/core-site.xml"));
            conf.addResource(new Path("/etc/hadoop/conf/hdfs-site.xml"));
            fs = FileSystem.get(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void getFiles() {
        try {
            FileStatus[] status = fs.listStatus(new Path(HDFS_URL));

            for (FileStatus fileStatus : status) {
                System.out.println("Getting file " + fileStatus.getPath() + " from HDFS...");

                FSDataInputStream inputStream = fs.open(fileStatus.getPath());
                ReadableByteChannel readableByteChannel = Channels.newChannel(inputStream);
                FileOutputStream fileOutputStream = new FileOutputStream(HDFS_LOCAL_DIRECTORY + fileStatus.getPath().getName());
                FileChannel fileChannel = fileOutputStream.getChannel();
                fileChannel.transferFrom(readableByteChannel, 0, Long.MAX_VALUE);
                fileChannel.close();
                readableByteChannel.close();

                System.out.println("Getting file " + fileStatus.getPath() + " finished successfully!");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void saveFiles() {
        try {
            File downloadDirectory = new File(downloadDirectoryPath);
            File[] files = downloadDirectory.listFiles((dir, name) -> name.contains("-full.csv"));
            if (files != null) {
                for (File file : files) {
                    System.out.println("Saving file " + file.getName() + " to HDFS...");

                    FSDataOutputStream outputStream = fs.create(new Path(HDFS_LOCAL_DIRECTORY + file.getName()), true);
                    outputStream.write(FileUtils.readFileToByteArray(file));

                    System.out.println("Saving file " + file.getName() + " to HDFS finished successfully!");
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public boolean newDataAvailable() {
        boolean newDataAvailable = false;
        File downloadDirectory = new File(downloadDirectoryPath);
        File hdfsDirectory = new File(HDFS_LOCAL_DIRECTORY);
        File[] downloadedFiles = downloadDirectory.listFiles();
        File[] hdfsFiles = hdfsDirectory.listFiles();
        if (downloadedFiles != null && hdfsFiles != null) {
            for (File downloadedFile : downloadedFiles) {
                if (!newDataAvailable && downloadedFile.getName().contains("-full.csv")) {
                    Optional<File> hdfsFile = Arrays.stream(hdfsFiles).findFirst().filter(f -> f.getName().equals(downloadedFile.getName()));
                    if (hdfsFile.isPresent() && hdfsFile.get().length() > downloadedFile.length()) {
                        newDataAvailable = true;
                    }
                }
            }
            return newDataAvailable;
        } else return hdfsFiles == null;
    }
}