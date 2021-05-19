package org.flume;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URL;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;

public class FileDownloader {

    private static final String DOWNLOAD_DIRECTORY = "download" + File.separator;

    private static final String[] DATA = {
            "http://data.dft.gov.uk/road-accidents-safety-data/RoadSafetyData_Vehicles_2015.zip",
            "http://data.dft.gov.uk/road-accidents-safety-data/RoadSafetyData_Accidents_2015.zip",
            "http://data.dft.gov.uk/road-accidents-safety-data/RoadSafetyData_Casualties_2015.zip",
            "http://data.dft.gov.uk/road-accidents-safety-data/dftRoadSafetyData_Casualties_2016.zip",
            "http://data.dft.gov.uk/road-accidents-safety-data/dftRoadSafetyData_Vehicles_2016.zip",
            "http://data.dft.gov.uk/road-accidents-safety-data/dftRoadSafety_Accidents_2016.zip",
            "http://data.dft.gov.uk.s3.amazonaws.com/road-accidents-safety-data/dftRoadSafetyData_Vehicles_2017.zip",
            "http://data.dft.gov.uk.s3.amazonaws.com/road-accidents-safety-data/dftRoadSafetyData_Casualties_2017.zip",
            "http://data.dft.gov.uk.s3.amazonaws.com/road-accidents-safety-data/dftRoadSafetyData_Accidents_2017.zip",
            "http://data.dft.gov.uk/road-accidents-safety-data/dftRoadSafetyData_Accidents_2018.csv",
            "http://data.dft.gov.uk/road-accidents-safety-data/dftRoadSafetyData_Casualties_2018.csv",
            "http://data.dft.gov.uk/road-accidents-safety-data/dftRoadSafetyData_Vehicles_2018.csv",
            "http://data.dft.gov.uk/road-accidents-safety-data/DfTRoadSafety_Accidents_2019.zip",
            "http://data.dft.gov.uk/road-accidents-safety-data/DfTRoadSafety_Vehicles_2019.zip",
            "http://data.dft.gov.uk/road-accidents-safety-data/DfTRoadSafety_Casualties_2019.zip"
    };

    public void downloadAllFiles() {
        createDownloadDir();
        for (String link : DATA) {
            downloadFile(link);
        }
    }

    private void downloadFile(String link) {
        String filename = getFileName(link);

        try {
            System.out.println("Downloading file " + filename + "...");

            URL url = new URL(link);
            ReadableByteChannel readableByteChannel = Channels.newChannel(url.openStream());
            FileOutputStream fileOutputStream = new FileOutputStream(DOWNLOAD_DIRECTORY + filename);
            FileChannel fileChannel = fileOutputStream.getChannel();
            fileChannel.transferFrom(readableByteChannel, 0, Long.MAX_VALUE);
            fileChannel.close();
            readableByteChannel.close();

            System.out.println("Downloading file " + filename + " finished successfully!");

        } catch (IOException e) {
            System.out.println(e.getMessage());
            throw new RuntimeException("Downloading file " + filename + " failed!");
        }
    }

    public String getDownloadDirectoryPath() {
        return DOWNLOAD_DIRECTORY;
    }

    private String getFileName(String link) {
        String[] urlParts = link.split("/");
        return urlParts[urlParts.length - 1];
    }

    private void createDownloadDir() {
        File file = new File("download");
        if (!file.exists())
            if (!file.mkdir()) {
                throw new RuntimeException("Couldn't create download directory");
            }
    }

    public static void main(String[] args) {
        FileDownloader fileDownloader = new FileDownloader();
        fileDownloader.downloadAllFiles();
    }

}
