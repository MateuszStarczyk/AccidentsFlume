package org.flume;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.Locale;
import java.util.Objects;

public class FileExtractor {

    private UnzipUtility unzipUtility;
    private String downloadDirectoryPath;

    public FileExtractor(String downloadDirectoryPath) {
        this.downloadDirectoryPath = downloadDirectoryPath;
        this.unzipUtility = new UnzipUtility();
    }

    public void extractAllFiles() {
        File downloadDirectory = new File(downloadDirectoryPath);
        for (File file : downloadDirectory.listFiles((dir, name) -> name.toLowerCase().endsWith(".zip"))) {
            unzipIfNecessary(file);
        }
    }

    private void unzipIfNecessary(File file) {
        if(file.getName().contains(".zip")) {

            try {
                System.out.println("Extracting file " + file.getName() + "...");

                unzipUtility.unzip(file.getPath(), downloadDirectoryPath);

                System.out.println("Extracting file " + file.getName() + " finished successfully!");

                file.deleteOnExit();

                System.out.println("File " + file.getName() + " deleted successfully!");

            } catch (IOException e) {
                throw new RuntimeException("Extracting file " + file.getName() + " failed!");
            }
        }
    }

}
