package org.flume;

import java.io.File;

public class FileCleaner {

    private String downloadDirectoryPath;

    public FileCleaner(String downloadDirectoryPath) {
        this.downloadDirectoryPath = downloadDirectoryPath;
    }

    public void deleteAllFiles() {
        File downloadDirectory = new File(downloadDirectoryPath);
        for (File file : downloadDirectory.listFiles(((dir, name) -> name.toLowerCase().endsWith(".csv")))) {
            if (file.exists()) {
                file.deleteOnExit();
            }
        }
    }

}
