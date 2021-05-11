package org.flume;

import java.io.File;

public class FileNameNormalizer {

    private String downloadDirectoryPath;

    public FileNameNormalizer(String downloadDirectoryPath) {
        this.downloadDirectoryPath = downloadDirectoryPath;
    }

    public void normalizeAllFileNames() {
        File downloadDirectory = new File(downloadDirectoryPath);
        for (File file : downloadDirectory.listFiles(((dir, name) -> name.toLowerCase().endsWith(".csv")))) {
            if (file.exists()) {
                normalizeFileName(file);
            }
        }
    }

    private void normalizeFileName(File file) {
        System.out.println("Normalizing filename " + file.getName() + "...");

        String newFilePath = file.getPath().replace(file.getName(), getNewFileName(file.getName()));
        if (file.renameTo(new File(newFilePath))) {
            System.out.println("Normalizing filename " + file.getName() + " finished successfully!");
        } else {
            System.out.println("Normalizing filename " + file.getName() + " failed!");
        }
    }

    private String getNewFileName(String fileName) {
        for (int year = 2000; year <= 2050; year++) {
            if (fileName.contains(String.valueOf(year))) {
                if (fileName.toUpperCase().contains("VEHICLES")) {
                    return "vehicles-" + year + ".csv";
                } else if (fileName.toUpperCase().contains("ACCIDENTS")) {
                    return "accidents-" + year + ".csv";
                } else if (fileName.toUpperCase().contains("CASUALTIES")) {
                    return "casualties-" + year + ".csv";
                }
            }
        }

        return "";
    }

}
