package org.flume;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class FileMerger {

    private String downloadDirectoryPath;

    public FileMerger(String downloadDirectoryPath) {
        this.downloadDirectoryPath = downloadDirectoryPath;
    }

    public void mergeAllFiles() {
        try {
            mergeAccidentsFiles();
            mergeCasualtiesFiles();
            mergeVehiclesFiles();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void mergeAccidentsFiles() throws IOException {
        File downloadDirectory = new File(downloadDirectoryPath);
        List<File> accidentsFiles = new ArrayList<>();
        for (File file : downloadDirectory.listFiles()) {
            String fileType = file.getName().split("-")[0];
            if(fileType.equals("accidents")) {
                accidentsFiles.add(file);
            }
        }
        mergeFiles(accidentsFiles, downloadDirectoryPath + File.separator + "accidents-full.csv", FileChecker.getAccidentsColumns());
    }

    private void mergeCasualtiesFiles() throws IOException {
        File downloadDirectory = new File(downloadDirectoryPath);
        List<File> casualtiesFiles = new ArrayList<>();
        for (File file : downloadDirectory.listFiles()) {
            String fileType = file.getName().split("-")[0];
            if(fileType.equals("casualties")) {
                casualtiesFiles.add(file);
            }
        }
        mergeFiles(casualtiesFiles, downloadDirectoryPath + File.separator + "casualties-full.csv", FileChecker.getCasualtiesColumns());
    }

    private void mergeVehiclesFiles() throws IOException {
        File downloadDirectory = new File(downloadDirectoryPath);
        List<File> vehiclesFiles = new ArrayList<>();
        for (File file : downloadDirectory.listFiles()) {
            String fileType = file.getName().split("-")[0];
            if(fileType.equals("vehicles")) {
                vehiclesFiles.add(file);
            }
        }
        mergeFiles(vehiclesFiles, downloadDirectoryPath + File.separator + "vehicles-full.csv", FileChecker.getVehiclesColumns());
    }

    private void mergeFiles(List<File> files, String newFileName, String[] headers) throws IOException {
        FileWriter fileWriter = new FileWriter(newFileName);
        PrintWriter printWriter = new PrintWriter(fileWriter);

        System.out.println("Merging files to " + newFileName + "...");

        String header = String.join(",", headers);
        printWriter.println(header);

        for(File file : files) {
            int rowcount = 0;
            BufferedReader reader = new BufferedReader(new FileReader(file));
            String line;

            while ((line = reader.readLine()) != null) {
                if(rowcount != 0) {
                    printWriter.println(line);
                }
                rowcount++;
            }
            reader.close();
        }
        printWriter.close();
        System.out.println("Merging files to " + newFileName + " finished successfully!");
    }

}
