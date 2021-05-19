package org.flume;

public class Main {

    public static void main(String[] args) {
        FileDownloader fileDownloader = new FileDownloader();
        FileExtractor fileExtractor = new FileExtractor(fileDownloader.getDownloadDirectoryPath());
        FileNameNormalizer fileNameNormalizer = new FileNameNormalizer(fileDownloader.getDownloadDirectoryPath());
        FileChecker fileChecker = new FileChecker(fileDownloader.getDownloadDirectoryPath());
        FileMerger fileMerger = new FileMerger(fileDownloader.getDownloadDirectoryPath());
        HDFSFilesManager hdfsFilesManager = new HDFSFilesManager(fileDownloader.getDownloadDirectoryPath());
        FileCleaner fileCleaner = new FileCleaner(fileDownloader.getDownloadDirectoryPath());

        fileDownloader.downloadAllFiles();
        fileExtractor.extractAllFiles();
        fileNameNormalizer.normalizeAllFileNames();
        fileChecker.checkFiles();
        fileMerger.mergeAllFiles();
        if (hdfsFilesManager.newDataAvailable()) {
            hdfsFilesManager.saveFiles();
        } else {
            System.out.println("Files have not changed.");
        }
        fileCleaner.deleteAllFiles();
    }

}
