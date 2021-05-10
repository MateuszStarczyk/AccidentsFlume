package org.flume;

import com.opencsv.CSVReader;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;

public class FileChecker {

    private static final int BOM_CODEPOINT = 65279;

    private static final String[] ACCIDENTS_COLUMNS = {"Accident_Index", "Location_Easting_OSGR", "Location_Northing_OSGR",
        "Longitude", "Latitude", "Police_Force", "Accident_Severity", "Number_of_Vehicles", "Number_of_Casualties",
        "Date", "Day_of_Week", "Time", "Local_Authority_(District)", "Local_Authority_(Highway)", "1st_Road_Class",
        "1st_Road_Number", "Road_Type", "Speed_limit", "Junction_Detail", "Junction_Control", "2nd_Road_Class",
        "2nd_Road_Number", "Pedestrian_Crossing-Human_Control", "Pedestrian_Crossing-Physical_Facilities",
        "Light_Conditions", "Weather_Conditions", "Road_Surface_Conditions", "Special_Conditions_at_Site",
        "Carriageway_Hazards", "Urban_or_Rural_Area", "Did_Police_Officer_Attend_Scene_of_Accident", "LSOA_of_Accident_Location"};

    private static final String[] CASUALTIES_COLUMNS = {"Accident_Index", "Vehicle_Reference", "Casualty_Reference",
        "Casualty_Class", "Sex_of_Casualty", "Age_of_Casualty", "Age_Band_of_Casualty", "Casualty_Severity",
            "Pedestrian_Location", "Pedestrian_Movement", "Car_Passenger", "Bus_or_Coach_Passenger",
            "Pedestrian_Road_Maintenance_Worker", "Casualty_Type", "Casualty_Home_Area_Type", "Casualty_IMD_Decile"};

    private static final String[] VEHICLES_COLUMNS = {"Accident_Index", "Vehicle_Reference", "Vehicle_Type",
        "Towing_and_Articulation", "Vehicle_Manoeuvre", "Vehicle_Location-Restricted_Lane",
        "Junction_Location", "Skidding_and_Overturning", "Hit_Object_in_Carriageway", "Vehicle_Leaving_Carriageway",
        "Hit_Object_off_Carriageway", "1st_Point_of_Impact", "Was_Vehicle_Left_Hand_Drive?",
        "Journey_Purpose_of_Driver", "Sex_of_Driver", "Age_of_Driver", "Age_Band_of_Driver", "Engine_Capacity_(CC)",
        "Propulsion_Code", "Age_of_Vehicle", "Driver_IMD_Decile", "Driver_Home_Area_Type", "Vehicle_IMD_Decile"};

    private String downloadDirectoryPath;

    public FileChecker(String downloadDirectoryPath) {
        this.downloadDirectoryPath = downloadDirectoryPath;
    }

    public void checkFiles() {
        File downloadDirectory = new File(downloadDirectoryPath);
        for (File file : downloadDirectory.listFiles()) {
            String fileType = file.getName().split("-")[0];
            try {
                switch (fileType) {
                    case "vehicles":
                        checkFile(file.getName(), file.getPath(), VEHICLES_COLUMNS);
                        break;
                    case "accidents":
                        checkFile(file.getName(), file.getPath(), ACCIDENTS_COLUMNS);
                        break;
                    case "casualties":
                        checkFile(file.getName(), file.getPath(), CASUALTIES_COLUMNS);
                        break;
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static String[] getAccidentsColumns() {
        return ACCIDENTS_COLUMNS;
    }

    public static String[] getCasualtiesColumns() {
        return CASUALTIES_COLUMNS;
    }

    public static String[] getVehiclesColumns() {
        return VEHICLES_COLUMNS;
    }

    private void checkFile(String fileName, String path, String[] headers) throws IOException {
        int rowcount = 0;
        FileReader filereader = new FileReader(path);
        CSVReader csvReader = new CSVReader(filereader);
        String[] nextRecord;

        while ((nextRecord = csvReader.readNext()) != null) {
            if(nextRecord.length == headers.length) {
                if(rowcount == 0) {
                    for(int i = 0; i < nextRecord.length; i++) {
                        String cellWithoutBom = nextRecord[i].codePointAt(0) == BOM_CODEPOINT ? nextRecord[i].substring(1) : nextRecord[i];
                        if(!cellWithoutBom.equals(headers[i])) {
                            throw new RuntimeException("Checking file " + fileName + " failed - wrong headers!");
                        }
                    }
                }
            } else {
                throw new RuntimeException("Checking file " + fileName + " failed - wrong number of columns in " + rowcount + " row!");
            }
            rowcount++;
        }
        System.out.println("Checking file " + fileName + " finished successfully!");
    }

}
