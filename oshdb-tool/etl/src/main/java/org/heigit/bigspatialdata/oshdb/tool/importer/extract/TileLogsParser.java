package org.heigit.bigspatialdata.oshdb.tool.importer.extract;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Date;
import org.heigit.bigspatialdata.oshdb.util.OSHDBTimestamp;
import org.heigit.bigspatialdata.oshdb.util.tilelogs.TileLog;

public class TileLogsParser {
  
  public static void main(String[] args) throws FileNotFoundException, IOException {
    
    try (BufferedReader br = new BufferedReader(new FileReader(
        "tiles-2019-03-31.txt"))) {
      br.lines().forEach((String line) -> {
        String[] split = line.split("/| ");
        
        //check for presence of tile before creating new one
        TileLog tileLog = new TileLog(
            Integer.valueOf(split[0]),
            Integer.valueOf(split[1]),
            Integer.valueOf(split[2]));
        
        tileLog.addLog(new OSHDBTimestamp(new Date()), Integer.valueOf(split[3]));
        
        //store in db
        System.out.println(tileLog);
      });
    }
    
  }
  
}
