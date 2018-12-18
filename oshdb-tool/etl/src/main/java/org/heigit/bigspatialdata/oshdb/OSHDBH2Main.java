package org.heigit.bigspatialdata.oshdb;


import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class OSHDBH2Main {

	public static void main(String[] args) throws ClassNotFoundException {

		Path h2 = Paths.get("/mnt/sds/sd17f001/oshdb/downloads/v0.4/global.oshdb");

		Class.forName("org.h2.Driver");
		try (Connection conn = DriverManager.getConnection("jdbc:h2:" + h2.toString() + ";ACCESS_MODE_DATA=r", "sa", "");
			 Statement stmt = conn.createStatement()){
			
			ResultSet rst= stmt.executeQuery("select count(*) from grid_nodes");
			while(rst.next()){
				System.out.println(rst.getLong(1));
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

}
