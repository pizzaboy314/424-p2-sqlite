package sql;

import java.io.BufferedReader;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

public class CreateTable {
	public static void main(String args[]) {
//		createTable();
		insertFromCSV("Divvy_Stations_2013.csv");
	}
	
	public static void insertFromCSV(String filename){
		
		String line = "";
		BufferedReader br = null;
		Connection c = null;
		Statement stmt = null;
		
		try {
			Class.forName("org.sqlite.JDBC");
			c = DriverManager.getConnection("jdbc:sqlite:stations.db");
			c.setAutoCommit(false);
			System.out.println("Opened database successfully");
			
			int i=0;
			br = new BufferedReader(new FileReader(filename));
			line = br.readLine();
			while ((line = br.readLine()) != null) {
				String[] station = line.split(",");
				station[0] = station[0].replaceAll("&", "and"); 
				
				stmt = c.createStatement();
				String sql = "INSERT INTO STATIONS (ID,NAME,LATITUDE,LONGITUDE,DPCAPACITY) "
						+ "VALUES (" + i + "," + "\'" + station[0] +"',"+ station[1] +","+ station[2] +","+ station[3] + ");";
				stmt.executeUpdate(sql);
				i++;
			}

			stmt.close();
			c.commit();
			c.close();
		} catch (Exception e) {
			System.err.println(e.getClass().getName() + ": " + e.getMessage());
			System.exit(0);
		}
		System.out.println("Records created successfully");
	}
	
	public static void createTable(){
		Connection c = null;
		Statement stmt = null;
		try {
			Class.forName("org.sqlite.JDBC");
			c = DriverManager.getConnection("jdbc:sqlite:stations.db");
			System.out.println("Opened database successfully");

			stmt = c.createStatement();
			String sql = "CREATE TABLE STATIONS "
					+ "(ID INT PRIMARY KEY     NOT NULL,"
					+ " NAME           STRING    NOT NULL, "
					+ " LATITUDE       FLOAT     NOT NULL, "
					+ " LONGITUDE      FLOAT     NOT NULL, "
					+ " DPCAPACITY     INT       NOT NULL)";
			stmt.executeUpdate(sql);
			stmt.close();
			c.close();
		} catch (Exception e) {
			System.err.println(e.getClass().getName() + ": " + e.getMessage());
			System.exit(0);
		}
		System.out.println("Table created successfully");
	}
}
