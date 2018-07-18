import java.io.*;
import java.sql.*;
import java.util.*;
import java.text.SimpleDateFormat;
import java.util.logging.*;

public class ms3Project {
	
	static int  NumberOfRecordsReceived =-1, NumberOfRecordsSuccessful=0, NumberOfRecordsFailed=0;
	@SuppressWarnings("resource")
	public static void main(String[] args) {
		FileWriter exceptionWriter=null;
		try{
		 

		String csvFile = "../MS3Task/ms3Interview.csv";
		BufferedReader br = null;
        String line = "";
        String cvsSplitBy = ",(?=([^\"]*\"[^\"]*\")*[^\"]*$)";
		FileWriter fileWriter=null;
		 
		
		
		Logger logger = Logger.getLogger("ms3Project");  
		FileHandler fh;
		 
		fh = new FileHandler("../MS3Task/ms3Project.log");  
        logger.addHandler(fh);
		SimpleFormatter formatter = new SimpleFormatter();  
        fh.setFormatter(formatter);  
        exceptionWriter=new FileWriter("../MS3Task/Exception.txt");
			
			br = new BufferedReader(new FileReader(csvFile));
			String timeStamp = new SimpleDateFormat("yyyy.MM.dd.HH.mm.ss").format(new java.util.Date());
			fileWriter = new FileWriter("BadData"+timeStamp+".csv");
			
			Class.forName("org.sqlite.JDBC");
			Connection connection = DriverManager.getConnection("jdbc:sqlite:C://sqlite/msdatabase.db");
			Statement stmt = connection.createStatement();
			
			
			
			
			String sqlTable="create table if not exists ms3datatable( A Varchar Not Null,  B Varchar Not Null,  C Varchar Not Null,  D Varchar Not Null,  E Varchar Not Null,  F Varchar Not Null,  G Varchar Not Null,  H Varchar Not Null,  I Varchar Not Null,  J Varchar Not Null);";
			stmt.execute(sqlTable);
			
            while ((line = br.readLine()) != null) 
			{ 
		
				if( NumberOfRecordsReceived == -1)
				{
					NumberOfRecordsReceived++;
					continue;
				}
				NumberOfRecordsReceived++;
		
				boolean flag=false; 
				
				
        
            String[] coloumn = line.split(cvsSplitBy);
			for(int i=0; i<coloumn.length; i++){ 
			
				if(coloumn[i] == null && coloumn[i].isEmpty()){
					flag=true;
					
				}
			}
			if( flag== false && coloumn.length == 10)
			{
  	
							
				String query="INSERT INTO ms3datatable (A, B, C, D, E,F, G,H,I,J) VALUES( ' "+coloumn[0]+" ',' "+coloumn[1]+" ',' "+coloumn[2]+" ',' "+coloumn[3]+" ',' "+coloumn[4]+" ',' "+coloumn[5]+" ',' "+coloumn[6]+" ',' "+coloumn[7]+" ',' "+coloumn[8]+" ',' "+coloumn[9]+" ')";
				stmt.addBatch(query);
				NumberOfRecordsSuccessful++;
			
			}
			else
			{
				fileWriter.append(line);
				fileWriter.append('\n');
				NumberOfRecordsFailed++;
				
			}
			try {
			if(NumberOfRecordsSuccessful % 100 == 0)
			stmt.executeBatch(); 
			}
			catch(Exception e)
			{
				exceptionWriter.append("\n \n Exception occured line @"+(NumberOfRecordsReceived-100)+"    to  "+(NumberOfRecordsReceived-100));
			}
			
		}
		
		
		
		logger.info("# of records received = "+NumberOfRecordsReceived); 
		logger.info("# of records successful = "+NumberOfRecordsSuccessful); 
		logger.info("# of records failed = "+NumberOfRecordsFailed);
		
		stmt.executeBatch(); 
		//connection.commit();
		fileWriter.close();
		connection.close();
		}
		catch(Exception e)
		{
	
		e.printStackTrace();
			
		}
		//System.out.println("Total Records"+NumberOfRecordsReceived+"Sucess"+NumberOfRecordsSuccessful+"failed="+NumberOfRecordsFailed);
		
	
	}
}