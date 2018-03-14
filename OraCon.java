import java.io.File;
import java.io.FileWriter; 
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.BufferedOutputStream;
import java.io.ObjectOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.Locale;
import java.util.Date;
import java.util.Map;
import java.util.HashMap;
import java.util.Properties;
import java.io.ByteArrayOutputStream;
//import java.util.ArrayList;
//import java.util.Arrays;
//import java.util.List;


public class OraCon {
    OraCon(String chunk, String Table ){
		
java.util.Properties info = new java.util.Properties();
info.put ("user", "usr");
info.put ("password","psw");
info.put ("defaultRowPrefetch","100000");
info.put ("defaultBatchValue","100");

	  Connection con=null;
	  PreparedStatement  stmt;
      ResultSet   rs;
	  byte[] buf;// = new byte[1384000000];
	  //byte [] bytes= new byte[1384000000];;
	  Long numBytes =0L;
	  int numFetch =0;
	  int col_num =0;
  try {
     Locale.setDefault(Locale.ENGLISH);
	 Class.forName ("oracle.jdbc.driver.OracleDriver");
     con = java.sql.DriverManager.getConnection("jdbc:oracle:thin:@(DESCRIPTION = (ADDRESS = (PROTOCOL = TCP)(HOST = os-2225.homecredit.ru)(PORT = 1521))(CONNECT_DATA = (SERVER = DEDICATED)(SERVICE_NAME = SID)))",
	    info);
		
   System.out.println("Connection with defaultRowPrefetch = 100000  -Ok");			
   System.out.println("Connection with defaultBatchValue = 100 -Ok");
 //  stmt  = con.prepareStatement("SELECT count(*) FROM user_tab_columns where table_name='CRD_LIMIT'");
 //   rs  = null;
  //  rs= stmt.executeQuery();
//	while(rs.next()) {
//		      col_num = rs.getInt(1);
//	}
	long t1 = System.currentTimeMillis();
	switch (chunk) {              
case "1":  stmt  = con.prepareStatement(" select * from CRD_LIMIT where rowid between 'AAADF1AAEAAAA5EAAA' and  'AAADF1AAPAAC6BZABs'");break;
case "2":  stmt  = con.prepareStatement(" select * from CRD_LIMIT where rowid between 'AAADF1AAPAAC6BZABt' and  'AAADF1AAYAAETgPABP'");break;
case "3":  stmt  = con.prepareStatement(" select * from CRD_LIMIT where rowid between 'AAADF1AAYAAETgPABQ' and  'AAADF1AAeAABhgGAAV'");break;
case "4":  stmt  = con.prepareStatement(" select * from CRD_LIMIT where rowid between 'AAADF1AAeAABhgGAAW' and  'AAADF1AAjAABkLOAAe'");break;

case "5":  stmt  = con.prepareStatement(" select * from CRD_LIMIT where rowid between 'AAADF1AAjAABkLOAAf' and  'AAADF1AAkAABjPjABi'");break;
case "6":  stmt  = con.prepareStatement(" select * from CRD_LIMIT where rowid between 'AAADF1AAkAABjPjABj' and  'AAADF1AAlAADX/3AAV'");break;
case "7":  stmt  = con.prepareStatement(" select * from CRD_LIMIT where rowid between 'AAADF1AAlAADX/3AAW' and  'AAADF1AAnAAAIKnABx'");break;
case "8":  stmt  = con.prepareStatement(" select * from CRD_LIMIT where rowid between 'AAADF1AAnAAAIKnABy' and  'AAADF1AArAAATmXACU'");break;

case "9":  stmt  = con.prepareStatement(" select * from CRD_LIMIT where rowid between 'AAADF1AArAAATmXACV' and  'AAADF1AAvAADTYXAAT'");break;
case "10": stmt  = con.prepareStatement(" select * from CRD_LIMIT where rowid between 'AAADF1AAvAADTYXAAU' and  'AAADF1AAyAAA2GUAAB'");break;
case "11": stmt  = con.prepareStatement(" select * from CRD_LIMIT where rowid between 'AAADF1AAyAAA2GUAAC' and  'AAADF1AA7AAADtsAAe'");break;
case "12": stmt  = con.prepareStatement(" select * from CRD_LIMIT where rowid between 'AAADF1AA7AAADtsAAf' and  'AAADF1AA+AAAAeEAAB'");break;

case "13": stmt  = con.prepareStatement(" select * from CRD_LIMIT where rowid between 'AAADF1AA+AAAAeEAAC' and  'AAADF1ABCAADDVxABZ'");break;
case "14": stmt  = con.prepareStatement(" select * from CRD_LIMIT where rowid between 'AAADF1ABCAADDVxABa' and  'AAADF1ABRAAA8J8ABW'");break;
case "15": stmt  = con.prepareStatement(" select * from CRD_LIMIT where rowid between 'AAADF1ABRAAA8J8ABX' and  'AAADF1ABVAACZ0NABG'");break;
case "16": stmt  = con.prepareStatement(" select * from CRD_LIMIT where rowid between 'AAADF1ABVAACZ0NABH' and  'AAADF1ABbAACOb/AB+'");break;
default: stmt  = con.prepareStatement(" select * from CRD_LIMIT ");break;
	}
	
	
	//rs.setFetchSize(100000);
    rs  = stmt.executeQuery();
	FileWriter fos = new FileWriter("log.txt", true) ;
   

	while(rs.next()) {
		//for (int i =1;i<=col_num;i++){
		buf = rs.getBytes(1);	
		if (!(rs.wasNull())){
		numBytes+=Long.valueOf(buf.length);
		}//}		
		numFetch++;		
	 }
	// byte[] bytes = outputStream.toByteArray();	
	 fos.write("Number of columns: " + col_num +";  Fetch number: " + numFetch + ";  Total Mb: " + numBytes/1024/1024+"\r\n");
     rs.close();
	 stmt.close();
	 con.close();
   long t2 = System.currentTimeMillis();
   fos.write("Read Time ,ms: "+ (t2-t1)+"\r\n");
   fos.close();
   }catch (java.sql.SQLException sqle) {
     System.out.println("Check parameters, errors: "+sqle);
   }catch (java.lang.ClassNotFoundException e) {
     System.out.println("Driver not found ojdbc ");
   }catch (FileNotFoundException ex) {
            System.out.println(ex);
   } catch (IOException ex1) {
          System.out.println(ex1);
    }    
 }  
    public static void main(String args[]){
		 new OraCon(args[0],args[1]);
	  }    
}
