package com.stackroute.datamunger.reader;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import com.stackroute.datamunger.query.DataTypeDefinitions;
import com.stackroute.datamunger.query.Header;

public class CsvQueryProcessor extends QueryProcessingEngine {

	// Parameterized constructor to initialize filename
	String fileName;
	public CsvQueryProcessor(String fileName) throws FileNotFoundException {
		this.fileName = fileName;
		File file = new File(fileName);
		if(!file.exists()) {
			throw new FileNotFoundException();
		}
	}

	/*
	 * Implementation of getHeader() method. We will have to extract the headers
	 * from the first line of the file.
	 * Note: Return type of the method will be Header
	 */
	
	@Override
	public Header getHeader() throws IOException {
		BufferedReader br = new BufferedReader( new FileReader(fileName));
	    String strLine = "";
		// read the first line
	    strLine = br.readLine();
	  //  System.out.println(strLine);
		// populate the header object with the String array containing the header names
	    String[] strArray = strLine.split(",");
	    Header head = new Header();
	    head.setHeaders(strArray);
		return head;
	}

	/**
	 * getDataRow() method will be used in the upcoming assignments
	 */
	
	@Override
	public void getDataRow() {

	}

	/*
	 * Implementation of getColumnType() method. To find out the data types, we will
	 * read the first line from the file and extract the field values from it. If a
	 * specific field value can be converted to Integer, the data type of that field
	 * will contain "java.lang.Integer", otherwise if it can be converted to Double,
	 * then the data type of that field will contain "java.lang.Double", otherwise,
	 * the field is to be treated as String. 
	 * Note: Return Type of the method will be DataTypeDefinitions
	 */
	
	@Override
	public DataTypeDefinitions getColumnType() throws IOException {

		  BufferedReader br = new BufferedReader(new FileReader(fileName));
          String header = br.readLine();
          String firstLine = br.readLine();
          String[] dataa = firstLine.split(",");
         
          String[] datas=new String[dataa.length+1];
     
          String intg = "java.lang.Integer";
          String str = "java.lang.String";
          int i=0;
          for(String s:dataa) {
             
              String regex = "[+-]?[0-9][0-9]*";
              if(s.matches(regex)) {
                  datas[i]=intg;
              }
              else {
                  datas[i]=str;
              }
              i+=1;
          }
          if(datas.length>17) {
              datas[17]= str;
          }
         
          DataTypeDefinitions datatypes = new DataTypeDefinitions();
          datatypes.setDataTypes(datas);
          br.close();
          return datatypes;
   }
}
	  
		


