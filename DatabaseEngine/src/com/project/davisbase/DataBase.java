package com.project.davisbase;

import java.io.File;
import java.io.RandomAccessFile;
import java.util.Scanner;

public class DataBase {
	static String version = "v1.00";
	static String copyright = "@2018 Zhenjie Zhao";
	public static int pageSize = 512;
	static boolean isExit = false;
	static String prompt = "davissql>";
	
	public static Scanner sc = new Scanner(System.in).useDelimiter(";");
	
	public static void main(String[] args) {
		initialCheck();//initialize the catalog
		splashScreen();//basic info
		
		String userCommand = ""; //user input
		

		while(!isExit) {
			System.out.print(prompt);
			userCommand = sc.next().replace("\n", " ").replace("\r", "").trim().toLowerCase();
			parseCommand(userCommand);
		}
		System.out.println("Bye!");

	}
	
	//check if the catalog and files exist, if not create them
	private static void initialCheck(){
		try {
			File dataDir = new File("data\\catalog");
			
			if(dataDir.mkdirs()){
				initialize();
			}else {
				
				String[] oldTableFiles = dataDir.list();
				boolean checkTab = false;
				boolean checkCol = false;
				for (int i=0; i<oldTableFiles.length; i++) {
					if(oldTableFiles[i].equals("davisbase_tables.tbl"))
						checkTab = true;
					if(oldTableFiles[i].equals("davisbase_columns.tbl"))
						checkCol = true;
				}
				
				if(!(checkTab && checkCol)){
					initialize();
				}
			}
		}
		catch (SecurityException e) {
			System.out.println(e);
		}

	}
	
	private static void initialize(){
		//initialize the data catalog
		//delete all files in the catalog
		try {
			File dataDir = new File("data/catalog");
			dataDir.mkdir();
			String[] oldTableFiles;
			oldTableFiles = dataDir.list();
			for (int i=0; i<oldTableFiles.length; i++) {
				File anOldFile = new File(dataDir, oldTableFiles[i]); 
				anOldFile.delete();
			}
		}
		catch (SecurityException e) {
			System.out.println(e);
		}

		try {
			RandomAccessFile tablesCatalog = new RandomAccessFile("data/catalog/davisbase_tables.tbl", "rw");
			//initialize the header
			tablesCatalog.setLength(pageSize);
			tablesCatalog.seek(0);
			tablesCatalog.write(0x0D); //this is a leaf
			tablesCatalog.writeByte(0x02);//there are two tables
			
			int size1 = 27; //size of davisbase_tables.tbl (2 + 4 + 1 + 1 + 1 + 16 + 2)
			int size2 = 28; //size of davisbase_columns.tbl (2 + 4 + 1 + 1 + 1 + 17 + 2)
			
			int offsetT = pageSize - size1;
			int offsetC = offsetT - size2;
			
			tablesCatalog.writeShort(offsetC);//write the start of the content
			tablesCatalog.writeInt(-1);//This is page number of the leaf to the right. The value is -1 (0xFFFFFFFF) if this is the rightmost leaf page.
			tablesCatalog.writeShort(offsetT);
			tablesCatalog.writeShort(offsetC);
			
			//write the record of "davisbase_tables"
			tablesCatalog.seek(offsetT);
			tablesCatalog.writeShort(18);//payload length (1 + 1 + 16)
			tablesCatalog.writeInt(1); //row_id
			tablesCatalog.writeByte(2);//1 column
			tablesCatalog.writeByte(28);//0x0C + 16
			tablesCatalog.writeByte(5);//smallint
			tablesCatalog.writeBytes("davisbase_tables");
			tablesCatalog.writeShort(0);
			
			//write the record of "davisbase_columns"
			tablesCatalog.seek(offsetC);
			tablesCatalog.writeShort(19);
			tablesCatalog.writeInt(2); 
			tablesCatalog.writeByte(2);
			tablesCatalog.writeByte(29);
			tablesCatalog.writeByte(5);
			tablesCatalog.writeBytes("davisbase_columns");
			tablesCatalog.writeShort(0);
			
			//HexDump.displayBinaryHex(tablesCatalog, 512);
			tablesCatalog.close();
		}
		catch (Exception e) {
			System.out.println(e);
		}
		
		try {
			RandomAccessFile columnsCatalog = new RandomAccessFile("data/catalog/davisbase_columns.tbl", "rw");
			columnsCatalog.setLength(pageSize);
			//write the header
			columnsCatalog.seek(0);       
			columnsCatalog.writeByte(0x0D); 
			columnsCatalog.writeByte(0x09);//9 rows 
			
			int[] offset = new int[10];
			offset[0] = pageSize - 39;
			offset[1] = offset[0] - 45;
			offset[2] = offset[1] - 48;
			
			offset[3] = offset[2] - 40;
			offset[4] = offset[3] - 46;
			offset[5] = offset[4] - 47;
			offset[6] = offset[5] - 45;
			offset[7] = offset[6] - 55;
			offset[8] = offset[7] - 47;
			
			columnsCatalog.writeShort(offset[8]); 
			columnsCatalog.writeInt(-1); //right page
			//Array of Record Locations
			for(int i = 0; i < 9;i++){
				columnsCatalog.writeShort(offset[i]);
			}
			//write the records
			columnsCatalog.seek(offset[0]);
			columnsCatalog.writeShort(33);//length of the payload
			columnsCatalog.writeInt(1); //row_id
			columnsCatalog.writeByte(5);//5 columns
			columnsCatalog.writeByte(28);//table_name is TEXT with length 16 ("davisbase_tables")
			columnsCatalog.writeByte(17);//column_name is TEXT with length 5 ("rowid")
			columnsCatalog.writeByte(15);//data_type is TEXT with length 3 ("INT")
			columnsCatalog.writeByte(4);//ordinal_position is TINYINT
			columnsCatalog.writeByte(14);//is_nullable is TEXT with length 2 ("NO")
			columnsCatalog.writeBytes("davisbase_tables"); 
			columnsCatalog.writeBytes("rowid"); 
			columnsCatalog.writeBytes("INT"); 
			columnsCatalog.writeByte(1); 
			columnsCatalog.writeBytes("NO"); 	
			
			columnsCatalog.seek(offset[1]);
			columnsCatalog.writeShort(39); 
			columnsCatalog.writeInt(2); 
			columnsCatalog.writeByte(5);
			columnsCatalog.writeByte(28);
			columnsCatalog.writeByte(22);
			columnsCatalog.writeByte(16);
			columnsCatalog.writeByte(4);
			columnsCatalog.writeByte(14);
			columnsCatalog.writeBytes("davisbase_tables"); 
			columnsCatalog.writeBytes("table_name"); 
			columnsCatalog.writeBytes("TEXT"); 
			columnsCatalog.writeByte(2);
			columnsCatalog.writeBytes("NO"); 
			
			//root page
			columnsCatalog.seek(offset[2]);
			columnsCatalog.writeShort(42);//length of the payload
			columnsCatalog.writeInt(3); //row_id
			columnsCatalog.writeByte(5);//5 columns
			columnsCatalog.writeByte(28);//table_name is TEXT with length 16 ("davisbase_tables")
			columnsCatalog.writeByte(21);//column_name is TEXT with length 5 ("root_page")
			columnsCatalog.writeByte(20);//data_type is TEXT with length 3 ("INT")
			columnsCatalog.writeByte(4);//ordinal_position is TINYINT
			columnsCatalog.writeByte(14);//is_nullable is TEXT with length 2 ("NO")
			columnsCatalog.writeBytes("davisbase_tables"); 
			columnsCatalog.writeBytes("root_page"); 
			columnsCatalog.writeBytes("SMALLINT"); 
			columnsCatalog.writeByte(3); 
			columnsCatalog.writeBytes("NO");
			
			columnsCatalog.seek(offset[3]);
			columnsCatalog.writeShort(34); 
			columnsCatalog.writeInt(4); 
			columnsCatalog.writeByte(5);
			columnsCatalog.writeByte(29);
			columnsCatalog.writeByte(17);
			columnsCatalog.writeByte(15);
			columnsCatalog.writeByte(4);
			columnsCatalog.writeByte(14);
			columnsCatalog.writeBytes("davisbase_columns");
			columnsCatalog.writeBytes("rowid");
			columnsCatalog.writeBytes("INT");
			columnsCatalog.writeByte(1);
			columnsCatalog.writeBytes("NO");
			
			columnsCatalog.seek(offset[4]);
			columnsCatalog.writeShort(40);
			columnsCatalog.writeInt(5); 
			columnsCatalog.writeByte(5);
			columnsCatalog.writeByte(29);
			columnsCatalog.writeByte(22);
			columnsCatalog.writeByte(16);
			columnsCatalog.writeByte(4);
			columnsCatalog.writeByte(14);
			columnsCatalog.writeBytes("davisbase_columns");
			columnsCatalog.writeBytes("table_name");
			columnsCatalog.writeBytes("TEXT");
			columnsCatalog.writeByte(2);
			columnsCatalog.writeBytes("NO");
			
			columnsCatalog.seek(offset[5]);
			columnsCatalog.writeShort(41);
			columnsCatalog.writeInt(6); 
			columnsCatalog.writeByte(5);
			columnsCatalog.writeByte(29);
			columnsCatalog.writeByte(23);
			columnsCatalog.writeByte(16);
			columnsCatalog.writeByte(4);
			columnsCatalog.writeByte(14);
			columnsCatalog.writeBytes("davisbase_columns");
			columnsCatalog.writeBytes("column_name");
			columnsCatalog.writeBytes("TEXT");
			columnsCatalog.writeByte(3);
			columnsCatalog.writeBytes("NO");
			
			columnsCatalog.seek(offset[6]);
			columnsCatalog.writeShort(39);
			columnsCatalog.writeInt(7); 
			columnsCatalog.writeByte(5);
			columnsCatalog.writeByte(29);
			columnsCatalog.writeByte(21);
			columnsCatalog.writeByte(16);
			columnsCatalog.writeByte(4);
			columnsCatalog.writeByte(14);
			columnsCatalog.writeBytes("davisbase_columns");
			columnsCatalog.writeBytes("data_type");
			columnsCatalog.writeBytes("TEXT");
			columnsCatalog.writeByte(4);
			columnsCatalog.writeBytes("NO");
			
			columnsCatalog.seek(offset[7]);
			columnsCatalog.writeShort(49); 
			columnsCatalog.writeInt(8); 
			columnsCatalog.writeByte(5);
			columnsCatalog.writeByte(29);
			columnsCatalog.writeByte(28);
			columnsCatalog.writeByte(19);
			columnsCatalog.writeByte(4);
			columnsCatalog.writeByte(14);
			columnsCatalog.writeBytes("davisbase_columns");
			columnsCatalog.writeBytes("ordinal_position");
			columnsCatalog.writeBytes("TINYINT");
			columnsCatalog.writeByte(5);
			columnsCatalog.writeBytes("NO");
			
			columnsCatalog.seek(offset[8]);
			columnsCatalog.writeShort(41); 
			columnsCatalog.writeInt(9); 
			columnsCatalog.writeByte(5);
			columnsCatalog.writeByte(29);
			columnsCatalog.writeByte(23);
			columnsCatalog.writeByte(16);
			columnsCatalog.writeByte(4);
			columnsCatalog.writeByte(14);
			columnsCatalog.writeBytes("davisbase_columns");
			columnsCatalog.writeBytes("is_nullable");
			columnsCatalog.writeBytes("TEXT");
			columnsCatalog.writeByte(6);
			columnsCatalog.writeBytes("NO");
			
			//HexDump.displayBinaryHex(columnsCatalog, 512);
			columnsCatalog.close();
		}
		catch (Exception e) {
			System.out.println(e);
		}
	}
	private static void splashScreen(){
		
	}
	public static String splashLine(String s, int num){
		String line = "";
		for(int i = 0; i < num; i++){
			line += s;
		}
		return line;
	}
	
	private static void help(){
		System.out.println(splashLine("*",100));
		System.out.println("SUPPORTED COMMANDS");
		System.out.println();
		System.out.println("\tSHOW TABLES;                                               Display all the tables in the database.");
		System.out.println("\tCREATE TABLE table_name (<column_name datatype>);          Create a new table in the database.");
		System.out.println("\tINSERT INTO table_name (column_list) VALUES (value1,value2,..);          Insert a new record into the table.");
		System.out.println("\tDELETE FROM TABLE table_name WHERE condition;     Delete a record from the table with the condition.");
		System.out.println("\tUPDATE table_name SET column_name = value WHERE condition; Modifies the records in the table.");
		System.out.println("\tSELECT * FROM table_name;                                  Display all records in the table.");
		System.out.println("\tSELECT * FROM table_name WHERE column_name operator value; Display records in the table where the given condition is satisfied.");
		System.out.println("\tDROP TABLE table_name;                                     Remove table data and its schema.");
		System.out.println("\tHELP;                                                      Show this help information.");
		System.out.println("\tEXIT;                                                      Exit the program.");
		System.out.println();
		System.out.println();
		System.out.println(splashLine("*",100));
	}
	
	private static void parseCommand(String command){
		String first = command.split(" ")[0];
		switch(first){
		case "show":
		    showTables();
		    break;
		
	    case "create":
	    	createTabel(command);
		    break;

		case "insert":
			insertRow(command);
			break;
			
		case "delete":
			deleteRow(command);
			break;	

		case "update":
			updateRow(command);
			break;
			
		case "select":
			parseQuery(command);
			break;

		case "drop":
			dropTable(command);
			break;	

		case "help":
			help();
			break;

		case "version":
			System.out.println("DavisBase Version " + version);
			System.out.println(copyright);
			break;

		case "exit":
			isExit = true;
			break;

		default:
			System.out.println("Syntax error!");
			System.out.println();
			break;
		}
	}
	
	private static void showTables(){
		//show tables in the database
		String table = "davisbase_tables";
		String[] cols = {"rowid", "table_name"};
		String[] cmptr = new String[0];
		ExecuteCommand.select(table, cols, cmptr);
	}
	
	private static void createTabel(String command){
		//create a table
		String tableName = command.split(" ")[2];
		
		String cols = command.split(tableName, 2)[1].trim();
		String[] create_cols = cols.substring(1, cols.length()-1).split(",");
		
		for(int i = 0; i < create_cols.length; i++){
			create_cols[i] = create_cols[i].trim();
		}
		
		if(tableExists(tableName)){
			System.out.println("Table "+tableName+" already exists.");
		}else{
			ExecuteCommand.createTable(tableName, create_cols);
		}
	}
	
	private static void insertRow(String command){
		//insert a row
		String[] tokens=command.split(" ");
		String tableName = tokens[2];
		String[] temp = command.split("values");
		String colums = temp[0].trim();
		String valuses = temp[1].trim();
		int colListStart = colums.indexOf("(");
		//create column list
		String[] column_list = null;
		if(colListStart != -1){
			column_list = colums.substring(colums.indexOf("(") + 1, colums.length()-1).split(",");
			for(int i = 0; i < column_list.length; i++)
				column_list[i] = column_list[i].trim();
		}
		
		
		String[] insert_vals = valuses.substring(1, valuses.length()-1).split(",");
		for(int i = 0; i < insert_vals.length; i++)
			insert_vals[i] = insert_vals[i].trim();
		if(!tableExists(tableName)){
			System.out.println("Table "+tableName+" does not exist.");
		}else{
			ExecuteCommand.insertIntoNoRowid(tableName, column_list, insert_vals);
		}
	}
	
	private static void deleteRow(String command){
		//delete a row
		String[] tokens=command.split(" ");
		String tableName = tokens[3];
		String[] temp = command.split("where");
		String conditionTemp = temp[1];
		String[] condition = parserCondition(conditionTemp);
		if(!tableExists(tableName)){
			System.out.println("Table "+tableName+" does not exist.");
		}else{
			//ExecuteCommand.deleteWithRowid(tableName, condition);
			ExecuteCommand.deleteRow(tableName, condition);
		}
	}
	
	private static void updateRow(String command){
		//update a row
		String[] tokens=command.split(" ");
		String tableName = tokens[1];
		String[] temp1 = command.split("set");
		String[] temp2 = temp1[1].split("where");
		String cmpTemp = temp2[1];
		String setTemp = temp2[0];
		String[] cmp = parserCondition(cmpTemp);
		String[] set = parserCondition(setTemp);
		if(!tableExists(tableName)){
			System.out.println("Table "+ tableName +" does not exist.");
		}else{
			ExecuteCommand.update(tableName, cmp, set);
		}
	}

	
	private static void dropTable(String command){
		//drop a table
		String[] tokens=command.split(" ");
		String tableName = tokens[2];
		if(!tableExists(tableName)){
			System.out.println("Table "+tableName+" does not exist.");
		}else{
			ExecuteCommand.drop(tableName);
		}
	}
	
	private static void parseQuery(String command){
		//parse a query
		String[] cmp;
		String[] column;
		String[] temp = command.split("where");
		if(temp.length > 1){
			cmp = parserCondition(temp[1].trim());
		}
		else{
			cmp = new String[0];
		}
		String[] select = temp[0].split("from");
		String tableName = select[1].trim();
		String cols = select[0].replace("select", "").trim();
		if(cols.contains("*")){
			column = new String[1];
			column[0] = "*";
		}else{
			column = cols.split(",");
			for(int i = 0; i < column.length; i++)
				column[i] = column[i].trim();
		}
		
		if(!tableExists(tableName)){
			System.out.println("Table "+tableName+" does not exist.");
		}else{
			ExecuteCommand.select(tableName, column, cmp);
		}
	}
	
	
	public static boolean tableExists(String tableName){
		tableName = tableName+".tbl";
		
		try {
			File dataDir;
			if(tableName.equals("davisbase_tables.tbl") || tableName.equals("davisbase_columns.tbl")){
				dataDir = new File("data/catalog");
			}else{
				dataDir = new File("data/user_data");
			}
			dataDir.mkdirs();
			String[] oldTableFiles;
			oldTableFiles = dataDir.list();
			for (int i = 0; i < oldTableFiles.length; i++) {
				if(oldTableFiles[i].equals(tableName))
					return true;
			}
		}
		catch (SecurityException se) {
			System.out.println("Unable to create data container directory");
			System.out.println(se);
		}

		return false;
	}
	
	public static String[] parserCondition(String equ){
		String comparator[] = new String[3];
		String temp[] = new String[2];
		if(equ.contains("<=")) {
			temp = equ.split("<=");
			comparator[0] = temp[0].trim();
			comparator[1] = "<=";
			comparator[2] = temp[1].trim();
		}else if(equ.contains(">=")) {
			temp = equ.split(">=");
			comparator[0] = temp[0].trim();
			comparator[1] = ">=";
			comparator[2] = temp[1].trim();
		}else if(equ.contains("=")) {
			temp = equ.split("=");
			comparator[0] = temp[0].trim();
			comparator[1] = "=";
			comparator[2] = temp[1].trim();
		}else if(equ.contains("<")) {
			temp = equ.split("<");
			comparator[0] = temp[0].trim();
			comparator[1] = "<";
			comparator[2] = temp[1].trim();
		}else if(equ.contains(">")) {
			temp = equ.split(">");
			comparator[0] = temp[0].trim();
			comparator[1] = ">";
			comparator[2] = temp[1].trim();
		}else{
			System.out.println("Wrong condition!");
		}

		return comparator;
	}

}
