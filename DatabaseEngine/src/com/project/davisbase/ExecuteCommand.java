package com.project.davisbase;

import java.io.File;
import java.io.RandomAccessFile;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;

public class ExecuteCommand {
	public static int pageSize = 512;
	public static String datePattern = "yyyy-MM-dd_HH:mm:ss";
	
	public static int pages(RandomAccessFile file){
		int num_pages = 0;
		try{
			num_pages = (int)(file.length()/(new Long(pageSize)));
		}catch(Exception e){
			System.out.println(e);
		}

		return num_pages;
	}
	
	private static int getMaxKey(String tableURL){
		int maxKey = 0;
		try{
			RandomAccessFile file = new RandomAccessFile(tableURL, "rw");
			
			int numOfPages = pages(file);
			int page = 1;
			
			//find the rightmost leaf
			for(int pageIndex = 1; pageIndex <= numOfPages; pageIndex++){
				int rm = Page.getRightMost(file, pageIndex);
				if(rm == -1){
					page = pageIndex;
				}
			}
			//find the maximum key (rowid)
			int[] keys = Page.getKeyArray(file, page);
			if(keys.length == 0){
				return 0;
			}
			maxKey = keys[0];
			for(int i = 0; i < keys.length; i++){
				if(keys[i] > maxKey){
					maxKey = keys[i];
				}
			}
			file.close();
		}catch(Exception e){
			System.out.println(e);
		}
		
		return maxKey;
		
	}
	
	public static void changeValueAtFixPosition(RandomAccessFile file, long loc, String dataType, String data){
		try{
			//file.seek(loc);
			String s;
			switch(dataType.toLowerCase()){
				case "tinyint":
					file.writeByte(new Byte(data));
					break;
				case "smallint":
					file.writeShort(new Short(data));
					break;
				case "int":
					file.writeInt(new Integer(data));
					break;
				case "bigint":
					file.writeLong(new Long(data));
					break;
				case "real":
					file.writeFloat(new Float(data));
					break;
				case "double":
					file.writeDouble(new Double(data));
					break;
				case "datetime":
					s = data;
					Date temp = new SimpleDateFormat(datePattern).parse(s.substring(1, s.length()-1));
					long time = temp.getTime();
					file.writeLong(time);
					break;
				case "date":
					s = data;
					s = s.substring(1, s.length()-1);
					s = s+"_00:00:00";
					Date temp2 = new SimpleDateFormat(datePattern).parse(s);
					long time2 = temp2.getTime();
					file.writeLong(time2);
					break;
				default:
					System.out.println("Data type error!");;
					break;
			}
			
		}catch(Exception e){
			System.out.println(e);
		}
		
	}
	
	public static long findChangePointer(RandomAccessFile file, long loc, int setOrd){
		try{
			file.seek(loc + 6);
			
			int numOfCols = file.readByte();//number of columns
			
			byte[] stc = new byte[numOfCols];
			file.read(stc);//get the array of data types
			
			for(int i = 1; i < setOrd - 1; i++){
				switch(stc[i-1]){
					case 0x00:  file.readByte();
								break;

					case 0x01:  file.readShort();
								break;

					case 0x02:  file.readInt();
								break;

					case 0x03:  file.readLong();
								break;

					case 0x04:  file.readByte();
								break;

					case 0x05:  file.readShort();
								break;

					case 0x06:  file.readInt();
								break;

					case 0x07:  file.readLong();
								break;

					case 0x08:  file.readFloat();
								break;

					case 0x09:  file.readDouble();
								break;

					case 0x0A:  file.readLong();
								break;

					case 0x0B:  file.readLong();
								break;

					default:    int len = new Integer(stc[i-1] - 0x0C);
								byte[] bytes = new byte[len];
								file.read(bytes);
								break;
				}
			}
			return file.getFilePointer();
		}catch(Exception e){
			System.out.println(e);
		}
		return -1;
	}
	
	//cmpOrd ordinal position of condition
	//setOrd ordinal position of set
	public static void updateAtSamePos(String tableName, String[] cmp, String[] set, int cmpOrd, int setOrd, String dataType){
		try{
			RandomAccessFile file;
			if(tableName.equals("davisbase_tables") || tableName.equals("davisbase_columns")){
				file = new RandomAccessFile("data/catalog/" + tableName + ".tbl", "rw");
			}else{
				file = new RandomAccessFile("data/user_data/" + tableName + ".tbl", "rw");
			}
			
			int numOfPages = pages(file);
			for(int page = 1; page <= numOfPages; page++){
				file.seek((page-1)*pageSize);
				byte pageType = file.readByte();
				if(pageType == 0x0D){
					byte numOfCells = Page.getCellNumber(file, page);
					for(int i = 0; i < numOfCells; i++){					
						long loc = Page.getCellLoc(file, page, i);	
						String[] vals = retrieveValues(file, loc);
						//int rowid = Integer.parseInt(vals[0]);

						//boolean check = conditionCheck(vals, rowid, cmp, cmpOrd);
						
						HashMap<String, String[]> columnInfoMap = getColumnInfoWithOrdKey(tableName);
						boolean check = conditionCheckNew(vals, cmp, columnInfoMap);
						
						if(check){
							long chaPos = findChangePointer(file, loc, setOrd);
							changeValueAtFixPosition(file, chaPos, dataType, set[2]);
						}
					}
				}
			}
			file.close();

		}catch(Exception e){
			System.out.println(e);
		}
	}
	
	public static void updateText(String tableName, String[] cmp, String[] set, int cmpOrd, int setOrd){
		try{
			RandomAccessFile file;
			if(tableName.equals("davisbase_tables") || tableName.equals("davisbase_columns")){
				file = new RandomAccessFile("data/catalog/" + tableName + ".tbl", "rw");
			}else{
				file = new RandomAccessFile("data/user_data/" + tableName + ".tbl", "rw");
			}
			HashMap<String, String[]> columnInfoMap = getColumnInfoWithOrdKey(tableName);
			
			int numOfPages = pages(file);
			for(int page = 1; page <= numOfPages; page++){
				file.seek((page - 1)*pageSize);
				byte pageType = file.readByte();
				if(pageType == 0x0D){
					byte numOfCells = Page.getCellNumber(file, page);
					for(int i = 0; i < numOfCells; i++){					
						long loc = Page.getCellLoc(file, page, i);
						int offset = Page.getCellOffset(file, page, i);
						String[] vals = retrieveValues(file, loc);
						
						//int rowid = Integer.parseInt(vals[0]);
						//boolean check = conditionCheck(vals, rowid, cmp, cmpOrd);
						
						boolean check = conditionCheckNew(vals, cmp, columnInfoMap);
						
						if(check){
							if(vals[setOrd - 1].length() < set[2].length()){
								System.out.println("value size overflow!");
								return;
							}
							vals[setOrd - 1] = set[2];
							byte[] stc = new byte[columnInfoMap.size() - 1];//column headers exclude rowid
							short plSize = (short) calPayloadSize(columnInfoMap, vals, stc);
							Page.insertJustCellData(file, page, offset, plSize, stc, vals);
							//long chaPos = findChangePointer(file, loc, setOrd);
						}
					}
				}
			}
			file.close();
		}catch(Exception e){
			System.out.println(e);
		}
	}
	
	public static void update(String tableName, String[] cmp, String[] set){
		HashMap<String, String[]> columnInfoMap = getColumnInfoWithNameKey(tableName);
		
		if(columnInfoMap.containsKey(set[0])){
			String dataType = columnInfoMap.get(set[0])[0];
			if(dataType.toLowerCase().equals("text")){//if the data type is text cannot update at the same location
				//delete and then insert
				int cmpOrd = Integer.parseInt(columnInfoMap.get(cmp[0])[1]);//order of the condition
				int setOrd = Integer.parseInt(columnInfoMap.get(set[0])[1]);//order of the set column
				updateText(tableName, cmp, set, cmpOrd, setOrd);
			}else{
				int cmpOrd = Integer.parseInt(columnInfoMap.get(cmp[0])[1]);//order of the condition
				int setOrd = Integer.parseInt(columnInfoMap.get(set[0])[1]);//order of the set column
				updateAtSamePos(tableName, cmp, set, cmpOrd, setOrd, dataType);
			}
		}else{
			System.out.println("Can not find " + cmp[0] + "!");
		}
		
	}
	
	public static void createTable(String tableName, String[] columns){
		//create table columns are the definition of columns
		try{
			//create table file 
			RandomAccessFile newfile = new RandomAccessFile("data/user_data/"+tableName+".tbl", "rw");
			newfile.setLength(pageSize);
			newfile.seek(0);
			newfile.writeByte(0x0D);//type of page
			newfile.writeByte(0x00);//number of record
			newfile.writeShort(pageSize);//pointer of the content
			newfile.writeInt(-1);
			newfile.close();
			
			//insert the new table into davisbase_tables.tbl
			int maxKey = getMaxKey("data/catalog/davisbase_tables.tbl");
			
			String[] tableValues = {Integer.toString(maxKey + 1), tableName, ""+0};//{rowid, table_name, rowid}
			insertIntoWithRowid("davisbase_tables", null, tableValues);
			
			//insert the new columns into davisbase_columns.tbl
			maxKey = getMaxKey("data/catalog/davisbase_columns.tbl");
			maxKey++;
			
			String[] rowidvalue = {Integer.toString(maxKey), tableName, "rowid", "INT", "1", "NO"};
			insertIntoWithRowid("davisbase_columns", null, rowidvalue);
			
			for(int i = 0; i < columns.length; i++){
				maxKey++;
				String[] token = columns[i].split("\\s+");
				String col_name = token[0];//column name
				String dt = token[1].toUpperCase();//data type uppercase
				String pos = Integer.toString(i + 2);//ordinal position
				String nullable;
				if(token.length > 2)
					nullable = "NO";
				else
					 nullable = "YES";
				String[] value = {Integer.toString(maxKey), tableName, col_name, dt, pos, nullable};
				
				insertIntoWithRowid("davisbase_columns", null, value);
			}
	
		}catch(Exception e){
			System.out.println(e);
		}
	}
	
	public static void insertIntoWithRowid(String tableName, String[] columns, String[] values){
		String newTable = "";
		if(tableName.equals("davisbase_tables") || tableName.equals("davisbase_columns")){
			newTable = "catalog/"+tableName;
		}else{
			newTable = "user_data/"+tableName;
		}
		try{
			RandomAccessFile file = new RandomAccessFile("data/" + newTable +".tbl", "rw");
			insertIntoWithRowid(file, tableName, columns, values);
			file.close();

		}catch(Exception e){
			System.out.println(e);
		}
	}
	
	public static void insertIntoNoRowid(String tableName, String[] columns, String[] values){
		String newTable = "";
		if(tableName.equals("davisbase_tables") || tableName.equals("davisbase_columns")){
			newTable = "catalog/"+tableName;
		}else{
			newTable = "user_data/"+tableName;
		}
		//add rowid into the values
		int maxKey = getMaxKey("data/" + newTable + ".tbl");
		String[] newValues = new String[values.length + 1];
		newValues[0] = Integer.toString(maxKey + 1);
		for(int i = 0; i < values.length; i++){
			newValues[i + 1] = values[i];
		}
		insertIntoWithRowid(tableName, columns, newValues);
	}
	
	
	//new
	//columns does not include rowid, valuse[0] is rowid
	public static void insertIntoWithRowid(RandomAccessFile file, String tableName, String[] columns, String[] values){
		HashMap<String, String[]> columnInfoMap = getColumnInfoWithOrdKey(tableName);
		if(columns == null || columns.length == 0){
			//construct full columns
			columns = new String[columnInfoMap.size() - 1];
			for(int i = 2; i <= columnInfoMap.size(); i++){
				columns[i - 2] = columnInfoMap.get("" + i)[0];
			}
		}
		
		if(columns.length != values.length - 1){//first value is rowid
			System.out.println("Column numbers does not match value number!");
		}
		
		//key check
		int key = new Integer(values[0]);
				
		int page = findRightMostLeaf(file);//locate the page where the record will be inserted
		
		HashMap<String, String> insertInfoMap = new HashMap<>();
		for(int i = 0; i < columns.length; i++){
			insertInfoMap.put(columns[i], values[i + 1]);
		}
		
		//check insert values nullable and construct full dressed values
		String[] fullValues = new String[columnInfoMap.size()];
		fullValues[0] = values[0];//rowid
		for(int i = 1; i <= columnInfoMap.size(); i++){
			//get the information of the column
			String[] colInfo = columnInfoMap.get("" + i);
			String colName = colInfo[0];
			//String dataType = colInfo[1];
			String isNullable = colInfo[2];
			if(colName.equals("rowid")){
				continue;
			}
			
			if(insertInfoMap.containsKey(colName)){
				fullValues[i - 1] = insertInfoMap.get(colName);
			}else{
				if(isNullable.toLowerCase().equals("no")){
					System.out.println(colName + "cannot be empty!");
				}else{
					fullValues[i - 1] = "null";
				}
			}
 		}
		
		byte[] stc = new byte[insertInfoMap.size()];//column headers exclude rowid
		short plSize = (short) calPayloadSize(columnInfoMap, fullValues, stc);//new one
		//short plSize = (short) calPayloadSize(tableName, fullValues, stc);//calculate the payload size and construct stc
		int cellSize = plSize + 6;//plus the 6 bytes info
		int offset = Page.checkLeafSpace(file, page, cellSize);//check if there is overflow and get the offset


		if(offset != -1){
			Page.insertLeafCell(file, page, offset, plSize, key, stc, fullValues);
		}else{
			int root = getRoot(tableName);
			int newRowid = Integer.parseInt(fullValues[0]);
			int newRoot = Page.extendLeaf(file, page, root, newRowid);
			updateRoot(tableName, newRoot);
			
			insertIntoWithRowid(file, tableName, null, fullValues);
		}
		
	}
	
	public static int findRightMostLeaf(RandomAccessFile file){
		int val = 0;
		try{
			int numPages = pages(file);
			for(int page = 1; page <= numPages; page++){
				if(Page.getRightMost(file, page) == -1){
					return page;
				}
			}
		}catch(Exception e){
			System.out.println(e);
		}
		return val;
	}
	
	public static void updateRoot(String tableName, int newRoot){
		//
		String[] cmp = new String[]{"table_name", "=", tableName};
		String[] set = new String[]{"root_page", "=", Integer.toString(newRoot)};
		ExecuteCommand.update("davisbase_tables", cmp, set);
	}
	
	public static int getRoot(String tableName){
		int root = 0;
		try{
			RandomAccessFile file = new RandomAccessFile("data/catalog/davisbase_tables.tbl", "rw");
			ResultBuffer buffer = new ResultBuffer();
			String[] columnName = {"rowid", "table_name", "root_page"};
			String[] cmp = {"table_name","=", tableName};
			filter(file, cmp, columnName, buffer);
			HashMap<Integer, String[]> content = buffer.content;
			for(String[] x : content.values()){
				root = Integer.parseInt(x[2]);
			}
			file.close();
		}catch(Exception e){
			System.out.println(e);
		}
		return root;
	}
	
	public static HashMap<String, String[]> getColumnInfoWithOrdKey(String tableName){
		//get the information of the columns and return a map with a key of ordinal_position
		//<ordinal_position, {column_name, data_type, is_nullable}>
		HashMap<String, String[]> columnInfo = new HashMap<>();
		try{
			RandomAccessFile file = new RandomAccessFile("data/catalog/davisbase_columns.tbl", "rw");
			ResultBuffer buffer = new ResultBuffer();
			String[] columnName = {"rowid", "table_name", "column_name", "data_type", "ordinal_position", "is_nullable"};
			String[] cmp = {"table_name","=", tableName};
			filter(file, cmp, columnName, buffer);
			HashMap<Integer, String[]> content = buffer.content;
			for(String[] x : content.values()){
				String[] cont = new String[] {x[2],x[3],x[5]};
				columnInfo.put(x[4], cont);
			}
			file.close();
		}catch(Exception e){
			System.out.println(e);
		}
		return columnInfo;
	}
	
	public static HashMap<String, String[]> getColumnInfoWithNameKey(String tableName){
		//get the information of the columns and return a map with a key of ordinal_position
		//<column_name, {data_type, ordinal_position, is_nullable}>
		HashMap<String, String[]> columnInfo = new HashMap<>();
		try{
			RandomAccessFile file = new RandomAccessFile("data/catalog/davisbase_columns.tbl", "rw");
			ResultBuffer buffer = new ResultBuffer();
			String[] columnName = {"rowid", "table_name", "column_name", "data_type", "ordinal_position", "is_nullable"};
			String[] cmp = {"table_name","=", tableName};
			filter(file, cmp, columnName, buffer);
			HashMap<Integer, String[]> content = buffer.content;
			for(String[] x : content.values()){
				String[] cont = new String[] {x[3],x[4],x[5]};
				columnInfo.put(x[2], cont);
			}
			file.close();
		}catch(Exception e){
			System.out.println(e);
		}
		return columnInfo;
	}
	
	//new
	public static int calPayloadSize(HashMap<String, String[]> columnInfoMapWithOrd, String[] vals, byte[] stc){
		int size = columnInfoMapWithOrd.size();//map includes rowid
		for(int i = 2; i <= columnInfoMapWithOrd.size(); i++){
			String dataType = columnInfoMapWithOrd.get("" + i)[1];
			stc[i - 2]= getStc(vals[i - 1], dataType);
			size = size + feildLength(stc[i - 2]);
		}
		
		return size;
	}
	
	public static byte getStc(String value, String dataType){
		if(value.equals("null")){
			switch(dataType){
				case "TINYINT":     return 0x00;
				case "SMALLINT":    return 0x01;
				case "INT":			return 0x02;
				case "BIGINT":      return 0x03;
				case "REAL":        return 0x02;
				case "DOUBLE":      return 0x03;
				case "DATETIME":    return 0x03;
				case "DATE":        return 0x03;
				case "TEXT":        return 0x0C;
				default:			return 0x00;
			}							
		}else{
			switch(dataType){
				case "TINYINT":     return 0x04;
				case "SMALLINT":    return 0x05;
				case "INT":			return 0x06;
				case "BIGINT":      return 0x07;
				case "REAL":        return 0x08;
				case "DOUBLE":      return 0x09;
				case "DATETIME":    return 0x0A;
				case "DATE":        return 0x0B;
				case "TEXT":        return (byte)(value.length()+0x0C);
				default:			return 0x00;
			}
		}
	}
	
	public static short feildLength(byte stc){
		switch(stc){
			case 0x00: return 1;
			case 0x01: return 2;
			case 0x02: return 4;
			case 0x03: return 8;
			case 0x04: return 1;
			case 0x05: return 2;
			case 0x06: return 4;
			case 0x07: return 8;
			case 0x08: return 4;
			case 0x09: return 8;
			case 0x0A: return 8;
			case 0x0B: return 8;
			default:   return (short)(stc - 0x0C);
		}
	}
	
	public static int searchKeyPage(RandomAccessFile file, int key){
		int val = 1;
		try{
			int numPages = pages(file);
			for(int page = 1; page <= numPages; page++){
				file.seek((page - 1)*pageSize);
				byte pageType = file.readByte();
				if(pageType == 0x0D){
					int[] keys = Page.getKeyArray(file, page);
					if(keys.length == 0)
						return 0;
					int rm = Page.getRightMost(file, page);
					if(keys[0] <= key && key <= keys[keys.length - 1]){
						return page;
					}else if(rm == 0 && keys[keys.length - 1] < key){
						return page;
					}
				}
			}
		}catch(Exception e){
			System.out.println(e);
		}

		return val;
	}
	
	public static String[] getNullable(String table){
		String[] nullable = new String[0];
		try{
			RandomAccessFile file = new RandomAccessFile("data/catalog/davisbase_columns.tbl", "rw");
			ResultBuffer buffer = new ResultBuffer();
			String[] columnName = {"rowid", "table_name", "column_name", "data_type", "ordinal_position", "is_nullable"};
			String[] cmp = {"table_name","=",table};
			filter(file, cmp, columnName, buffer);
			HashMap<Integer, String[]> content = buffer.content;
			ArrayList<String> array = new ArrayList<String>();
			for(String[] i : content.values()){
				array.add(i[5]);
			}
			int size=array.size();
			nullable = array.toArray(new String[size]);
			file.close();
			return nullable;
		}catch(Exception e){
			System.out.println(e);
		}
		return nullable;
	}
	
	
	
	public static void select(String tableName, String[] cols, String[] cmp){
		try{
			RandomAccessFile file;
			if(tableName.equals("davisbase_tables") || tableName.equals("davisbase_columns")){
				file = new RandomAccessFile("data/catalog/"+tableName+".tbl", "rw");
			}else{
				file = new RandomAccessFile("data/user_data/"+tableName+".tbl", "rw");
			}
			
			HashMap<String, String[]> columnInfoMapWithOrd = getColumnInfoWithOrdKey(tableName);
			int len = columnInfoMapWithOrd.size();
			String[] columnName = new String[len];
			String[] type = new String[len];
			
			for(int i = 1; i <= len; i++){
				String[] column = columnInfoMapWithOrd.get("" + i);
				columnName[i - 1] = column[0];
				type[i - 1] = column[1];
			}
			
			
			ResultBuffer resBuffer = new ResultBuffer();
			
			filter(file, cmp, columnName, type, resBuffer, columnInfoMapWithOrd);
			resBuffer.display(cols);
			file.close();
		}catch(Exception e){
			System.out.println(e);
		}
	}
	//select used
	public static void filter(RandomAccessFile file, String[] condition, String[] columnNames, 
			String[] types, ResultBuffer buffer, HashMap<String, String[]> columnInfoMapWithOrd){
		try{
			//get the number of the pages
			int numOfPages = pages(file);
			
			for(int pageIndex = 1; pageIndex <= numOfPages; pageIndex++){
				
				file.seek((pageIndex-1)*pageSize);//location of page
				byte pageType = file.readByte();
				
				//if it is the leaf page
				if(pageType == 0x0D){
					//get the number of the cells
					byte numOfCells = Page.getCellNumber(file, pageIndex);

					for(int cellIndex = 0; cellIndex < numOfCells; cellIndex++){
						//get the array of values of the cell
						long loc = Page.getCellLoc(file, pageIndex, cellIndex);//cell location
						String[] vals = retrieveValues(file, loc);
						int rowid = Integer.parseInt(vals[0]);
						
						//process date and datatime type
						for(int j = 0; j < types.length; j++){
							if(types[j].equals("DATE") || types[j].equals("DATETIME")){
								vals[j] = "'"+vals[j]+"'";
							}
						}
						
						//check if the value match the comparator
						//boolean check = conditionCheck(vals, rowid , condition, columnNames);
						
						boolean check = conditionCheckNew(vals, condition, columnInfoMapWithOrd);
						
						for(int j=0; j < types.length; j++){
							if(types[j].equals("DATE") || types[j].equals("DATETIME")){
								vals[j] = vals[j].substring(1, vals[j].length()-1);
							}
						}
						
						//add the record who meets the requirement
						if(check){
							buffer.add(rowid, vals);
						}
					}
				}
			}

			buffer.columnName = columnNames;
			buffer.colMaxLength = new int[columnNames.length];

		}catch(Exception e){
			System.out.println("Error at filter");
			e.printStackTrace();
		}

	}


	//get all the results and store them in the result buffer
	public static void filter(RandomAccessFile file, String[] cmp, String[] columnName, ResultBuffer buffer){
		try{
			
			int numOfPages = pages(file);
			for(int page = 1; page <= numOfPages; page++){
				
				file.seek((page-1)*pageSize);
				byte pageType = file.readByte();
				if(pageType == 0x0D)
				{
					byte numOfCells = Page.getCellNumber(file, page);

					for(int i=0; i < numOfCells; i++){						
						long loc = Page.getCellLoc(file, page, i);	
						String[] vals = retrieveValues(file, loc);
						int rowid=Integer.parseInt(vals[0]);

						boolean check = conditionCheck(vals, rowid, cmp, columnName);
						
						if(check)
							buffer.add(rowid, vals);
					}
				}
			}

			buffer.columnName = columnName;
			buffer.colMaxLength = new int[columnName.length];

		}catch(Exception e){
			System.out.println("Error at filter");
			e.printStackTrace();
		}

	}
	
	public static boolean conditionCheck(String[] values, int rowid, String[] condition, int colPos){

		boolean check = false;
		
		if(condition.length == 0){
			check = true;
		}else{
			if(colPos == 1){//rowid
				int val = Integer.parseInt(condition[2]);
				String operator = condition[1];
				switch(operator){
					case "=": if(rowid == val) 
								check = true;
							  else
							  	check = false;
							  break;
					case ">": if(rowid > val) 
								check = true;
							  else
							  	check = false;
							  break;
					case ">=": if(rowid >= val) 
						        check = true;
					          else
					  	        check = false;	
					          break;
					case "<": if(rowid < val) 
								check = true;
							  else
							  	check = false;
							  break;
					case "<=": if(rowid <= val) 
								check = true;
							  else
							  	check = false;	
							  break;
					case "!=": if(rowid != val)  
								check = true;
							  else
							  	check = false;	
							  break;						  							  							  							
				}
			}else{
				if(condition[2].equals(values[colPos-1]))
					check = true;
				else
					check = false;
			}
		}
		return check;
	}
	
	public static double toDoubleValue(String value, String dataType){
		double res = 0;
		try{
			switch(dataType){
			case "DATETIME":
				Date temp = new SimpleDateFormat(datePattern).parse(value.substring(1, value.length()-1));
				res = temp.getTime();
				break;
			case "DATE":
				value = value.substring(1, value.length()-1);
				value = value+"_00:00:00";
				Date temp2 = new SimpleDateFormat(datePattern).parse(value);
				res = temp2.getTime();
				break;
			default:
				res = Double.parseDouble(value);
				break;
		}
		}catch(Exception e){
			System.out.println(e);
		}
		
		
		return res;
	}
	
	public static boolean conditionCheckNew(String firstValue, String operator, String secondValue, String dataType){
		boolean check = false;
		
		if(operator.equals("=")){
			check = firstValue.equals(secondValue);
		}else{
			double value1 = toDoubleValue(firstValue, dataType);
			double value2 = toDoubleValue(secondValue, dataType);
			
			switch(operator){
				
				case ">": if(value1 > value2) 
							check = true;
						  else
						  	check = false;
						  break;
				case ">=": if(value1 >= value2) 
					        check = true;
				          else
				  	        check = false;	
				          break;
				case "<": if(value1 < value2) 
							check = true;
						  else
						  	check = false;
						  break;
				case "<=": if(value1 <= value2) 
							check = true;
						  else
						  	check = false;	
						  break;
				case "!=": if(value1 != value2)  
							check = true;
						  else
						  	check = false;	
						  break;						  							  							  							
			}
		}
		
		return check;
	}
	
	public static boolean conditionCheckNew(String[] values, String[] condition, HashMap<String, String[]> columnInforWithOrd){

		boolean check = false;
		
		if(condition.length == 0){
			check = true;
		}else{
			for(int i = 1; i <= columnInforWithOrd.size(); i++){
				//<ordinal_position, {column_name, data_type, is_nullable}>
				String[] lolumnInfo = columnInforWithOrd.get("" + i);
				String columnName = lolumnInfo[0];
				String dataType = lolumnInfo[1];
				if(condition[0].equals(columnName)){
					check = conditionCheckNew(values[i - 1], condition[1], condition[2], dataType);
				}
			}
			
		}
		return check;
	}
	
	
	public static boolean conditionCheck(String[] values, int rowid, String[] condition, String[] columnNames){

		boolean check = false;
		
		if(condition.length == 0){
			check = true;
		}else{
			int colPos = 1;
			//find the column name in the condition
			for(int i = 0; i < columnNames.length; i++){
				if(columnNames[i].equals(condition[0])){
					colPos = i + 1;
					break;
				}
			}
			
			if(colPos == 1){//rowid
				int val = Integer.parseInt(condition[2]);
				String operator = condition[1];
				switch(operator){
					case "=": if(rowid == val) 
								check = true;
							  else
							  	check = false;
							  break;
					case ">": if(rowid > val) 
								check = true;
							  else
							  	check = false;
							  break;
					case ">=": if(rowid >= val) 
						        check = true;
					          else
					  	        check = false;	
					          break;
					case "<": if(rowid < val) 
								check = true;
							  else
							  	check = false;
							  break;
					case "<=": if(rowid <= val) 
								check = true;
							  else
							  	check = false;	
							  break;
					case "!=": if(rowid != val)  
								check = true;
							  else
							  	check = false;	
							  break;						  							  							  							
				}
			}else{
				if(condition[2].equals(values[colPos-1]))
					check = true;
				else
					check = false;
			}
		}
		return check;
	}
	
	//get the values of a cell
	public static String[] retrieveValues(RandomAccessFile file, long loc){
		
		String[] values = null;
		try{
			SimpleDateFormat dateFormat = new SimpleDateFormat (datePattern);

			file.seek(loc + 2);
			int key = file.readInt();//rowid
			int numOfCols = file.readByte();//number of columns
			
			byte[] stc = new byte[numOfCols];
			file.read(stc);//get the array of data types
			
			values = new String[numOfCols + 1]; //result array including rowid
			
			values[0] = Integer.toString(key);
			
			for(int i = 1; i <= numOfCols; i++){
				switch(stc[i-1]){
					case 0x00:  file.readByte();
					            values[i] = "null";
								break;

					case 0x01:  file.readShort();
					            values[i] = "null";
								break;

					case 0x02:  file.readInt();
					            values[i] = "null";
								break;

					case 0x03:  file.readLong();
					            values[i] = "null";
								break;

					case 0x04:  values[i] = Integer.toString(file.readByte());
								break;

					case 0x05:  values[i] = Integer.toString(file.readShort());
								break;

					case 0x06:  values[i] = Integer.toString(file.readInt());
								break;

					case 0x07:  values[i] = Long.toString(file.readLong());
								break;

					case 0x08:  values[i] = String.valueOf(file.readFloat());
								break;

					case 0x09:  values[i] = String.valueOf(file.readDouble());
								break;

					case 0x0A:  Long temp = file.readLong();
								Date dateTime = new Date(temp);
								values[i] = dateFormat.format(dateTime);
								break;

					case 0x0B:  temp = file.readLong();
								Date date = new Date(temp);
								values[i] = dateFormat.format(date).substring(0,10);
								break;

					default:    int len = new Integer(stc[i-1] - 0x0C);
								byte[] bytes = new byte[len];
								file.read(bytes);
								values[i] = new String(bytes);
								break;
				}
			}

		}catch(Exception e){
			System.out.println(e);
		}

		return values;
	}
	
	public static void drop(String tableName){
		try{
			String[] condition = {"table_name", "=", tableName};
			deleteRow("davisbase_tables",condition);
			deleteRow("davisbase_columns",condition);
			
			File oldFile = new File("data/user_data", tableName+".tbl"); 
			oldFile.delete();
		}catch(Exception e){
			System.out.println(e);
		}

	}
	
	public static void deleteRow(String tableName, String[] condition){
		String newTable = "";
		if(tableName.equals("davisbase_tables") || tableName.equals("davisbase_columns")){
			newTable = "catalog/"+tableName;
		}else{
			newTable = "user_data/"+tableName;
		}
		try{
			RandomAccessFile file = new RandomAccessFile("data/" + newTable + ".tbl", "rw");
			HashMap<String, String[]> columnInfoMap = getColumnInfoWithOrdKey(tableName);
			String[] columns = new String[columnInfoMap.size()];
			for(int i = 1; i <= columnInfoMap.size(); i++){
				columns[i - 1] = columnInfoMap.get("" + i)[0];
			}
			
			int numOfPages = pages(file);			
			for(int page = 1; page <= numOfPages; page ++){
				file.seek((page-1)*pageSize);
				byte fileType = file.readByte();
				if(fileType == 0x0D){
					short[] cellsAddr = Page.getCellArray(file, page);
					short lastCellAddr = cellsAddr[0];
					int k = 0;
					for(int i = 0; i < cellsAddr.length; i++){
						//long loc = Page.getCellLoc(file, page, i);
						long loc = (page - 1)*pageSize + cellsAddr[i];
						String[] vals = retrieveValues(file, loc);
						//String tb = vals[1];
						//int rowid = Integer.parseInt(vals[0]);
						//boolean check = conditionCheck(vals, rowid, condition, columns);
						boolean check = conditionCheckNew(vals, condition, columnInfoMap);
						if(!check){
							Page.setCellOffset(file, page, k, cellsAddr[i]);
							lastCellAddr = cellsAddr[i];
							k++;
						}
					}
					Page.setCellNumber(file, page, (byte)k);
					Page.seContentStart(file, page, lastCellAddr);
				}
			}
			file.close();
		}catch(Exception e){
			System.out.println(e);
		}
		
	}
	
}
