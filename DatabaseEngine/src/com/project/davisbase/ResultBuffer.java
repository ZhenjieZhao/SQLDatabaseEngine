package com.project.davisbase;
import java.util.HashMap;
import java.util.SortedSet;
import java.util.TreeSet;

class ResultBuffer{
	//this class is used to store tempt result
	public int numOfRow; 
	public HashMap<Integer, String[]> content;
	public String[] columnName; 
	public int[] colMaxLength; 
	
	public ResultBuffer(){
		numOfRow = 0;
		content = new HashMap<Integer, String[]>();
	}

	public void add(int rowid, String[] val){
		content.put(rowid, val);
		numOfRow++;
	}

	public void updateFormat(){
		//format stores the maximum length of column name and values
		for(int i = 0; i < colMaxLength.length; i++)
			colMaxLength[i] = columnName[i].length();
		
		
		
		for(String[] i : content.values()){
			for(int j = 0; j < i.length; j++){
				if(colMaxLength[j] < i[j].length()){
					colMaxLength[j] = i[j].length();
				}
			}
		}
			
	}

	public String fix(int len, String s) {
		return String.format("%-"+(len+3)+"s", s);
	}
	
	private void horLine(){
		for(int l: colMaxLength)
			System.out.print(DataBase.splashLine("-", l+3));
	}


	public void display(String[] col){
		
		if(numOfRow == 0){
			System.out.println("Empty set.");
		}else{
			updateFormat();
			
			if(col[0].equals("*")){
				//top line
				horLine();
				
				System.out.println();
				//table header
				for(int i = 0; i < columnName.length; i++)
					System.out.print(fix(colMaxLength[i], columnName[i])+"|");
				
				System.out.println();
				
				horLine();
				
				System.out.println();
				
				
				//table content
				SortedSet<Integer> keys = new TreeSet<>(content.keySet());
				for (int key : keys) { 
				   String[] value = content.get(key);
				   for(int j = 0; j < value.length; j++)
						System.out.print(fix(colMaxLength[j], value[j])+"|");
					System.out.println();
				   // do something
				}
				
				
				/*
				for(String[] i : content.values()){
					for(int j = 0; j < i.length; j++)
						System.out.print(fix(colMaxLength[j], i[j])+"|");
					System.out.println();
				}*/
				
				//bottom line
				horLine();
				System.out.println();
			
			}else{
				int[] control = new int[col.length];
				for(int j = 0; j < col.length; j++)
					for(int i = 0; i < columnName.length; i++)
						if(col[j].equals(columnName[i]))
							control[j] = i;

				for(int j = 0; j < control.length; j++)
					System.out.print(DataBase.splashLine("-", colMaxLength[control[j]]+3));
				
				System.out.println();
				
				for(int j = 0; j < control.length; j++)
					System.out.print(fix(colMaxLength[control[j]], columnName[control[j]])+"|");
				
				System.out.println();
				
				for(int j = 0; j < control.length; j++)
					System.out.print(DataBase.splashLine("-", colMaxLength[control[j]]+3));
				
				System.out.println();
				
				for(String[] i : content.values()){
					for(int j = 0; j < control.length; j++)
						System.out.print(fix(colMaxLength[control[j]], i[control[j]])+"|");
					System.out.println();
				}
				for(int l: colMaxLength)
					System.out.print(DataBase.splashLine("-", l+3));
				System.out.println();
			}
		}
	}
}