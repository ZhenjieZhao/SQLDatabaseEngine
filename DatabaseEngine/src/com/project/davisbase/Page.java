package com.project.davisbase;
import java.io.RandomAccessFile;

import java.util.Date;
import java.text.SimpleDateFormat;

public class Page{
	public static int pageSize = 512;
	public static final String datePattern = "yyyy-MM-dd_HH:mm:ss";
	public static int fileroot = 0;
	
	public static short calPayloadSize(String[] values, String[] dataType){
		int val = dataType.length; 
		for(int i = 1; i < dataType.length; i++){
			String dt = dataType[i];
			switch(dt){
				case "TINYINT":
					val = val + 1;
					break;
				case "SMALLINT":
					val = val + 2;
					break;
				case "INT":
					val = val + 4;
					break;
				case "BIGINT":
					val = val + 8;
					break;
				case "REAL":
					val = val + 4;
					break;		
				case "DOUBLE":
					val = val + 8;
					break;
				case "DATETIME":
					val = val + 8;
					break;
				case "DATE":
					val = val + 8;
					break;
				case "TEXT":
					String text = values[i];
					int len = text.length();
					val = val + len;
					break;
				default:
					break;
			}
		}
		return (short)val;
	}

	public static int makeInteriorPage(RandomAccessFile file){
		int num_pages = 0;
		try{
			num_pages = (int)(file.length()/(new Long(pageSize)));
			num_pages = num_pages + 1;
			file.setLength(pageSize * num_pages);
			file.seek((num_pages-1)*pageSize);
			file.writeByte(0x05);
			file.writeByte(0x00);
			file.writeShort(pageSize);//content start pointer
		}catch(Exception e){
			System.out.println(e);
		}

		return num_pages;
	}

	public static int makeLeafPage(RandomAccessFile file){
		int pagenum = 0;
		try{
			pagenum = (int)(file.length()/(new Long(pageSize)));
			pagenum += 1;
			file.setLength(pageSize * pagenum);//create a new page
			//initialize the header of the new page
			file.seek((pagenum-1)*pageSize);
			file.writeByte(0x0D);
			file.writeByte(0x00);
			file.writeShort(pageSize);//content start pointer
			file.writeInt(-1);
		}catch(Exception e){
			System.out.println(e);
		}

		return pagenum;

	}

	public static void updateRightPage(RandomAccessFile file, int page, int rightPage){
		try{
			file.seek((page-1)*pageSize + 4);
			file.writeInt(rightPage);
		}catch(Exception e){
			System.out.println("updateRightPage error!");
			System.out.println(e);
		}
		
	}
	
	public static boolean isInteriorFull(RandomAccessFile file, int interiorPage){
		byte numCells = getCellNumber(file, interiorPage);
		if(numCells > 50)
			return true;
		else
			return false;
	}
	
	/*public static int extendInteriorNode(RandomAccessFile file, int interiorPage, int newRowid){
		int parent = getParent(file, interiorPage);
		if(parent == 0){
			int newInterioPage = makeInteriorPage(file);
			int rootPage = makeInteriorPage(file);
			fileroot = rootPage;
			updateRightPage(file, rootPage, newInterioPage);
			if()
			insertInteriorCell(file, rootPage, interiorPage, newRowid);
		}else{
			
		}
		return parent;
	}*/
	
	//extend a leaf and then return a new root number
	public static int extendLeaf(RandomAccessFile file, int page, int root, int newRowid){
		fileroot = root;
		int newLeaf = makeLeafPage(file);
		updateRightPage(file, page, newLeaf);
		
		int parent = getParent(file, page);
		if(fileroot == 0){//if there is no interior node
			int rootPage = makeInteriorPage(file);
			fileroot = rootPage;
			updateRightPage(file, rootPage, newLeaf);
			insertInteriorCell(file, rootPage, page, newRowid);
		}else{
			
			int insertparent = insertIntoInteriorNode(file, parent, page, newRowid);
			updateRightPage(file, insertparent, newLeaf);
			/*parent = extendInteriorNode(file, parent, newRowid);
			updateRightPage(file, parent, newLeaf);
			insertInteriorCell(file, parent, page, newRowid);*/
		}		
		
		return fileroot;
	}
	
	public static int insertIntoInteriorNode(RandomAccessFile file, int parent, int child, int newRowid){
		int insertparent = 0;
		int greatParent = getParent(file, parent);
		try{
			if(!isInteriorFull(file, parent)){
				insertInteriorCell(file, parent, child, newRowid);
				insertparent = parent;
			}else{
				if(fileroot == parent){
					int rootPage = makeInteriorPage(file);
					fileroot = rootPage;
					int newParent = makeInteriorPage(file);
					insertparent = newParent;
					
					updateRightPage(file, rootPage, newParent);
					insertInteriorCell(file, rootPage, parent, newRowid);
				}else{
					int newParent = makeInteriorPage(file);
					insertparent = newParent;
					
					int insertGreatParent = insertIntoInteriorNode(file, greatParent, parent, newRowid);
					updateRightPage(file, insertGreatParent, insertparent);
				}
			}
		}catch(Exception e){
			System.out.println(e);
		}
		return insertparent;
	}
	
	

	public static int[] getKeyArray(RandomAccessFile file, int page){
		int num = new Integer(getCellNumber(file, page));
		
		int[] array = new int[num];
		if(num == 0){
			return array;
		}

		try{
			file.seek((page-1)*pageSize);
			byte pageType = file.readByte();
			byte offset = 0;
			switch(pageType){
			    case 0x0d:
				    offset = 2;
				    break;
				case 0x05:
					offset = 4;
					break;
				default:
					offset = 2;
					break;
			}

			for(int i = 0; i < num; i++){
				long loc = getCellLoc(file, page, i);
				file.seek(loc+offset);
				array[i] = file.readInt();
			}

		}catch(Exception e){
			System.out.println(e);
		}

		return array;
	}
	
	public static short[] getCellArray(RandomAccessFile file, int page){
		int num = new Integer(getCellNumber(file, page));
		short[] array = new short[num];

		try{
			file.seek((page-1)*pageSize + 8);
			for(int i = 0; i < num; i++){
				array[i] = file.readShort();
			}
		}catch(Exception e){
			System.out.println(e);
		}

		return array;
	}

	
	public static long getPointerLoc(RandomAccessFile file, int page, int parent){
		long val = 0;
		try{
			int numCells = new Integer(getCellNumber(file, parent));
			for(int i=0; i < numCells; i++){
				long loc = getCellLoc(file, parent, i);
				file.seek(loc);
				int childPage = file.readInt();
				if(childPage == page){
					val = loc;
				}
			}
		}catch(Exception e){
			System.out.println(e);
		}

		return val;
	}

	public static void setPointerLoc(RandomAccessFile file, long loc, int parent, int page){
		try{
			if(loc == 0){
				file.seek((parent-1)*pageSize+4);
			}else{
				file.seek(loc);
			}
			file.writeInt(page);
		}catch(Exception e){
			System.out.println(e);
		}
	} 

	
	public static void insertInteriorCell(RandomAccessFile file, int page, int child, int key){
		try{
			
			file.seek((page-1)*pageSize+2);
			short contentStart = file.readShort();
			
			if(contentStart == 0)
				contentStart = 512;
			
			contentStart = (short)(contentStart - 8);
			
			//write the cell content (left child page, key)
			file.seek((page-1)*pageSize + contentStart);
			file.writeInt(child);
			file.writeInt(key);
			
			file.seek((page-1)*pageSize+2);
			file.writeShort(contentStart);
			
			//add the new cell location into the header
			byte num = getCellNumber(file, page);
			setCellOffset(file, page ,num, contentStart);
			//update the number of cell
			num = (byte) (num + 1);
			setCellNumber(file, page, num);

		}catch(Exception e){
			System.out.println(e);
		}
	}

	public static void insertJustCellData(RandomAccessFile file, int page, int offset, short plsize, byte[] stc, String[] vals){
		try{
			String s;
			file.seek((page-1)*pageSize + offset);//pointer to the new record start position
			file.writeShort(plsize);//payload content
			int key = Integer.parseInt(vals[0]);
			file.writeInt(key);//rowid
			int colNum = vals.length - 1;//number of columns
			file.writeByte(colNum);
			file.write(stc);//array of column types
			for(int i = 1; i < vals.length; i++){
				switch(stc[i-1]){
					case 0x00:
						file.writeByte(0);
						break;
					case 0x01:
						file.writeShort(0);
						break;
					case 0x02:
						file.writeInt(0);
						break;
					case 0x03:
						file.writeLong(0);
						break;
					case 0x04:
						file.writeByte(new Byte(vals[i]));
						break;
					case 0x05:
						file.writeShort(new Short(vals[i]));
						break;
					case 0x06:
						file.writeInt(new Integer(vals[i]));
						break;
					case 0x07:
						file.writeLong(new Long(vals[i]));
						break;
					case 0x08:
						file.writeFloat(new Float(vals[i]));
						break;
					case 0x09:
						file.writeDouble(new Double(vals[i]));
						break;
					case 0x0A:
						s = vals[i];
						Date temp = new SimpleDateFormat(datePattern).parse(s.substring(1, s.length()-1));
						long time = temp.getTime();
						file.writeLong(time);
						break;
					case 0x0B:
						s = vals[i];
						s = s.substring(1, s.length()-1);
						s = s+"_00:00:00";
						Date temp2 = new SimpleDateFormat(datePattern).parse(s);
						long time2 = temp2.getTime();
						file.writeLong(time2);
						break;
					default:
						file.writeBytes(vals[i]);
						break;
				}
			}
			
		}catch(Exception e){
			System.out.println(e);
		}
	}
	
	public static void insertLeafCell(RandomAccessFile file, int page, int offset, short plsize, int key, byte[] stc, String[] vals){
		try{
			String s;
			file.seek((page-1)*pageSize + offset);//pointer to the new record start position
			file.writeShort(plsize);//payload content
			file.writeInt(key);//rowid
			int colNum = vals.length - 1;//number of columns
			file.writeByte(colNum);
			file.write(stc);//array of column types
			for(int i = 1; i < vals.length; i++){
				switch(stc[i-1]){
					case 0x00:
						file.writeByte(0);
						break;
					case 0x01:
						file.writeShort(0);
						break;
					case 0x02:
						file.writeInt(0);
						break;
					case 0x03:
						file.writeLong(0);
						break;
					case 0x04:
						file.writeByte(new Byte(vals[i]));
						break;
					case 0x05:
						file.writeShort(new Short(vals[i]));
						break;
					case 0x06:
						file.writeInt(new Integer(vals[i]));
						break;
					case 0x07:
						file.writeLong(new Long(vals[i]));
						break;
					case 0x08:
						file.writeFloat(new Float(vals[i]));
						break;
					case 0x09:
						file.writeDouble(new Double(vals[i]));
						break;
					case 0x0A:
						s = vals[i];
						Date temp = new SimpleDateFormat(datePattern).parse(s.substring(1, s.length()-1));
						long time = temp.getTime();
						file.writeLong(time);
						break;
					case 0x0B:
						s = vals[i];
						s = s.substring(1, s.length()-1);
						s = s+"_00:00:00";
						Date temp2 = new SimpleDateFormat(datePattern).parse(s);
						long time2 = temp2.getTime();
						file.writeLong(time2);
						break;
					default:
						file.writeBytes(vals[i]);
						break;
				}
			}
			//add the start of the new record
			int n = getCellNumber(file, page);
			byte tmp = (byte) (n + 1);
			setCellNumber(file, page, tmp);//update the number of the cells
			file.seek((page-1)*pageSize + 8 + n*2);
			file.writeShort(offset);
			//update start of the content
			file.seek((page-1)*pageSize + 2);
			int content = file.readShort();
			if(content >= offset || content == pageSize){
				file.seek((page-1)*pageSize + 2);
				file.writeShort(offset);
			}
		}catch(Exception e){
			System.out.println(e);
		}
	}

	public static void updateLeafCell(RandomAccessFile file, int page, int offset, int plsize, int key, byte[] stc, String[] vals){
		try{
			String s;
			file.seek((page-1)*pageSize+offset);
			file.writeShort(plsize);
			file.writeInt(key);
			int col = vals.length - 1;
			file.writeByte(col);
			file.write(stc);
			for(int i = 1; i < vals.length; i++){
				switch(stc[i-1]){
					case 0x00:
						file.writeByte(0);
						break;
					case 0x01:
						file.writeShort(0);
						break;
					case 0x02:
						file.writeInt(0);
						break;
					case 0x03:
						file.writeLong(0);
						break;
					case 0x04:
						file.writeByte(new Byte(vals[i]));
						break;
					case 0x05:
						file.writeShort(new Short(vals[i]));
						break;
					case 0x06:
						file.writeInt(new Integer(vals[i]));
						break;
					case 0x07:
						file.writeLong(new Long(vals[i]));
						break;
					case 0x08:
						file.writeFloat(new Float(vals[i]));
						break;
					case 0x09:
						file.writeDouble(new Double(vals[i]));
						break;
					case 0x0A:
						s = vals[i];
						Date temp = new SimpleDateFormat(datePattern).parse(s.substring(1, s.length()-1));
						long time = temp.getTime();
						file.writeLong(time);
						break;
					case 0x0B:
						s = vals[i];
						s = s.substring(1, s.length()-1);
						s = s+"_00:00:00";
						Date temp2 = new SimpleDateFormat(datePattern).parse(s);
						long time2 = temp2.getTime();
						file.writeLong(time2);
						break;
					default:
						file.writeBytes(vals[i]);
						break;
				}
			}
		}catch(Exception e){
			System.out.println(e);
		}
	}

	
	public static boolean checkInteriorSpace(RandomAccessFile file, int page){
		//if cannot add new cell return true
		byte numCells = getCellNumber(file, page);
		if(numCells > 30)
			return true;
		else
			return false;
	}

	public static int checkLeafSpace(RandomAccessFile file, int page, int size){
		int val = -1;

		try{
			file.seek((page-1)*pageSize + 2);//content pointer
			int contentStart = file.readShort();//start of the content
			if(contentStart == 0)//if initialized
				return pageSize - size - 10;
			int numCells = getCellNumber(file, page);
			int space = contentStart - 10 - 2*numCells;
			if(size < space)
				return contentStart - size;//return offset
			
		}catch(Exception e){
			System.out.println(e);
		}

		return val;
	}

	
	public static int getParentOld(RandomAccessFile file, int page){
		int val = 0;

		try{
			file.seek((page-1)*pageSize + 8);
			val = file.readInt();
		}catch(Exception e){
			System.out.println(e);
		}

		return val;
	}
	
	public static int getParent(RandomAccessFile file, int page){
		int val = 0;
		if(fileroot == 0){
			return 0;
		}
		try{
			val = fileroot;
			
			while(getRightMost(file, val) != page){
				val = getRightMost(file, val);
				if(getPageType(file, val) == 0x0d){
					System.out.println("Find parent error!");
					break;
				}
			}
		}catch(Exception e){
			System.out.println(e);
		}

		return val;
	}

	public static void setParent(RandomAccessFile file, int page, int parent){
		try{
			file.seek((page-1)*pageSize+8);
			file.writeInt(parent);
		}catch(Exception e){
			System.out.println(e);
		}
	}
	
	public static int getRightMost(RandomAccessFile file, int page){
		int rl = 0;

		try{
			file.seek((page - 1)*pageSize + 4);
			rl = file.readInt();
		}catch(Exception e){
			System.out.println("Error at getRightMost");
		}

		return rl;
	}

	public static void setRightMost(RandomAccessFile file, int page, int rightLeaf){

		try{
			file.seek((page-1)*pageSize+4);
			file.writeInt(rightLeaf);
		}catch(Exception e){
			System.out.println("Error at setRightMost");
		}

	}

	public static boolean hasKey(RandomAccessFile file, int page, int key){
		int[] keys = getKeyArray(file, page);
		for(int i : keys)
			if(key == i)
				return true;
		return false;
	}
	
	public static long getCellLoc(RandomAccessFile file, int page, int id){
		long loc = 0;
		try{
			file.seek((page-1)*pageSize + 8 + id*2);
			short offset = file.readShort();
			long orig = (page-1)*pageSize;
			loc = orig + offset;
		}catch(Exception e){
			System.out.println(e);
		}
		return loc;
	}

	public static byte getCellNumber(RandomAccessFile file, int page){
		byte val = 0;

		try{
			file.seek((page-1)*pageSize + 1);
			val = file.readByte();
		}catch(Exception e){
			System.out.println(e);
		}

		return val;
	}

	public static void setCellNumber(RandomAccessFile file, int page, byte num){
		try{
			file.seek((page-1)*pageSize+1);
			file.writeByte(num);
		}catch(Exception e){
			System.out.println(e);
		}
	}
	
	public static void seContentStart(RandomAccessFile file, int page, short lastCellAddr){
		try{
			file.seek((page-1)*pageSize + 2);
			file.writeShort(lastCellAddr);
		}catch(Exception e){
			System.out.println(e);
		}
	}
	
	public static short getCellOffset(RandomAccessFile file, int page, int id){
		short offset = 0;
		try{
			file.seek((page-1)*pageSize + 8 + id*2);
			offset = file.readShort();
		}catch(Exception e){
			System.out.println(e);
		}
		return offset;
	}

	public static void setCellOffset(RandomAccessFile file, int page, int numOfCell, int offset){
		try{
			file.seek((page-1)*pageSize + 8 + numOfCell*2);
			file.writeShort(offset);
		}catch(Exception e){
			System.out.println(e);
		}
	}
    
	public static byte getPageType(RandomAccessFile file, int page){
		byte type=0x05;
		try {
			file.seek((page-1)*pageSize);
			type = file.readByte();
		} catch (Exception e) {
			System.out.println(e);
		}
		return type;
	}

}















