package project2;

import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Random;

public class RandPDatasets {

	public static void main(String[] args) {
		try{
			FileOutputStream out = new FileOutputStream("datasetPoints.txt" );
			Random rand = new Random();
			int linesLens = 12000000;
		    int x = 0;
		    int y = 0;
			
			for(int i = 1; i <= linesLens; i++) {
		    	
		    	// x axis of points
				x = 1 + rand.nextInt(10000);
				
				// y axis of points
				y = 1 + rand.nextInt(10000);
				
				// write (3,15)
				out.write((x+","+y).getBytes());
				
		    	// switch to the next line
		    	out.write("\r\n".getBytes());
		    	
		    }
		    out.close();
		    
		    FileOutputStream out1 = new FileOutputStream("datasetRec.txt" );
			Random rand1 = new Random();
			int lineLens1 = 6000000;
			int x1 = 0;
			int y1 = 0;
			int h = 0;
			int w = 0;
			
			for(int i = 1; i <= lineLens1; i++) {
				
				// bottomLeft_x
				x1 = 1 + rand1.nextInt(10000);
				
				// bottomRight_y
				y1 = 1 + rand1.nextInt(10000);
				
				// h
				h = rand1.nextInt(20)+1;
				
				// w
				w = rand1.nextInt(5)+1;
				
				// write <bottomLeft_x, bottomRight_y, h, w>
				out1.write((x1 + "," + y1 + "," + h + "," + w).getBytes());
				
		    	// switch to the next line
		    	out1.write("\r\n".getBytes());
		    	
			}  
		    out1.close();
		    
		}catch(IOException e){
			System.out.print("Exception");
		}
	
	}

}
