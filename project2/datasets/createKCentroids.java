package project2;

import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Random;

public class createKCentroids {

	public static void main(String[] args) {
		try {
			FileOutputStream out = new FileOutputStream("kCentroids.txt" );
			Random rand = new Random();
			int k = 10 + rand.nextInt(91);
			float x = 0.0f;
			float y = 0.0f;
			
			for(int i = 1; i <= k; i++) {
				
				x = 1 + (10000-1) * rand.nextFloat();
				y = 1 + (10000-1) * rand.nextFloat();
				
				out.write((x+","+y).getBytes());
				
				out.write("\r\n".getBytes());
			}
			out.close();
			
		}catch(IOException e){
			System.out.print("Exception");
		}
		

	}

}
