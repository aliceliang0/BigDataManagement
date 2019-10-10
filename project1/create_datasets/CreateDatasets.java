package project1;

import java.util.Random;
import java.io.*;

public class CreateDatasets {

	public static void main(String[] args) {
		try{
			FileOutputStream out = new FileOutputStream("customer.txt" );
			Random rand = new Random();
			int numberCustomer = 50000;
		    
			for(int i = 1; i <= numberCustomer; i++) {
		    	// create the ID of customers 
		    	out.write((i + "").getBytes());
		    	out.write(",".getBytes());
		    	
		    	// create the Name of customers 
		    	int length = 10 + rand.nextInt(11); 
		    	StringBuffer customer = new StringBuffer();
		    	String str="abcdefghijklmnopqrstuvwxyz";
		    	for(int j = 1; j <= length; j++) {
		    		int number = rand.nextInt(26);
		    		customer.append(str.charAt(number));
		    	}
		    	out.write((customer + "").getBytes());
		    	out.write(",".getBytes());
		    	
		    	// create the Age of customers 
		    	int age = 10 + rand.nextInt(61); 
		    	out.write((age + "").getBytes());
		    	out.write(",".getBytes());
		    	
		    	// create the Gender of customers
		        int genderCode = rand.nextInt(2); // if genderCode = 0, then the person is "male", if genderCode = 1, then the person is "female"
		        if(genderCode == 0) out.write(("male").getBytes()); 
		        else out.write(("female").getBytes());
		    	out.write(",".getBytes());
		        
		    	// create the CountryCode of customers
		    	int countryCode = 1 + rand.nextInt(10);
		    	out.write((countryCode + "").getBytes());
		    	out.write(",".getBytes());
		    	
		    	// create the Salary of customers 
		    	float salary = 100 + (10000 - 100) * rand.nextFloat();
		    	out.write((salary + "").getBytes());
		    	
		    	// switch to the next line
		    	out.write("\r\n".getBytes());
		    	
		    	
		    }
		    out.close();
		    
		    FileOutputStream out1 = new FileOutputStream("transactions.txt" );
			Random rand1 = new Random();
			int numberTransaction = 5000000;
			
			for(int i = 1; i <= numberTransaction; i++) {
				// create TransID
				out1.write((i + "").getBytes());
		    	out1.write(",".getBytes());
				
		    	// create custID
		    	int custID = 1 + rand1.nextInt(50000);
		    	out1.write((custID + "").getBytes());
		    	out1.write(",".getBytes());
		    	
		    	// create TransTotal
		    	float transTotal = 10 + (1000 - 10) * rand1.nextFloat();
		    	out1.write((transTotal + "").getBytes());
		    	out1.write(",".getBytes());
		    	
		    	// create TransNumItems
		    	int transNumItems = 1 + rand1.nextInt(10);
		    	out1.write((transNumItems + "").getBytes());
		    	out1.write(",".getBytes());
		    	
		    	// create TransDesc
		    	int length1 = 20 + rand.nextInt(31);
		    	StringBuffer trans = new StringBuffer();
		    	String str1="abcdefghijklmnopqrstuvwxyz";
		    	for(int j = 1; j <= length1; j++) {
		    		int number = rand.nextInt(26);
		    		trans.append(str1.charAt(number));
		    	}
		    	out1.write((trans + "").getBytes());
		    	out1.write(",".getBytes());	
		    	
		    	// switch to the next line
		    	out1.write("\r\n".getBytes());
			}  
		    out1.close();
		    
		}catch(IOException e){
			System.out.print("Exception");
		}
	
	}

}
