package com.db2;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.PriorityQueue;
import java.util.Queue;

public class mains {
	public static HashMap<Integer, Transaction> transMap = new HashMap<Integer, Transaction>();
	public static HashMap<String, LockTable> lockMap = new HashMap<String, LockTable>();
	public static Queue<Integer> q = new PriorityQueue<Integer>();
	public static String[] data = new String[20];
	public static LinkedList<String> waitTransactionList = new LinkedList<String>();

	public static void main(String args[]) {

		mains obj1 = new mains();
		data = obj1.ReadFile();
		System.out.println("Implementation of Rigorous 2PL");
		TransactionProcess process = new TransactionProcess();
		process.readTransactions();

	}

	public String[] ReadFile() {

		// reading file line by line in Java using BufferedReader
		FileInputStream fis = null;
		BufferedReader reader = null;
		String[] myarray;
		myarray = new String[20];
		try {
//			fis = new FileInputStream("C:/Users/Luv/Desktop/input1.txt");
			fis = new FileInputStream("C:/Users/Luv/Desktop/input2.txt.txt");
			reader = new BufferedReader(new InputStreamReader(fis));
			int i = 0;
			String line = reader.readLine();

			while (line != null) {
				myarray[i] = line;
				line = reader.readLine();
				System.out.println(myarray[i]);
				i++;
			}
			reader.close();
		}

		catch (Exception e) {
			System.out.println("Error");
		}
		return myarray;
	}
}
