package com.db2;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

public class mains {
	public static HashMap<Integer, Transaction> transMap = new HashMap<Integer, Transaction>();
	public static HashMap<String, LockTable> lockMap = new HashMap<String, LockTable>();
	public static String[] data = null;
	public static LinkedList<String> waitTransactionList = new LinkedList<String>();

	public static void main(String args[]) throws IOException {

		mains obj1 = new mains();
		data = obj1.ReadFile();
		System.out.println("Implementation of Rigorous 2PL");
		TransactionProcess process = new TransactionProcess();
		process.readTransactions();
		System.exit(0);

	}

	public String[] ReadFile() throws IOException {
		BufferedReader in = new BufferedReader(new FileReader("C:/Users/Luv/Desktop/input7.txt"));
		String str=null;
		ArrayList<String> lines = new ArrayList<String>();
		while((str = in.readLine()) != null){
		    lines.add(str.replaceAll("\\s",""));
		}
		String[] linesArray = lines.toArray(new String[lines.size()]);
		
		return linesArray;
	}
}
