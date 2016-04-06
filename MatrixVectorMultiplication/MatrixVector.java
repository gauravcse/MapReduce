package gaurav.hadoop;

import java.io.*;
import java.util.*;
import java.util.Map.Entry;

public class MatrixVector {
	static ArrayList<Integer> vector;
	static HashMap<Integer,String> keyvalue = new HashMap<Integer,String>();
	
	public static void Mapper()throws IOException {
		//FileInputStream file = new FileInputStream("input.txt");
		BufferedReader br= new BufferedReader(new InputStreamReader(new FileInputStream("input.txt")));
		String line="";
		while((line=br.readLine()) != null) {
			String array[]=line.split(" ");
			String currentValue=keyvalue.get(Integer.parseInt(array[0]));
			if(currentValue == null )
				keyvalue.put(Integer.parseInt(array[0]),array[2]);
			else {
				currentValue+=" "+array[2];
				keyvalue.put(Integer.parseInt(array[0]),currentValue);
			}
		}
		br.close();	
		Reducer();
	}
	
	public static void Reducer()throws IOException {
		BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("out.txt"))); 
		Set<Map.Entry<Integer, String>> set = keyvalue.entrySet();
		Iterator<Map.Entry<Integer, String>> itr = set.iterator();
		while(itr.hasNext()) {
			Map.Entry<Integer, String> entry = (Entry<Integer, String>)itr.next();
			int key=entry.getKey();
			String value=entry.getValue();
			String elements[]=value.split(" ");
			int output=0;
			for(int i=0;i< elements.length; i++) {
				output+=(Integer.parseInt(elements[i])*vector.get(i));
			}
			bw.write(String.valueOf(key)+" "+String.valueOf(output)+"\n");
		}
		bw.close();
	}
	
	public static void main(String args[])throws IOException,NullPointerException {
		BufferedReader br= new BufferedReader(new InputStreamReader(new FileInputStream("vector.txt")));
		String line="";
		vector= new ArrayList<Integer>();
		while((line=br.readLine())!= null) {
			String arr[]=line.split(" ");
			for(String element: arr) {
				//System.out.print(element+"\t");
				vector.add(Integer.parseInt(element));
			}
		}
		/*vector.add(1);
		vector.add(2);
		vector.add(3);*/
		Mapper();
		//br.close();
	}
}
