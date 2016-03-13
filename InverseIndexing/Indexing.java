package gaurav.hadoop;

import java.io.*;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;
import java.util.StringTokenizer;

public class Indexing {
	public static HashMap <String,String> dict = new HashMap<String,String>();
	public static void Mapper()throws IOException {
		FileInputStream file=new FileInputStream("data.txt");
		BufferedReader br=new BufferedReader(new InputStreamReader(file));
		String line="";
		String comment="";
		int id=0;
		while((line=br.readLine())!=null) {
			line=line.trim();
			if(line.equals("<post>")==true)
				continue;
			else if(line.equals("<id>")==true) {
				line=br.readLine();
				id=Integer.parseInt(line.trim());
				line=br.readLine();
			}
			else if(line.equals("<comment>")==true) {
				line=br.readLine();
				comment=line.trim();
				line=br.readLine();
			}
			else if(line.equals("</post>")==true) {
				StringTokenizer st=new StringTokenizer(comment);
				while(st.hasMoreTokens()) 	{
					String word=st.nextToken();
					Reducer(word.toLowerCase(),id);
				}
			}
		}
		br.close();
	}
	public static void Reducer(String key,int value) {
		if(key.endsWith(".")==true)
			key=key.substring(0,key.length()-1);
		String currentValue=dict.get(key);
		String newValue="";
		if(currentValue!=null)
			newValue=currentValue+","+value;
		else
			newValue=String.valueOf(value);
		dict.put(key, newValue);
		/*for(Map.Entry<String, String> entry : dict.entrySet()){
			System.out.println(entry.getKey());
		}*/
		
	}
	
	public static void main(String args[])throws IOException {
		Mapper();
		FileOutputStream file=new FileOutputStream("result.txt");
		BufferedWriter bw=new BufferedWriter(new OutputStreamWriter(file));
		for(Map.Entry<String, String> entry : dict.entrySet()) {
			String key=entry.getKey();
			String value=entry.getValue();
			bw.write(key+" : "+value);
			bw.newLine();
		}
		bw.close();
	}
	
}
