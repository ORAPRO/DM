package com.dm.util;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.Properties;

public class PropertyReader {
	
	private static PropertyReader propertyReader=null;
	
	private PropertyReader()
	{
		
	}
	
	public static PropertyReader getPropertyReader()
	{
		if(propertyReader==null)
		{
			propertyReader=new PropertyReader();
		}
		
		return propertyReader;
	}
	
	
  Properties prop = null;
  public void loadProperties(final String fileName) {
	
	  prop=new Properties();
	InputStream is = null;

	try {

		is = new FileInputStream(fileName);
		prop.load(is);

	} catch (IOException io) {
		io.printStackTrace();
	} finally {
		if (is != null) {
			try {
				is.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

	}
  }
  
  public String getProperty(final String key)
  {
	  if(prop!=null)
	  {
		  return prop.getProperty(key);
	  }
	  
	  return null;
  }
  
  void printPropeties()
  {
	  if(prop!=null)
	  {
		  for (Map.Entry<Object, Object> p: prop.entrySet()) {
			  System.out.println(p.getKey()+"=="+p.getValue());
			
		}
	  }
  }
  
  public static void main(String[] args) {
	
	  final String fileName="DM.properties";
	  PropertyReader reader=PropertyReader.getPropertyReader();
	  reader.loadProperties(fileName);
	  reader.printPropeties();
}
}