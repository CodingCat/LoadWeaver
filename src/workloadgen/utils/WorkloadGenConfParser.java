package workloadgen.utils;

import java.util.HashMap;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

public class WorkloadGenConfParser {
	private static WorkloadGenConfParser _instance = null;
	private HashMap<String, String> propertyMap = null;
	private String confPath = null;
	
	public WorkloadGenConfParser(String path){
		this.confPath = path;
		propertyMap = new HashMap<String, String>();
		parse();
	}
	
	/**
	 * parsing operation
	 */
	private void parse() {
		try {
			DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
			factory.setIgnoringElementContentWhitespace(true);
			DocumentBuilder builder;
			builder = factory.newDocumentBuilder();
			Document doc = builder.parse(confPath);
			Element configurationRoot = doc.getDocumentElement();
			NodeList propertyList = configurationRoot.getChildNodes();
			for (int i = 0; i < propertyList.getLength(); i++) {
				Node property = propertyList.item(i);
				Element eleProperty = (Element) property;
				propertyMap.put(getTagValue("name", eleProperty),
						getTagValue("value", eleProperty));
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * get the value with String type 
	 * @param tagName, the property name
	 * @param ele, the element
	 * @return the value
	 */
	private String getTagValue(String tagName, Element ele){
		NodeList nlList = ele.getElementsByTagName(tagName).item(0).getChildNodes();
		Node nValue = (Node) nlList.item(0);
		return nValue.getNodeValue();
	}
	
	/**
	 * get the instance of WorkloadGenConfParser
	 * @param confPath, the path of configuration file 
	 * @return the newly constructed or existed instance
	 */
	static WorkloadGenConfParser Instance(String confPath){
		if (_instance == null){
			_instance = new WorkloadGenConfParser(confPath);
		}
		return _instance;
	}
	
	/**
	 * return the string value from the configuration file
	 * @param propertyName the propertyName 
	 * @param defaultValue if propertyName hasn't been set, return defaultValue
	 * @return propertyMap[propertyName] or defaultValue
	 */
	public String getString(String propertyName, String defaultValue){
		if (propertyMap.containsKey(propertyName)){
			return propertyMap.get(propertyName);
		}
		else{
			return defaultValue;
		}
	}
	
	/**
	 * return the int value from the configuration file
	 * @param propertyName: the propertyName 
	 * @param defaultValue: if propertyName hasn't been set, return defaultValue
	 * @return propertyMap[propertyName] or defaultValue
	 */
	public int getInt(String propertyName, int defaultValue){
		if (propertyMap.containsKey(propertyName)){
			return Integer.parseInt((propertyMap.get(propertyName)));
		}
		else{
			return defaultValue;
		}
	}
	
	/**
	 * return the bool value from the configuration file
	 * @param propertyName the propertyName 
	 * @param defaultValue if propertyName hasn't been set, return defaultValue
	 * @return propertyMap[propertyName] or defaultValue
	 */
	public boolean getBoolean(String propertyName, boolean defaultValue){
		if (propertyMap.containsKey(propertyName)){
			return Boolean.parseBoolean(propertyMap.get(propertyName));
		}
		else{
			return defaultValue;
		}
	}
	
	
	/**
	 * return the double value from the configuration file
	 * @param propertyName the propertyName 
	 * @param defaultValue if propertyName hasn't been set, return defaultValue
	 * @return propertyMap[propertyName] or defaultValue
	 */
	public double getDouble(String propertyName, double defaultValue){
		if (propertyMap.containsKey(propertyName)){
			return Double.parseDouble(propertyMap.get(propertyName));
		}
		else{
			return defaultValue;
		}
	}
	
}
