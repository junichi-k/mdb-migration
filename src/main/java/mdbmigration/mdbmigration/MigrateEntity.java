package mdbmigration.mdbmigration;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.commons.lang3.StringUtils;

public class MigrateEntity {
	private String tableName;
	public void setTableName(String tableName){
		this.tableName = tableName;
	}
	
	/**
	 * key: カラム名
	 * value: データ型
	 */
	private Map<String, String> columnNameMap = new LinkedHashMap<String, String>();
	public void setColumNameMap(String columnName, String dataTypeName){
		columnNameMap.put(columnName, dataTypeName);
	}
	
	public String getCreateTableSQL(){
		StringBuilder sb = new StringBuilder();
		sb.append("CREATE TABLE ");
		sb.append(tableName);
		sb.append("(");
		List<String> columnDef = new ArrayList<String>();
		for(Entry<String, String> e : columnNameMap.entrySet()){
			columnDef.add(e.getKey() + " " + e.getValue());
		}
		sb.append(StringUtils.join(columnDef, ','));
		sb.append(")");
		return sb.toString();
	}

	/**
	 * key: カラム名
	 * value: 値
	 */
	private Map<String, String> dataMap = new TreeMap<String, String>();
	public void clearData(){
		dataMap = new HashMap<String, String>();
	}
	
	public void setDataMap(String columnName, String value){
		dataMap.put(columnName, value);
	}
	
	public String getInsertSQL(){
		StringBuilder sb = new StringBuilder();
		sb.append("INSERT INTO ");
		sb.append(tableName);
		sb.append("(");
		List<String> columnDef = new ArrayList<String>();
		for(Entry<String, String> e : columnNameMap.entrySet()){
			if(dataMap.containsKey(e.getKey())){
				columnDef.add(e.getKey());
			}
		}
		sb.append(StringUtils.join(columnDef, ','));
		sb.append(")VALUES(");
		
		List<String> dataDef = new ArrayList<String>();
		for(Entry<String, String> e : columnNameMap.entrySet()){
			if(dataMap.containsKey(e.getKey())){
				if(dataMap.get(e.getKey()) != null){
					dataDef.add("'" + dataMap.get(e.getKey()) + "'");
				}else{
					dataDef.add("null");
				}
			}
		}
		sb.append(StringUtils.join(dataDef, ','));
		sb.append(");");
		return sb.toString();
	}
}
