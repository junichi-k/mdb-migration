package mdbmigration.mdbmigration;

import java.util.HashMap;
import java.util.Map;

public abstract class TableMapping {
	public abstract String getOriginTableName();
	public abstract String getMigrateTableName();
	
	/**
	 * key:元カラム名
	 * value:移行先カラム名
	 */
	protected Map<String, String> columnMap = new HashMap<String, String>();
	public boolean containsOriginColumnName(String originColumnName){
		return columnMap.containsKey(originColumnName);
	}
	public String getMigrateColumnName(String originColumnName){
		return columnMap.get(originColumnName);
	}
	
	/**
	 * key:元カラムの型
	 * value:移行先カラムの型
	 */
	protected Map<String, String> dataTypeMap = new HashMap<String, String>();
	public boolean containsMigrateDataType(String originColumnName){
		return dataTypeMap.containsKey(originColumnName);
	}
	public String getDataType(String originColumnName){
		return dataTypeMap.get(originColumnName);
	}
}
