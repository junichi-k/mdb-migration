package mdbmigration.mdbmigration;

import java.util.Map;

public abstract class TableMapping {
	public abstract String getOriginTableName();
	public abstract String getMigrateTableName();
	
	/**
	 * key:元カラム名
	 * value:移行先カラム名
	 */
	protected abstract Map<String, String> getColumnMap();
	public boolean containsOriginColumnName(String originColumnName){
		if(getColumnMap() == null){
			return false;
		}
		return getColumnMap().containsKey(originColumnName);
	}
	public String getMigrateColumnName(String originColumnName){
		if(getColumnMap() == null){
			return null;
		}
		return getColumnMap().get(originColumnName);
	}
	
	/**
	 * key:元カラムの型
	 * value:移行先カラムの型
	 */
	protected abstract Map<String, String> getDataTypeMap();
	public boolean containsMigrateDataType(String originColumnName){
		if(getDataTypeMap() == null){
			return false;
		}
		return getDataTypeMap().containsKey(originColumnName);
	}
	public String getData(String originColumnName){
		if(getDataTypeMap() == null){
			return null;
		}
		return getDataTypeMap().get(originColumnName);
	}
	
	/**
	 * key:移行先カラム
	 * value:変換値
	 */
	protected abstract Map<String, String> getValueMap();
	
	public boolean containsMigrateValue(String migrateColumnName){
		if(getValueMap() == null){
			return false;
		}
		return getValueMap().containsKey(migrateColumnName);
	}
	public String getValue(String migrateColumnName){
		if(getValueMap() == null){
			return null;
		}
		return getValueMap().get(migrateColumnName);
	}
}
