package mdbmigration.mdbmigration;

import java.io.ByteArrayInputStream;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.commons.lang3.StringUtils;

public class MigrateEntity {
	private String tableName;
	public void setTableName(String tableName){
		this.tableName = tableName;
	}
	
	private TableMapping tableMapping;
	public void setTableMapping(TableMapping tableMapping){
		this.tableMapping = tableMapping;
	}
	
	/**
	 * key: カラム名(移行後のカラム名)
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
	 * key: カラム名(移行元のカラム名)
	 * value: 値
	 */
	private Map<String, String> dataMap = new TreeMap<String, String>();
	public void clearData(){
		dataMap = new HashMap<String, String>();
	}
	
	public void setDataMap(String columnName, String value){
		dataMap.put(columnName, value);
	}
	
	public String getPreparedStatementSql(){
		StringBuilder sb = new StringBuilder();
		sb.append("INSERT INTO ");
		sb.append(tableName);
		sb.append("(");
		List<String> columnDef = new ArrayList<String>();
		for(Entry<String, String> e : columnNameMap.entrySet()){
			columnDef.add(e.getKey());
		}
		sb.append(StringUtils.join(columnDef, ','));
		sb.append(")VALUES(");
		List<String> dataDef = new ArrayList<String>();
		for(Entry<String, String> e : columnNameMap.entrySet()){
			dataDef.add("?");
		}
		sb.append(StringUtils.join(dataDef, ','));
		sb.append(")");
		return sb.toString();
	}
	
	public PreparedStatement applyPreparedStatement(PreparedStatement ps) throws SQLException{
		int index = 1;
		for(Entry<String, String> e : columnNameMap.entrySet()){
			if(tableMapping != null && tableMapping.containsMigrateValue(e.getKey())){
				setValue(ps, index, e.getValue(), tableMapping.getValueMap(e.getKey()));
				index++;
				continue;
			}
			if(dataMap.containsKey(e.getKey())){
				setValue(ps, index, e.getValue(), dataMap.get(e.getKey()));
			}else{
				setValue(ps, index, e.getValue(), null);
			}
			index++;
		}
		return ps;
	}
	
	private SimpleDateFormat sdf = new SimpleDateFormat("EEE MMM dd HH:mm:ss z yyyy", Locale.US);
	private SimpleDateFormat convertSdf = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
	
	/**
	 * 型を判断して値をPreparedStatementにセットする
	 * @param ps
	 * @param index
	 * @param type
	 * @param value 空はnullとして扱う
	 * @throws SQLException
	 */
	private void setValue(PreparedStatement ps, int index, String type, String value) throws SQLException{
		PgDataType pgDataType = PgDataType.getPgType(type);
		if(pgDataType == null){
			throw new RuntimeException("型が見つかりません");
		}
		if(StringUtils.isEmpty(value)){
			ps.setNull(index, pgDataType.sqlType);
			return;
		}
		switch(pgDataType){
		case BOOLEAN:
			ps.setBoolean(index, value.toLowerCase().equals("true"));
			break;
		case BYTE:
			ps.setByte(index, Byte.valueOf(value));
			break;
		case INT:
		case GUID:
			ps.setInt(index, Integer.parseInt(value));
			break;
		case LONG:
		case MONEY:
		case NUMERIC:
			ps.setLong(index, Long.parseLong(value));
			break;
		case FLOAT:
			ps.setFloat(index, Float.parseFloat(value));
			break;
		case DOUBLE:
			ps.setDouble(index, Double.parseDouble(value));
			break;
		case SHORT_DATE_TIME:
			try {
				Date d = sdf.parse(value);
				ps.setTimestamp(index, Timestamp.valueOf(convertSdf.format(d)));
			} catch (ParseException e) {
				throw new RuntimeException(value + ":Timestampに変換できません");
			}
			break;
		case BINARY:
		case OLE:
			ps.setBinaryStream(index, new ByteArrayInputStream(value.getBytes()));
			break;
		case TEXT:
		case MEMO:
		case UNKNOWN_0D:
		case UNKNOWN_11:
			ps.setString(index, value);
			break;
		}
	}
}
