package mdbmigration.mdbmigration;

import java.sql.Types;

import com.healthmarketscience.jackcess.DataType;

public enum PgDataType {
	BOOLEAN(DataType.BOOLEAN, "BOOLEAN", Types.BOOLEAN),
	BYTE(DataType.BYTE, "BYTE", Types.BIT),
	INT(DataType.INT, "INT", Types.INTEGER),
	LONG(DataType.LONG, "BIGINT", Types.BIGINT),
	MONEY(DataType.MONEY, "BIGINT", Types.BIGINT),
	FLOAT(DataType.FLOAT, "FLOAT", Types.FLOAT),
	DOUBLE(DataType.DOUBLE, "DOUBLE PRECISION", Types.DOUBLE),
	SHORT_DATE_TIME(DataType.SHORT_DATE_TIME, "TIMESTAMP", Types.TIMESTAMP),
	BINARY(DataType.BINARY, "BINARY", Types.BINARY),
	TEXT(DataType.TEXT, "TEXT", Types.BLOB),
	OLE(DataType.OLE, "BINARY", Types.BINARY),
	MEMO(DataType.MEMO, "TEXT", Types.BLOB),
	UNKNOWN_0D(DataType.UNKNOWN_0D, "TEXT", Types.BLOB),
	GUID(DataType.GUID, "INT", Types.INTEGER),
	NUMERIC(DataType.NUMERIC, "BIGINT", Types.BIGINT),
	UNKNOWN_11(DataType.UNKNOWN_11, "TEXT", Types.BLOB),
	;
	
	public DataType mdbType;
	public String type;
	public int sqlType;
	PgDataType(DataType mdbType, String type, int sqlType){
		this.mdbType = mdbType;
		this.type = type;
		this.sqlType = sqlType;
	}
	
	public static PgDataType getPgType(DataType mdbType){
		for(PgDataType pgDataType : PgDataType.values()){
			if(pgDataType.mdbType == mdbType){
				return pgDataType;
			}
		}
		return null;
	}
	
	public static PgDataType getPgType(String type){
		for(PgDataType pgDataType : PgDataType.values()){
			if(pgDataType.type.equals(type)){
				return pgDataType;
			}
		}
		return null;
	}
}
