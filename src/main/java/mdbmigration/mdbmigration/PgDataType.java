package mdbmigration.mdbmigration;

import com.healthmarketscience.jackcess.DataType;

public enum PgDataType {
	BOOLEAN(DataType.BOOLEAN, "BOOLEAN"),
	BYTE(DataType.BYTE, "BYTE"),
	INT(DataType.INT, "INT"),
	LONG(DataType.LONG, "BIGINT"),
	MONEY(DataType.MONEY, "BIGINT"),
	FLOAT(DataType.FLOAT, "FLOAT"),
	DOUBLE(DataType.DOUBLE, "DOUBLE PRECISION"),
	SHORT_DATE_TIME(DataType.SHORT_DATE_TIME, "TIMESTAMP"),
	BINARY(DataType.BINARY, "BINARY"),
	TEXT(DataType.TEXT, "TEXT"),
	OLE(DataType.OLE, "BINARY"),
	MEMO(DataType.MEMO, "TEXT"),
	UNKNOWN_0D(DataType.UNKNOWN_0D, "TEXT"),
	GUID(DataType.GUID, "INT"),
	NUMERIC(DataType.NUMERIC, "BIGINT"),
	UNKNOWN_11(DataType.UNKNOWN_11, "TEXT"),
	;
	
	public DataType mdbType;
	public String type;
	PgDataType(DataType mdbType, String type){
		this.mdbType = mdbType;
		this.type = type;
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
