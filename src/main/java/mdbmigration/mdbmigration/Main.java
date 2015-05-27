package mdbmigration.mdbmigration;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.healthmarketscience.jackcess.Database;
import com.healthmarketscience.jackcess.DatabaseBuilder;

public class Main {

	public static void main(String[] args){
		if(args.length == 1 && args[0].toLowerCase().equals("help")){
			for(ArgsType argType : ArgsType.values()){
				System.out.println(argType.argName + ":\t" + argType.help);
			}
			return;
		}
		Map<String, String> argMap = new HashMap<String, String>();
		for(String a : args){
			List<String> params = Arrays.asList(a.split("="));
			if(params.size() > 1){
				argMap.put(params.get(0), params.get(1));
			}else{
				throw new RuntimeException("引数:" + params.get(0) + "の値がセットされていません");
			}
		}
		
		String filePath = "";
		if(argMap.containsKey(ArgsType.FILE_PATH.argName)){
			filePath = argMap.get(ArgsType.FILE_PATH.argName);
		}else{
			throw new RuntimeException(ArgsType.FILE_PATH.argName + "が指定されていません");
		}
		String hostName = "";
		if(argMap.containsKey(ArgsType.HOST_NAME.argName)){
			hostName = argMap.get(ArgsType.HOST_NAME.argName);
		}else{
			throw new RuntimeException(ArgsType.HOST_NAME.argName + "が指定されていません");
		}
		String port = "5432";
		if(argMap.containsKey(ArgsType.PORT.argName)){
			port = argMap.get(ArgsType.PORT.argName);
		}
		String databaseName = "";
		if(argMap.containsKey(ArgsType.DATABASE_NAME.argName)){
			databaseName = argMap.get(ArgsType.DATABASE_NAME.argName);
		}else{
			throw new RuntimeException(ArgsType.DATABASE_NAME.argName + "が指定されていません");
		}
		String schema = "public";
		if(argMap.containsKey(ArgsType.SCHEMA.argName)){
			schema = argMap.get(ArgsType.SCHEMA.argName);
		}
		String user = "";
		if(argMap.containsKey(ArgsType.USER.argName)){
			user = argMap.get(ArgsType.USER.argName);
		}else{
			throw new RuntimeException(ArgsType.USER.argName + "が指定されていません");
		}
		String pass = "";
		if(argMap.containsKey(ArgsType.PASS.argName)){
			user = argMap.get(ArgsType.PASS.argName);
		}
		List<String> targetTableNames = new ArrayList<String>();
		if(argMap.containsKey(ArgsType.TABLE.argName)){
			String tableNameArg = argMap.get(ArgsType.TABLE.argName);
			String[] tableNames = tableNameArg.split(",");
			targetTableNames = Arrays.asList(tableNames);
		}
		Set<String> excludedTableNames = new HashSet<String>();
		if(argMap.containsKey(ArgsType.EXCLUDED_TABLE.argName)){
			String tableNameArg = argMap.get(ArgsType.EXCLUDED_TABLE.argName);
			String[] tableNames = tableNameArg.split(",");
			excludedTableNames = new HashSet<String>(Arrays.asList(tableNames));
		}
		MdbMigration mdbMigration = null;
		Database mdb = null;
		try {
			mdb = DatabaseBuilder.open(new File(filePath));
			mdbMigration = new MdbMigration(mdb, hostName, port, databaseName, schema, user, pass);
			mdbMigration.setTargetTableNames(targetTableNames);
			mdbMigration.setExcludedTable(excludedTableNames);
			mdbMigration.execute();
		} catch (IOException e1) {
			e1.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		} finally{
			if(mdb != null){
				try {
					mdb.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}
	
	public enum ArgsType{
		FILE_PATH("filePath", "mdbファイルのパス"),
		HOST_NAME("hostName", "postgresqlのホスト名"),
		PORT("port", "postgresqlのポート番号(デフォルト5432)"),
		DATABASE_NAME("databaseName", "postgresqlのデータベース名"),
		SCHEMA("schema", "テーブルのスキーマ"),
		USER("user", "postgresqlのユーザー名"),
		PASS("pass", "postgresqlのパスワード"),
		TABLE("table", "指定したテーブルのみ移行する(カンマ区切りで複数指定可能)"),
		EXCLUDED_TABLE("excluded", "指定したテーブルは移行しない(カンマ区切りで複数指定可能)");
		
		public String argName;
		public String help;
		ArgsType(String argName, String help){
			this.argName = argName;
			this.help = help;
		}
	}
}
