package mdbmigration.mdbmigration;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
		
		MdbMigration mdbMigration = new MdbMigration(filePath, hostName, port, databaseName, schema, user, pass);
		try {
			mdbMigration.execute();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public enum ArgsType{
		FILE_PATH("filePath", "mdbファイルのパス"),
		HOST_NAME("hostName", "postgresqlのホスト名"),
		PORT("port", "postgresqlのポート番号(デフォルト5432)"),
		DATABASE_NAME("databaseName", "postgresqlのデータベース名"),
		SCHEMA("schema", "テーブルのスキーマ"),
		USER("user", "postgresqlのユーザー名"),
		PASS("pass", "postgresqlのパスワード"),;
		
		public String argName;
		public String help;
		ArgsType(String argName, String help){
			this.argName = argName;
			this.help = help;
		}
	}
}
