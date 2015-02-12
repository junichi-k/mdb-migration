package mdbmigration.mdbmigration;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.healthmarketscience.jackcess.Column;
import com.healthmarketscience.jackcess.Database;
import com.healthmarketscience.jackcess.Table;

public class MdbMigration {
	private String filePath;
	private String hostName;
	private String port;
	private String databaseName;
	private String schema;
	private String user;
	private String pass;
	private Map<String, TableMapping> convertTableMap = new HashMap<String, TableMapping>();
	
	public MdbMigration(String filePath, String hostName, String port, String databaseName, String schema, String user, String pass){
		this.filePath = filePath;
		this.hostName = hostName;
		this.port = port;
		this.databaseName = databaseName;
		this.schema = schema;
		this.user = user;
		this.pass = pass;
	}
	
	public MdbMigration(String filePath, String hostName, String port, String databaseName, String schema, String user, String pass, List<TableMapping> tableMappings){
		this.filePath = filePath;
		this.hostName = hostName;
		this.port = port;
		this.databaseName = databaseName;
		this.schema = schema;
		this.user = user;
		this.pass = pass;
		for(TableMapping tableMapping : tableMappings){
			this.convertTableMap.put(tableMapping.getOriginTableName(), tableMapping);
		}
	}
	
	private Connection con = null;
	public void execute() throws ClassNotFoundException, SQLException{
		Class.forName("org.postgresql.Driver");
		
		try {
			String url = "jdbc:postgresql://" + hostName + ":" + port + "/" + databaseName;
			System.out.println(url);
			con = DriverManager.getConnection(url, user, pass);
			migration();
		} catch (SQLException e) {
			e.printStackTrace();
		} finally{
			if(con != null){
				con.close();
			}
		}
	}
	
	private void migration() throws SQLException{
		Statement statement = con.createStatement();
		statement.execute("SET SEARCH_PATH TO '" + schema + "'");
		allDropTable();
		openMdb();
	}
	
	private void allDropTable() throws SQLException{
		String sql = "select * from pg_tables where tablename not like 'pg%' and tablename not like 'sql_%'";
		Statement statement = con.createStatement();
		ResultSet rs = statement.executeQuery(sql);
		while(rs.next()){
			StringBuilder sb = new StringBuilder();
			sb.append("DROP TABLE IF EXISTS ");
			sb.append(rs.getString(2));
			sb.append(";");
			System.out.println(sb.toString());
			Statement st = con.createStatement();
			st.execute(sb.toString());
		}
	}
	
	private void openMdb(){
		Database mdb = null;
		try {
			mdb = Database.open(new File(filePath));
			Set<String> tableNames = mdb.getTableNames();
			for(String tableName : tableNames){
				Table table = mdb.getTable(tableName);
				List<Column> columns = table.getColumns();
				
				MigrateEntity me = new MigrateEntity();
				if(convertTableMap.containsKey(tableName)){
					me.setTableName(convertTableMap.get(tableName).getMigrateTableName());
				}else{
					me.setTableName(tableName);
				}
				for(Column c : columns){
					String columnName = c.getName();
					String dataTypeName = PgDataType.getPgType(c.getType()).type;
					if(convertTableMap.containsKey(tableName)){
						if(convertTableMap.get(tableName).containsOriginColumnName(c.getName())){
							columnName = convertTableMap.get(tableName).getMigrateColumnName(c.getName());
						}
						if(convertTableMap.get(tableName).containsMigrateDataType(c.getName())){
							dataTypeName = convertTableMap.get(tableName).getDataType(c.getName());
						}
					}
					me.setColumNameMap(columnName, dataTypeName);
				}
				try {
					Statement statement = con.createStatement();
					statement.execute(me.getCreateTableSQL());
					System.out.println("success:" + me.getCreateTableSQL());
				} catch (SQLException e1) {
					System.out.println("fail:" + me.getCreateTableSQL());
					// CREATE TABLEに失敗したらINSERT SQLは実行しないためcontinue
					continue;
				}
				
				Iterator<Map<String, Object>> rowIterator = table.iterator();
				while(rowIterator.hasNext()){
					Map<String, Object> row = rowIterator.next();
					me.clearData();
					for(Entry<String, Object> e : row.entrySet()){
						if(e.getValue() != null){
							me.setDataMap(e.getKey(), e.getValue().toString());
						}else{
							me.setDataMap(e.getKey(), null);
						}
					}
					try {
						Statement statement = con.createStatement();
						statement.execute(me.getInsertSQL());
						System.out.println("success:" + me.getInsertSQL());
					} catch (SQLException e1) {
						System.out.println("fail:" + me.getInsertSQL());
					}
				}
			}
			System.out.println("END");
		} catch (IOException e) {
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
	
}
