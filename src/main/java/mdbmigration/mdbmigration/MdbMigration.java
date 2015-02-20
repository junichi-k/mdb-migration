package mdbmigration.mdbmigration;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.healthmarketscience.jackcess.Column;
import com.healthmarketscience.jackcess.Database;
import com.healthmarketscience.jackcess.Table;

public class MdbMigration {
	private Database mdb;
	private String hostName;
	private String port;
	private String databaseName;
	private String schema;
	private String user;
	private String pass;
	private Map<String, TableMapping> convertTableMap = new HashMap<String, TableMapping>();
	
	
	public MdbMigration(String filePath, String hostName, String port, String databaseName, String schema, String user, String pass) throws IOException{
		this.mdb = Database.open(new File(filePath));
		this.hostName = hostName;
		this.port = port;
		this.databaseName = databaseName;
		this.schema = schema;
		this.user = user;
		this.pass = pass;
		this.targetTableNames = new ArrayList<String>();
	}
	
	public MdbMigration(String filePath, String hostName, String port, String databaseName, String schema, String user, String pass, List<TableMapping> tableMappings) throws IOException{
		this(filePath, hostName, port, databaseName, schema, user, pass);
		for(TableMapping tableMapping : tableMappings){
			this.convertTableMap.put(tableMapping.getOriginTableName(), tableMapping);
		}
	}
	
	private List<String> targetTableNames;
	public void setTargetTableNames(List<String> targetTableNames){
		this.targetTableNames = targetTableNames;
	}
	
	private boolean onlyTableSchema = false;
	public void setOnlyTableSchema(boolean onlyTableSchema){
		this.onlyTableSchema = onlyTableSchema;
	}
	
	private Set<String> excludedTable = new HashSet<String>();
	public void setExcludedTable(Set<String> excludedTable){
		this.excludedTable = excludedTable;
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
		Set<String> tableNames = getTableNames();
		for(String tableName : tableNames){
			if(targetTableNames.size() != 0 && !targetTableNames.contains(tableName)){
				continue;
			}
			if(excludedTable.size() != 0 && excludedTable.contains(tableName)){
				continue;
			}
			if(convertTableMap.containsKey(tableName)){
				tableName = convertTableMap.get(tableName).getMigrateTableName();
			}
			StringBuilder sb = new StringBuilder();
			sb.append("DROP TABLE IF EXISTS ");
			sb.append(tableName);
			sb.append(";");
			System.out.println(sb.toString());
			Statement st = con.createStatement();
			st.execute(sb.toString());
		}
	}
	
	public Set<String> getTableNames(){
		return mdb.getTableNames();
	}
	
	public Table getTable(String tableName) throws IOException{
		return mdb.getTable(tableName);
	}
	
	public List<Column> getColumns(String tableName) throws IOException{
		if(tableName != null){
			Table table = getTable(tableName);
			if(table != null){
				return table.getColumns();
			}
		}
		return null;
	}
	
	private void openMdb(){
		try {
			Set<String> tableNames = getTableNames();
			for(String tableName : tableNames){
				if(targetTableNames.size() != 0 && !targetTableNames.contains(tableName)){
					continue;
				}
				if(excludedTable.size() != 0 && excludedTable.contains(tableName)){
					continue;
				}
				Table table = getTable(tableName);
				List<Column> columns = table.getColumns();
				MigrateEntity me = new MigrateEntity();
				TableMapping tableMapping = convertTableMap.get(tableName);
				if(tableMapping != null){
					me.setTableName(tableMapping.getMigrateTableName());
				}else{
					me.setTableName(tableName);
				}
				for(Column c : columns){
					String columnName = c.getName();
					String dataTypeName = PgDataType.getPgType(c.getType()).type;
					if(tableMapping != null){
						if(tableMapping.containsOriginColumnName(c.getName())){
							columnName = tableMapping.getMigrateColumnName(c.getName());
						}
						if(tableMapping.containsMigrateDataType(c.getName())){
							dataTypeName = tableMapping.getDataType(c.getName());
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
				if(onlyTableSchema){
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
					PreparedStatement ps = con.prepareStatement(me.getPreparedStatementSql());
					try {
						ps = me.applyPreparedStatement(ps);
						ps.executeUpdate();
						System.out.println("success:" + ps);
						ps.clearParameters();
					} catch (SQLException e1) {
						System.out.println("fail:" + ps);
					}
				}
			}
			System.out.println("END");
		} catch (IOException e) {
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
	
}
