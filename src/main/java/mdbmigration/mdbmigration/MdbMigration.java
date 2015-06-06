package mdbmigration.mdbmigration;

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
import java.util.stream.Collectors;

import com.healthmarketscience.jackcess.Column;
import com.healthmarketscience.jackcess.Database;
import com.healthmarketscience.jackcess.Row;
import com.healthmarketscience.jackcess.Table;

public class MdbMigration {
	private String hostName;
	private String port;
	private String databaseName;
	private String schema;
	private String user;
	private String pass;
	private Map<String, TableMapping> convertTableMap = new HashMap<String, TableMapping>();
	
	public MdbMigration(Database mdb, String hostName, String port, String databaseName, String schema, String user, String pass) throws IOException{
		this.tableNames = mdb.getTableNames();
		for(String table : this.tableNames){
			this.tableMap.put(table, mdb.getTable(table));
		}
		this.hostName = hostName;
		this.port = port;
		this.databaseName = databaseName;
		this.schema = schema;
		this.user = user;
		this.pass = pass;
		this.targetTableNames = new HashSet<String>();
	}
	
	public MdbMigration(Database mdb, String hostName, String port, String databaseName, String schema, String user, String pass, List<TableMapping> tableMappings) throws IOException{
		this(mdb, hostName, port, databaseName, schema, user, pass);
		for(TableMapping tableMapping : tableMappings){
			this.convertTableMap.put(tableMapping.getOriginTableName(), tableMapping);
		}
	}
	
	private Set<String> targetTableNames;
	/**
	 * 指定のテーブル名のみ移行するテーブル名をセットする
	 * @param targetTableNames
	 */
	public void setTargetTableNames(Set<String> targetTableNames){
		this.targetTableNames = targetTableNames;
	}
	
	/**
	 * テーブル定義のみにするかどうか
	 */
	private boolean onlyTableSchema = false;
	/**
	 * テーブル定義のみにするかどうかをセットする
	 * @param onlyTableSchema
	 */
	public void setOnlyTableSchema(boolean onlyTableSchema){
		this.onlyTableSchema = onlyTableSchema;
	}
	
	private Set<String> excludedTable = new HashSet<String>();
	/**
	 * 移行から除外するテーブル名をセットする
	 * @param excludedTable
	 */
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
		openMdb();
	}
	
	/**
	 * スキーマごと削除する
	 * @param hostName
	 * @param port
	 * @param databaseName
	 * @param schema 
	 * @param user
	 * @param pass
	 * @param authorization
	 * @throws ClassNotFoundException
	 * @throws SQLException
	 */
	public static void dropSchema(String hostName, String port, String databaseName, String schema, String user, String pass, String authorization) throws ClassNotFoundException, SQLException{
		Class.forName("org.postgresql.Driver");
		Connection c = null;
		try {
			String url = "jdbc:postgresql://" + hostName + ":" + port + "/" + databaseName;
			System.out.println(url);
			c = DriverManager.getConnection(url, user, pass);
			Statement st1 = c.createStatement();
			String dropSql = "DROP SCHEMA IF EXISTS " + schema + " CASCADE;";
			System.out.println(dropSql);
			st1.execute(dropSql);
			
			Statement st2 = c.createStatement();
			String createSql = "CREATE SCHEMA " + schema + " AUTHORIZATION " + authorization + ";";
			System.out.println(createSql);
			st2.execute(createSql);
		} catch (SQLException e) {
			e.printStackTrace();
		} finally{
			if(c != null){
				c.close();
			}
		}
	}
	
	private Set<String> tableNames;
	public Set<String> getTableNames(){
		return tableNames;
	}
	
	public List<MigrateEntity> getFilteringMigrateEntities(){
		return getTableNames().stream()
			.filter(e -> targetTableNames.size() == 0 || targetTableNames.contains(e))
			.filter(e -> excludedTable.size() == 0 || !excludedTable.contains(e))
			.map(tableName -> getMigrateEntity(tableName))
			.collect(Collectors.toList());
	}
	
	private Map<String, Table> tableMap = new HashMap<>();
	public Table getTable(String tableName) throws IOException{
		return tableMap.get(tableName);
	}
	
	public List<? extends Column> getColumns(String tableName) throws IOException{
		if(tableName != null){
			Table table = getTable(tableName);
			if(table != null){
				return table.getColumns();
			}
		}
		return null;
	}
	
	private void openMdb(){
		// 並列処理される前に予めセットしておく
		setMigrateEntity();
		Set<String> tableNames = getTableNames();
		tableNames.parallelStream()
			.filter(e -> targetTableNames.size() == 0 || targetTableNames.contains(e))
			.filter(e -> excludedTable.size() == 0 || !excludedTable.contains(e))
			.forEach(tableName -> {
				try {
					migrateTable(tableName);
				} catch (Exception e1) {
					throw new RuntimeException(e1);
				}
		});
		System.out.println("END");

	}
	
	public List<MigrateEntity> getMigrateEntities(){
		if(migrateEntityMap == null){
			setMigrateEntity();
		}
		List<MigrateEntity> migrateEntities = new ArrayList<MigrateEntity>();
		for(Entry<String, MigrateEntity> e : migrateEntityMap.entrySet()){
			migrateEntities.add(e.getValue());
		}
		return migrateEntities;
	}
	
	private Map<String, MigrateEntity> migrateEntityMap;
	public MigrateEntity getMigrateEntity(String table){
		if(migrateEntityMap == null){
			setMigrateEntity();
		}
		return migrateEntityMap.get(table);
	}
	
	private void setMigrateEntity(){
		if(migrateEntityMap != null){
			return;
		}
		migrateEntityMap = new HashMap<String, MigrateEntity>();
		tableNames.parallelStream()
		.filter(e -> targetTableNames.size() == 0 || targetTableNames.contains(e))
		.filter(e -> excludedTable.size() == 0 || !excludedTable.contains(e))
		.forEach(tableName -> {
			try {
				Table t = getTable(tableName);
				List<? extends Column> columns = t.getColumns();
				MigrateEntity me = new MigrateEntity();
				TableMapping tableMapping = convertTableMap.get(tableName);
				if(tableMapping != null){
					me.setTableName(tableMapping.getMigrateTableName());
					me.setTableMapping(tableMapping);
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
							dataTypeName = tableMapping.getData(c.getName());
						}
					}
					me.setColumNameMap(columnName, dataTypeName);
				}
				migrateEntityMap.put(tableName, me);
			} catch (Exception e1) {
				throw new RuntimeException(e1);
			}
		});
	}
	
	private void migrateTable(String tableName) throws SQLException, IOException{
		MigrateEntity me = getMigrateEntity(tableName);
		try {
			Statement statement = con.createStatement();
			statement.execute(me.getCreateTableSQL());
			System.out.println("success:" + me.getCreateTableSQL());
		} catch (SQLException e1) {
			System.out.println(e1.toString());
			System.out.println("fail:" + me.getCreateTableSQL());
		}
		if(onlyTableSchema){
			return;
		}
		Iterator<Row> rowIterator = getTable(tableName).iterator();
		while(rowIterator.hasNext()){
			Map<String, Object> row = rowIterator.next();
			me.clearData();
			for(Entry<String, Object> e : row.entrySet()){
				String key = e.getKey();
				if(me.getTableMapping().containsOriginColumnName(e.getKey())){
					key = me.getTableMapping().getMigrateColumnName(e.getKey());
				}
				if(e.getValue() != null){
					me.setDataMap(key, e.getValue().toString());
				}else{
					me.setDataMap(key, null);
				}
			}
			PreparedStatement ps = con.prepareStatement(me.getPreparedStatementSql());
			try {
				ps = me.applyPreparedStatement(ps);
				ps.executeUpdate();
				System.out.println("success:" + ps);
				ps.clearParameters();
			} catch (SQLException e1) {
				System.out.println(e1.toString());
				System.out.println("fail:" + ps);
			}
		}
	}
	
}
