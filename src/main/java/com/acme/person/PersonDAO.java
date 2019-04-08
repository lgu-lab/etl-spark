package com.acme.person;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Map;

import org.demo.framework.db.DbOperations;
import org.demo.framework.db.GenericJdbcDAO;

public class PersonDAO extends GenericJdbcDAO<Map<String,Object>> implements DbOperations { 

	private final static String SQL_SELECT_ALL = "select id, firstName, lastName from Person";

	private final static String SQL_SELECT = "select id, firstName, lastName from Person where id = ?";

	private final static String SQL_INSERT = "insert into Person ( id, firstName, lastName ) values ( ?, ?, ? )";

	private final static String SQL_UPDATE = "update Person set firstName = ?, lastName = ? where id = ?";

	private final static String SQL_DELETE = "delete from Person where id = ?";

	private final static String SQL_COUNT_ALL = "select count(*) from Person";

	private final static String SQL_COUNT = "select count(*) from Person where id = ?";
	
    //----------------------------------------------------------------------
	@Override
	protected String getSqlSelect() {
		return SQL_SELECT ;
	}
    //----------------------------------------------------------------------
	@Override
	protected String getSqlSelectAll() {
		return SQL_SELECT_ALL;
	}
    //----------------------------------------------------------------------
	@Override
	protected String getSqlInsert() {
		return SQL_INSERT ;
	}
    //----------------------------------------------------------------------
	@Override
	protected String getSqlUpdate() {
		return SQL_UPDATE ;
	}
    //----------------------------------------------------------------------
	@Override
	protected String getSqlDelete() {
		return SQL_DELETE ;
	}
    //----------------------------------------------------------------------
	@Override
	protected String getSqlCount() {
		return SQL_COUNT ;
	}
    //----------------------------------------------------------------------
	@Override
	protected String getSqlCountAll() {
		return SQL_COUNT_ALL ;
	}

    //----------------------------------------------------------------------
	@Override
	protected void setValuesForPrimaryKey(PreparedStatement ps, int i, Map<String,Object>map) throws SQLException {
		//--- Set PRIMARY KEY from bean to PreparedStatement ( SQL "WHERE key=?, ..." )
		setValue(ps, i++, (Integer) map.get("id") ) ; // "id" : int
	}

    //----------------------------------------------------------------------
	@Override
	protected void setValuesForInsert(PreparedStatement ps, int i, Map<String,Object>map) throws SQLException {
		//--- Set PRIMARY KEY and DATA from bean to PreparedStatement ( SQL "SET x=?, y=?, ..." )
		setValue(ps, i++, (Integer) map.get("id") ) ; // "id" : int
		setValue(ps, i++, (String) map.get("firstName" ) ) ; // "firstName" : java.lang.String
		setValue(ps, i++, (String) map.get("lastName" ) ) ; // "lastName" : java.lang.String
	}

    //----------------------------------------------------------------------
	@Override
	protected void setValuesForUpdate(PreparedStatement ps, int i, Map<String,Object>map) throws SQLException {
		//--- Set DATA from bean to PreparedStatement ( SQL "SET x=?, y=?, ..." )
		setValue(ps, i++, (String) map.get("firstName" ) ) ; // "firstName" : java.lang.String
		setValue(ps, i++, (String) map.get("firstName" ) ) ; // "lastName" : java.lang.String
		//--- Set PRIMARY KEY from bean to PreparedStatement ( SQL "WHERE key=?, ..." )
		setValue(ps, i++, (Integer) map.get("id") ) ; // "id" : int
	}

	
	@Override
	public void save(Map<String, Object> map) {
		if ( super.doExists(map) ) {
			super.doUpdate(map);
		}
		else {
			super.doInsert(map);
		}
	}

	@Override
	public boolean update(Map<String, Object> map) {
		int r = super.doUpdate(map);
		return r > 0 ;
	}

	@Override
	public void create(Map<String, Object> map) {
		super.doInsert(map);
	}

	@Override
	public boolean delete(Map<String, Object> map) {
		int r = super.doDelete(map);
		return r > 0 ;
	}

	@Override
	public boolean exists(Map<String, Object> map) {
		return super.doExists(map);
	}

}
