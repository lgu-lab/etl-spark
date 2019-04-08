package org.demo.framework.db;

import java.util.Map;

/**
 * Persistence Interface 
 */
public interface DbOperations { 

	/**
	 * Saves the given entity in the database (create or update)
	 * @param entity
	 * @return entity
	 */
	void save(Map<String,Object>map);

	/**
	 * Updates the given entity in the database
	 * @param entity
	 * @return true if the entity has been updated, false if not found and not updated
	 */
	boolean update(Map<String,Object>map);

	/**
	 * Creates the given entity in the database
	 * @param entity
	 * @return
	 */
	void create(Map<String,Object>map);

	/**
	 * Deletes an entity using the Id / Primary Key stored in the given object
	 * @param the entity to be deleted (supposed to have a valid Id/PK )
	 * @return true if the entity has been deleted, false if not found and not deleted
	 */
	boolean delete(Map<String,Object>map);

	/**
	 * Returns true if an entity exists with the given Id / Primary Key 
	 * @param id
	 * @return
	 */
	public boolean exists(Map<String,Object>map) ;

}
