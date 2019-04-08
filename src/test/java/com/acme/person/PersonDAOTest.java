/*
 * Created on 2019-04-04 ( Date ISO 2019-04-04 - Time 17:50:18 )
 * Generated by Telosys ( http://www.telosys.org/ ) version 3.0.0
 */
package com.acme.person;

import static org.junit.Assert.assertTrue;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

/**
 * Generic test class for a persistence service
 * 
 * @author Telosys 
 *
 */
public class PersonDAOTest {

	/**
	 * Generic test for a persistence service
	 * @param persistenceService
	 * @throws SQLException
	 */
	@Test
	public void testPersistenceService() {
    	System.out.println("--- test PersonPersistence ");

    	Map<String, Object> map = new HashMap<>() ;
    	map.put("id", 12 );
    	map.put("firstName", "Bob" );
    	map.put("lastName", "Morane" );
    	
    	PersonDAO dao = new PersonDAO();

    	System.out.println("exists : " + map );
    	boolean r = dao.exists(map);
    	System.out.println("exists : " + r);

    	//--- DELETE
    	System.out.println("Delete : " + map );
    	dao.delete(map) ; // Just to be sure it doesn't exist before insert

    	//--- CREATE
    	System.out.println("Create : " + map );
    	dao.create(map);
    	assertTrue( dao.exists(map) );

//    	//--- FIND
//    	System.out.println("Find by id..." );
//
//
//    	System.out.println("Found : " + person2 );
//    	assertNotNull(person2);
//		assertTrue( person2.getId() == 100 ) ;
//    	assertTrue( persistenceService.exists(person2) ) ;
//    	
//    	//--- UPDATE
//		//--- Change values
//		person2.setFirstName("B"); // "firstName" : java.lang.String
//		person2.setLastName("B"); // "lastName" : java.lang.String
//    	System.out.println("Update : " + person2 );
//    	assertTrue( persistenceService.update(person2) );
//    	
//    	//--- RELOAD AFTER UPDATE
//    	System.out.println("Find by id..." );
//
//    	PersonRecord person3 = persistenceService.findById(100);
//    	assertNotNull(person3);
//    	System.out.println("Found : " + person3 );
//
//		// Check same data in the reloaded instance
//		assertEquals(person2.getFirstName(), person3.getFirstName() ); 
//		assertEquals(person2.getLastName(), person3.getLastName() ); 
//
//    	//--- DELETE
//    	System.out.println("Delete : " + person2 );
//    	assertTrue( persistenceService.delete(person2) ); // Delete #1 : OK
//    	assertFalse( persistenceService.delete(person2) ); // Nothing (already deleted)
//    	assertFalse( persistenceService.deleteById(100) ); // Nothing (already deleted)
//
//		long finalCount = persistenceService.countAll() ;
//    	System.out.println("Final count = " + finalCount );
//		assertEquals(initialCount, finalCount);
//
//    	assertFalse( persistenceService.exists(100) ) ;
//    	assertFalse( persistenceService.exists(person2) ) ;
//    	person2 = persistenceService.findById(100);
//    	assertNull( person2 );
//    	
    	System.out.println("Normal end of persistence service test." );
	}
}
