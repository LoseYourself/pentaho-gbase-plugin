/*******************************************************************************
 *
 * 
 *
 * Copyright (C) 2011-2019 by Sun : http://www.kingbase.com.cn
 *
 *******************************************************************************
 *
 *
 *    Email : snj1314@163.com
 *
 *
 ******************************************************************************/

package org.pentaho.di.core.database.test;

import static org.junit.Assert.assertEquals;

import org.junit.Test;
import org.pentaho.di.core.database.GBaseDatabaseMeta;

/**
 * 
 * 
 * @author Sun
 * @since 2019年9月6日
 * @version
 * 
 */
public class GBaseDatabaseMetaTest {

  @Test
  public void testDriverClass() {
    GBaseDatabaseMeta dbMeta = new GBaseDatabaseMeta();
    assertEquals("com.informix.jdbc.IfxDriver", dbMeta.getDriverClass());
  }

  @Test
  public void testDefaultDatabasePort() {
    GBaseDatabaseMeta dbMeta = new GBaseDatabaseMeta();
    assertEquals(1526, dbMeta.getDefaultDatabasePort());
  }

}
