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

package org.pentaho.di.core.database;

import org.pentaho.di.core.Const;
import org.pentaho.di.core.exception.KettleDatabaseException;
import org.pentaho.di.core.plugins.DatabaseMetaPlugin;
import org.pentaho.di.core.row.ValueMetaInterface;

/**
 * 
 * 
 * @author Sun
 * @since 2019年9月5日
 * @version
 * 
 */
@DatabaseMetaPlugin(type = "GBASE", typeDescription = "GBase Database")
public class GBaseDatabaseMeta extends BaseDatabaseMeta implements DatabaseInterface {

  @Override
  public int[] getAccessTypeList() {
    return new int[] { DatabaseMeta.TYPE_ACCESS_NATIVE, DatabaseMeta.TYPE_ACCESS_ODBC, };
  }

  @Override
  public int getDefaultDatabasePort() {
    if (getAccessType() == DatabaseMeta.TYPE_ACCESS_NATIVE) {
      return 1526;
    }
    return -1;
  }

  @Override
  public String getDriverClass() {
    if (getAccessType() == DatabaseMeta.TYPE_ACCESS_ODBC) {
      return "sun.jdbc.odbc.JdbcOdbcDriver";
    } else {
      return "com.informix.jdbc.IfxDriver";
    }
  }

  @Override
  public String getURL(String hostname, String port, String databaseName) throws KettleDatabaseException {
    if (getAccessType() == DatabaseMeta.TYPE_ACCESS_ODBC) {
      return "jdbc:odbc:" + databaseName;
    } else {
      return "jdbc:informix-sqli://" + hostname + ":" + port + "/" + databaseName + ":INFORMIXSERVER=" + getServername() + ";DELIMIDENT=Y";
    }
  }

  @Override
  public boolean needsPlaceHolder() {
    return true;
  }

  @Override
  public boolean needsToLockAllTables() {
    return false;
  }

  @Override
  public String getSQLQueryFields(String tableName) {
    return "SELECT FIRST 1 * FROM " + tableName;
  }

  @Override
  public String getSQLTableExists(String tablename) {
    return getSQLQueryFields(tablename);
  }

  @Override
  public String getSQLColumnExists(String columnname, String tablename) {
    return getSQLQueryColumnFields(columnname, tablename);
  }

  public String getSQLQueryColumnFields(String columnname, String tableName) {
    return "SELECT FIRST 1 " + columnname + " FROM " + tableName;
  }

  @Override
  public String getSQLLockTables(String[] tableNames) {
    StringBuilder sql = new StringBuilder(128);
    for (int i = 0; i < tableNames.length; i++) {
      sql.append("LOCK TABLE " + tableNames[i] + " IN SHARE MODE;" + Const.CR);
    }
    return sql.toString();
  }

  @Override
  public String getAddColumnStatement(String tablename, ValueMetaInterface v, String tk, boolean use_autoinc, String pk, boolean semicolon) {
    return "ALTER TABLE " + tablename + " ADD " + getFieldDefinition(v, tk, pk, use_autoinc, true, false);
  }

  @Override
  public String getModifyColumnStatement(String tablename, ValueMetaInterface v, String tk, boolean use_autoinc, String pk, boolean semicolon) {
    return "ALTER TABLE " + tablename + " MODIFY " + getFieldDefinition(v, tk, pk, use_autoinc, true, false);
  }

  @Override
  public String getDropColumnStatement(String tablename, ValueMetaInterface v, String tk, boolean use_autoinc, String pk, boolean semicolon) {
    return "ALTER TABLE " + tablename + " DROP COLUMN " + v.getName() + Const.CR;
  }

  @Override
  public String getFieldDefinition(ValueMetaInterface v, String tk, String pk, boolean use_autoinc, boolean add_fieldname, boolean add_cr) {
    String retval = "";

    String fieldname = v.getName();
    int length = v.getLength();
    int precision = v.getPrecision();

    if (add_fieldname) {
      retval += fieldname + " ";
    }

    int type = v.getType();
    switch (type) {
    case ValueMetaInterface.TYPE_TIMESTAMP:
      retval += "DATETIME";
      break;
    case ValueMetaInterface.TYPE_DATE:
      retval += "DATETIME YEAR to FRACTION";
      break;
    case ValueMetaInterface.TYPE_BOOLEAN:
      if (supportsBooleanDataType()) {
        retval += "BOOLEAN";
      } else {
        retval += "CHAR(1)";
      }
      break;
    case ValueMetaInterface.TYPE_NUMBER:
    case ValueMetaInterface.TYPE_INTEGER:
    case ValueMetaInterface.TYPE_BIGNUMBER:
      if (fieldname.equalsIgnoreCase(tk) || // Technical key
          fieldname.equalsIgnoreCase(pk) // Primary key
      ) {
        if (use_autoinc) {
          retval += "SERIAL8";
        } else {
          retval += "INTEGER PRIMARY KEY";
        }
      } else {
        if ((length < 0 && precision < 0) || precision > 0 || length > 9) {
          retval += "FLOAT";
        } else { // Precision == 0 && length<=9
          retval += "INTEGER";
        }
      }
      break;
    case ValueMetaInterface.TYPE_STRING:
      if (length >= DatabaseMeta.CLOB_LENGTH) {
        retval += "CLOB";
      } else {
        if (length < 256) {
          retval += "VARCHAR";
          if (length > 0) {
            retval += "(" + length + ")";
          }
        } else {
          if (length < 32768) {
            retval += "LVARCHAR";
          } else {
            retval += "TEXT";
          }
        }
      }
      break;
    default:
      retval += " UNKNOWN";
      break;
    }

    if (add_cr) {
      retval += Const.CR;
    }

    return retval;
  }

  @Override
  public String[] getReservedWords() {
    return new String[] {
        /*
         * Transact-SQL Reference: Reserved Keywords Includes future keywords: could be reserved in future releases of SQL
         * Server as new features are implemented. REMARK: When SET QUOTED_IDENTIFIER is ON (default), identifiers can be
         * delimited by double quotation marks, and literals must be delimited by single quotation marks. When SET
         * QUOTED_IDENTIFIER is OFF, identifiers cannot be quoted and must follow all Transact-SQL rules for identifiers.
         */
        "AAO", "ABS", "ABSOLUTE", "ACCELERATE", "ACCESS", "ACCESS_METHOD", "ACCOUNT", "ACOS", "ACOSH", "ACTIVE", "ADD",
        "ADDRESS", "ADD_MONTHS", "ADMIN", "AFTER", "AGGREGATE", "ALIGNMENT", "ALL", "ALL_ROWS", "ALLOCATE", "ALTER",
        "AND", "ANSI", "ANY", "APPEND", "AQT", "ARRAY", "AS", "ASC", "ASCII", "ASIN", "ASINH", "ASYNC", "AT", "ATAN",
        "ATAN2", "ATANH", "ATTACH", "ATTRIBUTES", "AUDIT", "AUTHENTICATION", "AUTHID", "AUTHORIZATION", "AUTHORIZED",
        "AUTO", "AUTOFREE", "AUTOLOCATE", "AUTO_READAHEAD", "AUTO_REPREPARE", "AUTO_STAT_MODE", "AVG", "AVOID_EXECUTE",
        "AVOID_FACT", "AVOID_FULL", "AVOID_HASH", "AVOID_INDEX", "AVOID_INDEX_SJ", "AVOID_MULTI_INDEX", "AVOID_NL",
        "AVOID_STAR_JOIN", "BARGROUP", "BASED", "BEFORE", "BEGIN", "BETWEEN", "BIGINT", "BIGSERIAL", "BINARY", "BITAND",
        "BITANDNOT", "BITNOT", "BITOR", "BITXOR", "BLOB", "BLOBDIR", "BOOLEAN", "BOTH", "BOUND_IMPL_PDQ", "BSON",
        "BUCKETS", "BUFFERED", "BUILTIN", "BY", "BYTE", "CACHE", "CALL", "CANNOTHASH", "CARDINALITY", "CASCADE", "CASE",
        "CAST", "CEIL", "CHAR", "CHAR_LENGTH", "CHARACTER", "CHARACTER_LENGTH", "CHARINDEX", "CHECK", "CHR", "CLASS",
        "CLASS_ORIGIN", "CLEANUP", "CLIENT", "CLOB", "CLOBDIR", "CLOSE", "CLUSTER", "CLUSTER_TXN_SCOPE", "COBOL",
        "CODESET", "COLLATION", "COLLECTION", "COLUMN", "COLUMNS", "COMMIT", "COMMITTED", "COMMUTATOR", "COMPONENT",
        "COMPONENTS", "COMPRESSED", "CONCAT", "CONCURRENT", "CONNECT", "CONNECTION", "CONNECTION_NAME",
        "CONNECT_BY_ISCYCLE", "CONNECT_BY_ISLEAF", "CONNECT_BY_ROOT", "CONST", "CONSTRAINT", "CONSTRAINTS",
        "CONSTRUCTOR", "CONTEXT", "CONTINUE", "COPY", "COS", "COSH", "COSTFUNC", "COUNT", "CRCOLS", "CREATE", "CROSS",
        "CUME_DIST", "CURRENT", "CURRENT_ROLE", "CURRENT_USER", "CURRVAL", "CURSOR", "CYCLE", "DATA", "DATABASE",
        "DATAFILES", "DATASKIP", "DATE", "DATETIME", "DAY", "DBA", "DBDATE", "DBINFO", "DBPASSWORD", "DBSA",
        "DBSERVERNAME", "DBSECADM", "DBSSO", "DEALLOCATE", "DEBUG", "DEBUGMODE", "DEBUG_ENV", "DEC", "DECIMAL",
        "DECLARE", "DECODE", "DECRYPT_BINARY", "DECRYPT_CHAR", "DEC_T", "DEFAULT", "DEFAULTESCCHAR", "DEFAULT_ROLE",
        "DEFAULT_USER", "DEFERRED", "DEFERRED_PREPARE", "DEFINE", "DEGREES", "DELAY", "DELETE", "DELETING", "DELIMITED",
        "DELIMITER", "DELUXE", "DENSERANK", "DENSE_RANK", "DESC", "DESCRIBE", "DESCRIPTOR", "DETACH", "DIAGNOSTICS",
        "DIRECTIVES", "DIRTY", "DISABLE", "DISABLED", "DISCARD", "DISCONNECT", "DISK", "DISTINCT", "DISTRIBUTEBINARY",
        "DISTRIBUTESREFERENCES", "DISTRIBUTIONS", "DOCUMENT", "DOMAIN", "DONOTDISTRIBUTE", "DORMANT", "DOUBLE", "DROP",
        "DTIME_T", "EACH", "ELIF", "ELSE", "ENABLE", "ENABLED", "ENCRYPT_AES", "ENCRYPT_TDES", "ENCRYPTION", "END",
        "ENUM", "ENVIRONMENT", "ERKEY", "ERROR", "ESCAPE", "EXCEPT", "EXCEPTION", "EXCLUSIVE", "EXEC", "EXECUTE",
        "EXECUTEANYWHERE", "EXEMPTION", "EXISTS", "EXIT", "EXP", "EXPLAIN", "EXPLICIT", "EXPRESS", "EXPRESSION",
        "EXTDIRECTIVES", "EXTEND", "EXTENT", "EXTERNAL", "EXTYPEID", "EXTYPELENGTH", "EXTYPENAME", "EXTYPEOWNERLENGTH",
        "EXTYPEOWNERNAME", "FACT", "FALLBACK", "FALSE", "FAR", "FETCH", "FILE", "FILETOBLOB", "FILETOCLOB",
        "FILLFACTOR", "FILTERING", "FINAL", "FIRST", "FIRST_ROWS", "FIRST_VALUE", "FIXCHAR", "FIXED", "FLOAT", "FLOOR",
        "FLUSH", "FOLLOWING", "FOR", "FORCE", "FORCED", "FORCE_DDL_EXEC", "FOREACH", "FOREIGN", "FORMAT",
        "FORMAT_UNITS", "FORTRAN", "FOUND", "FRACTION", "FRAGMENT", "FRAGMENTS", "FREE", "FROM", "FULL", "FUNCTION",
        "GB", "GENBSON", "GENERAL", "GET", "GETHINT", "GIB", "GLOBAL", "GO", "GOTO", "GRANT", "GREATERTHAN",
        "GREATERTHANOREQUAL", "GRID", "GRID_NODE_SKIP", "GROUP", "HANDLESNULLS", "HASH", "HAVING", "HDR",
        "HDR_TXN_SCOPE", "HEX", "HIGH", "HINT", "HOLD", "HOME", "HOUR", "IDATA", "IDSLBACREADARRAY", "IDSLBACREADSET",
        "IDSLBACREADTREE", "IDSLBACRULES", "IDSLBACWRITEARRAY", "IDSLBACWRITESET", "IDSLBACWRITETREE",
        "IDSSECURITYLABEL", "IF", "IFX_*", "ILENGTH", "IMMEDIATE", "IMPLICIT", "IMPLICIT_PDQ", "IN", "INACTIVE",
        "INCREMENT", "INDEX", "INDEXES", "INDEX_ALL", "INDEX_SJ", "INDICATOR", "INFORMIX", "INFORMIXCONRETRY",
        "INFORMIXCONTIME", "INIT", "INITCAP", "INLINE", "INNER", "INOUT", "INSENSITIVE", "INSERT", "INSERTING",
        "INSTEAD", "INSTR", "INT", "INT8", "INTEG", "INTEGER", "INTERNAL", "INTERNALLENGTH", "INTERSECT", "INTERVAL",
        "INTO", "INTRVL_T", "IS", "ISCANONICAL", "ISOLATION", "ITEM", "ITERATOR", "ITYPE", "JAVA", "JOIN", "JSON", "KB",
        "KEEP", "KEY", "KIB", "LABEL", "LABELEQ", "LABELGE", "LABELGLB", "LABELGT", "LABELLE", "LABELLT", "LABELLUB",
        "LABELTOSTRING", "LAG", "LANGUAGE", "LAST", "LAST_DAY", "LAST_VALUE", "LATERAL", "LEAD", "LEADING", "LEFT",
        "LEN", "LENGTH", "LESSTHAN", "LESSTHANOREQUAL", "LET", "LEVEL", "LIKE", "LIMIT", "LIST", "LISTING", "LOAD",
        "LOCAL", "LOCATOR", "LOCK", "LOCKS", "LOCOPY", "LOC_T", "LOG", "LOG10", "LOGN", "LONG", "LOOP", "LOTOFILE",
        "LOW", "LOWER", "LPAD", "LTRIM", "LVARCHAR", "MATCHED", "MATCHES", "MAX", "MAXERRORS", "MAXLEN", "MAXVALUE",
        "MB", "MDY", "MEDIAN", "MEDIUM", "MEMORY", "MEMORY_RESIDENT", "MERGE", "MESSAGE_LENGTH", "MESSAGE_TEXT", "MIB",
        "MIN", "MINUS", "MINUTE", "MINVALUE", "MOD", "MODE", "MODERATE", "MODIFY", "MODULE", "MONEY", "MONTH",
        "MONTHS_BETWEEN", "MORE", "MULTISET", "MULTI_INDEX", "NAME", "NCHAR", "NEAR_SYNC", "NEGATOR", "NEW", "NEXT",
        "NEXT_DAY", "NEXTVAL", "NLSCASE", "NO", "NOCACHE", "NOCYCLE", "NOMAXVALUE", "NOMIGRATE", "NOMINVALUE", "NONE",
        "NON_RESIDENT", "NON_DIM", "NOORDER", "NORMAL", "NOT", "NOTEMPLATEARG", "NOTEQUAL|", "NOVALIDATE", "NTILE",
        "NULL", "NULLABLE", "NULLIF", "NULLS", "NUMBER", "NUMERIC", "NUMROWS", "NUMTODSINTERVAL", "NUMTOYMINTERVAL",
        "NVARCHAR", "NVL", "OCTET_LENGTH", "OF", "OFF", "OLD", "ON", "ONLINE", "ONLY", "OPAQUE", "OPCLASS", "OPEN",
        "OPTCOMPIND", "OPTIMIZATION", "OPTION", "OR", "ORDER", "ORDERED", "OUT", "OUTER", "OUTPUT", "OVER", "OVERRIDE",
        "PAGE", "PARALLELIZABLE", "PARAMETER", "PARTITION", "PASCAL", "PASSEDBYVALUE", "PASSWORD", "PDQPRIORITY",
        "PERCALL_COST", "PERCENT_RANK", "PIPE", "PLI", "PLOAD", "POLICY", "POW", "POWER", "PRECEDING", "PRECISION",
        "PREPARE", "PREVIOUS", "PRIMARY", "PRIOR", "PRIVATE", "PRIVILEGES", "PROBE", "PROCEDURE", "PROPERTIES",
        "PUBLIC", "PUT", "QUARTER", "RADIANS", "RAISE", "RANGE", "RANK", "RATIOTOREPORT", "RATIO_TO_REPORT", "RAW",
        "READ", "REAL", "RECORDEND", "REFERENCES", "REFERENCING", "REGISTER", "REJECTFILE", "RELATIVE", "RELEASE",
        "REMAINDER", "RENAME", "REOPTIMIZATION", "REPEATABLE", "REPLACE", "REPLICATION", "RESOLUTION", "RESOURCE",
        "RESTART", "RESTRICT", "RESUME", "RETAIN", "RETAINUPDATELOCKS", "RETURN", "RETURNED_SQLSTATE", "RETURNING",
        "RETURNS", "REUSE", "REVERSE", "REVOKE", "RIGHT", "ROBIN", "ROLE", "ROLLBACK", "ROLLFORWARD", "ROLLING", "ROOT",
        "ROUND", "ROUTINE", "ROW", "ROWID", "ROWIDS", "ROWNUMBER", "ROWS", "ROW_COUNT", "ROW_NUMBER", "RPAD", "RTRIM",
        "RULE", "SAMEAS", "SAMPLES", "SAMPLING", "SAVE", "SAVEPOINT", "SCHEMA", "SCALE", "SCROLL", "SECLABEL_BY_COMP",
        "SECLABEL_BY_NAME", "SECLABEL_TO_CHAR", "SECOND", "SECONDARY", "SECURED", "SECURITY", "SECTION", "SELCONST",
        "SELECT", "SELECTING", "SELECT_GRID", "SELECT_GRID_ALL", "SELFUNC", "SELFUNCARGS", "SENSITIVE", "SEQUENCE",
        "SERIAL", "SERIAL8", "SERIALIZABLE", "SERVER", "SERVER_NAME", "SERVERUUID", "SESSION", "SET", "SETSESSIONAUTH",
        "SHARE", "SHORT", "SIBLINGS", "SIGNED", "SIN", "SITENAME", "SIZE", "SKIP", "SMALLFLOAT", "SMALLINT", "SOME",
        "SOURCEID", "SOURCETYPE", "SPACE", "SPECIFIC", "SQL", "SQLCODE", "SQLCONTEXT", "SQLERROR", "SQLSTATE",
        "SQLWARNING", "SQRT", "STABILITY", "STACK", "STANDARD", "START", "STAR_JOIN", "STATCHANGE", "STATEMENT",
        "STATIC", "STATISTICS", "STATLEVEL", "STATUS", "STDEV", "STEP", "STOP", "STORAGE", "STORE", "STRATEGIES",
        "STRING", "STRINGTOLABEL", "STRUCT", "STYLE", "SUBCLASS_ORIGIN", "SUBSTR", "SUBSTRING", "SUBSTRING_INDEX",
        "SUM", "SUPPORT", "SYNC", "SYNONYM", "SYS*", "TABLE", "TABLES", "TAN", "TASK", "TB", "TEMP", "TEMPLATE", "TEST",
        "TEXT", "THEN", "TIB", "TIME", "TO", "TODAY", "TO_CHAR", "TO_DATE", "TO_DSINTERVAL", "TO_NUMBER",
        "TO_YMINTERVAL", "TRACE", "TRAILING", "TRANSACTION", "TRANSITION", "TREE", "TRIGGER", "TRIGGERS", "TRIM",
        "TRUE", "TRUNC", "TRUNCATE", "TRUSTED", "TYPE", "TYPEDEF", "TYPEID", "TYPENAME", "TYPEOF", "UID", "UNBOUNDED",
        "UNCOMMITTED", "UNDER", "UNION", "UNIQUE", "UNIQUECHECK", "UNITS", "UNKNOWN", "UNLOAD", "UNLOCK", "UNSIGNED",
        "UPDATE", "UPDATING", "UPON", "UPPER", "USAGE", "USE", "USELASTCOMMITTED", "USER", "USE_DWA", "USE_HASH",
        "USE_NL", "USING", "USTLOW_SAMPLE", "VALUE", "VALUES", "VAR", "VARCHAR", "VARIABLE", "VARIANCE", "VARIANT",
        "VARYING", "VERCOLS", "VIEW", "VIOLATIONS", "VOID", "VOLATILE", "WAIT", "WARNING", "WEEKDAY", "WHEN",
        "WHENEVER", "WHERE", "WHILE", "WITH", "WITHOUT", "WORK", "WRITE", "WRITEDOWN", "WRITEUP", "XADATASOURCE", "XID",
        "XLOAD", "XUNLOAD", "YEAR" };
  }

  @Override
  public String[] getUsedLibraries() {
    return new String[] { "ifxjdbc.jar" };
  }

  public String getExtraOptionsHelpText() {
    return "http://www.gbase.cn/";
  }

}
