//--------------------------------------------------------------------------------------------------
//
//  BDB Library
//  Copyright (C) 2009  Artem Rodygin
//
//  This program is free software: you can redistribute it and/or modify
//  it under the terms of the GNU General Public License as published by
//  the Free Software Foundation, either version 3 of the License, or
//  (at your option) any later version.
//
//  This program is distributed in the hope that it will be useful,
//  but WITHOUT ANY WARRANTY; without even the implied warranty of
//  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
//  GNU General Public License for more details.
//
//  You should have received a copy of the GNU General Public License
//  along with this program.  If not, see <http://www.gnu.org/licenses/>.
//
//--------------------------------------------------------------------------------------------------
//  Author                  Date            Description of modifications
//--------------------------------------------------------------------------------------------------
//  Artem Rodygin           2009-11-18      Initial creation.
//--------------------------------------------------------------------------------------------------

/** @file include/bdb.h
 * Main include file of the BDB Library.
 * @author Artem Rodygin
 */

#ifndef BDB_H
#define BDB_H

// Standard C/C++ Libraries
#include <stack>
#include <stdint.h>
#include <string>
#include <vector>

// Protocol Buffers
#include <google/protobuf/message.h>

// C++ Logging Library
#include <log4cplus/logger.h>

/** Modificator for API, exported from this DLL. */
#if defined(_MSC_VER) && defined(_WINDLL)
  #ifdef  bdb_EXPORTS
  #define BDB_EXPORT __declspec(dllexport)
  #else
  #define BDB_EXPORT __declspec(dllimport)
  #endif
#else
  #define BDB_EXPORT
#endif

//--------------------------------------------------------------------------------------------------
//  Constants.
//--------------------------------------------------------------------------------------------------

/**
 * Name of library's logging port.
 */
#define BDB_LOGGER_PORT     "bdb"

/** @defgroup errcodes BDB exception codes. */
//@{
#define BDB_ERROR_UNKNOWN       1   /**< Unknown error.                */
#define BDB_ERROR_NOT_FOUND     2   /**< Object is not found.          */
#define BDB_ERROR_EXISTS        3   /**< Object already exists.        */
#define BDB_ERROR_FOREIGN_KEY   4   /**< Foreign constraint violation. */
//@}

/** Berkeley DB forward definitions. */
struct __db;            typedef struct __db          DB;            /**< @typedef */
struct __db_env;        typedef struct __db_env      DB_ENV;        /**< @typedef */
struct __db_sequence;   typedef struct __db_sequence DB_SEQUENCE;   /**< @typedef */
struct __db_txn;        typedef struct __db_txn      DB_TXN;        /**< @typedef */
struct __dbc;           typedef struct __dbc         DBC;           /**< @typedef */
struct __db_dbt;        typedef struct __db_dbt      DBT;           /**< @typedef */

/** @namespace bdb
 * BDB Library namespace.
 */
namespace bdb
{

using std::stack;
using std::string;
using std::vector;

using google::protobuf::Message;

// Local forward definitions.
class exception;
class database;
class sequence;
class table;
class index;
class recordset;

//--------------------------------------------------------------------------------------------------
//  Typedefs.
//--------------------------------------------------------------------------------------------------

/**
 * List of bdb::recordsets to join.
 */
typedef vector <recordset *> joinlist;

//--------------------------------------------------------------------------------------------------
//  Callback functions prototypes.
//--------------------------------------------------------------------------------------------------

/**
 * Application-specified keys comparison function.
 *
 * @param [in] dbt1 "DBT" structure, representing the application supplied key.
 * @param [in] dbt2 "DBT" structure, representing the current btree's key.
 * @return 0 if dbt1 is equal to dbt2, \n
 *         -1 if dbt1 is less than dbt2, \n
 *         +1 if dbt1 is greater than dbt2.
 */
typedef int (*compare_callback) (DB *, const DBT * dbt1, const DBT * dbt2);

/**
 * Application-specified function to create a key for secondary index from key/data of primary table.
 *
 * @param [in]  key    "DBT" structure, referencing the primary key.
 * @param [in]  data   "DBT" structure, referencing the primary data.
 * @param [out] result Zeroed "DBT" structure, which the callback function should fill in.
 * @return 0 on success, \n
 *         a non-zero value if index should not contain a record for specified primary key/data pair.
 */
typedef int (*index_callback) (DB *, const DBT * key, const DBT * data, DBT * result);

/**
 * Application-specified function to update a data from primary table based on key/data from foreign one.
 *
 * @param [in]     pkey    "DBT" structure, referencing the primary key.
 * @param [in,out] pdata   "DBT" structure, referencing the primary data.
 * @param [in]     fkey    "DBT" structure, referencing the foreign key.
 * @param [out]    changed A pointer to a boolean value, indicated whether the primary data has been changed.
 * @return 0 on success, \n
 *         a non-zero value if primary data cannot be updated.
 */
typedef int (*nullify_callback) (DB *, const DBT * pkey, DBT * pdata, const DBT * fkey, int * changed);

//--------------------------------------------------------------------------------------------------
//  Static functions.
//--------------------------------------------------------------------------------------------------

// Berkeley DB version.
BDB_EXPORT void version (int * major, int * minor, int * patch = NULL);

/** @defgroup serialization Serialization between Berkeley DB and Protocol Buffers. */
//@{
BDB_EXPORT void serialize   (const Message * from, DBT * to);
BDB_EXPORT void unserialize (const DBT * from, Message * to);
BDB_EXPORT void release     (DBT * dbt);
//@}

/** @private */
void* malloc  (size_t size);
/** @private */
void* realloc (void * ptr, size_t size);
/** @private */
void  free    (void * ptr);

//--------------------------------------------------------------------------------------------------
//  Static variables.
//--------------------------------------------------------------------------------------------------

/** @private Library's logger. */
static log4cplus::Logger logger = log4cplus::Logger::getInstance(BDB_LOGGER_PORT);

//--------------------------------------------------------------------------------------------------
//  BDB classes.
//--------------------------------------------------------------------------------------------------

/**
 * BDB exception.
 */
class exception
{
public:

    BDB_EXPORT exception  (int error);     /**< @private */
    BDB_EXPORT ~exception () throw ();     /**< @private */

    /**
     * @return Exception's error code.
     */
    inline int error () { return m_error; }

protected:

    int m_error;    /**< @private */
};

/**
 * User database.
 */
class database
{
    friend class sequence;
    friend class table;
    friend class index;
    friend class recordset;

public:

    BDB_EXPORT database  (const char * home, bool create = false);
    BDB_EXPORT ~database () throw ();

    BDB_EXPORT sequence * add_sequence (const char * name, bool create = false);
    BDB_EXPORT table    * add_table    (const char * name, compare_callback fn_cmp, bool create = false);

    BDB_EXPORT void begin_transaction    ();
    BDB_EXPORT void commit_transaction   ();
    BDB_EXPORT void rollback_transaction ();

protected:

    string               m_home;        /**< @private Home path of the database.  */
    DB_ENV             * m_env;         /**< @private DB environment.             */
    DB                 * m_seq;         /**< @private Sequences database.         */
    stack <DB_TXN*>      m_txns;        /**< @private Active nested transactions. */
    vector <sequence*>   m_sequences;   /**< @private List of database sequences. */
    vector <table*>      m_tables;      /**< @private List of database tables.    */
};

/**
 * Sequence of autogenerating unique IDs.
 */
class sequence
{
    friend class database;

protected:

    sequence  (database * db, const char * name, bool create = false);
    ~sequence () throw ();

public:

    BDB_EXPORT int64_t id ();

protected:

    database    * m_database;   /**< @private Master database. */
    DB_SEQUENCE * m_seq;        /**< @private A sequence.      */
};

/**
 * Database table.
 */
class table
{
    friend class database;
    friend class index;
    friend class recordset;

protected:

    table  (const char * name);
    table  (database * db, const char * name, compare_callback fn_cmp, bool create = false);
    ~table () throw ();

public:

    BDB_EXPORT index * add_index (const char * name,
                                  index_callback fn_idx,
                                  compare_callback fn_cmp,
                                  bool unique = false);

    BDB_EXPORT bool exists (const Message * key                      );
    BDB_EXPORT void remove (const Message * key                      );
    BDB_EXPORT void insert (const Message * key, const Message * data);
    BDB_EXPORT void update (const Message * key, const Message * data);

    BDB_EXPORT void select (const Message * key, Message * data);

protected:

    /** @private */
    inline string get_filename () { return m_name + ".db"; }

protected:

    string             m_name;          /**< @private Name of the table.         */
    DB               * m_db;            /**< @private Berkeley DB database.      */
    database         * m_database;      /**< @private Master database.           */
    compare_callback   m_callback;      /**< @private Keys comparision function. */
    vector <index*>    m_indexes;       /**< @private List of table indexes.     */
};

/**
 * Index of database table.
 */
class index : protected table
{
    friend class table;
    friend class recordset;

protected:

    index (table * tbl,
           const char * name,
           index_callback fn_idx,
           compare_callback fn_cmp,
           compare_callback fn_dup,
           bool unique = false);

public:

    BDB_EXPORT void add_foreign (table * foreign, bool cascade = true);
    BDB_EXPORT void add_foreign (table * foreign, nullify_callback nullify);

    BDB_EXPORT bool exists (const Message * key);

protected:

    /** @private */
    inline string get_filename () { return m_name + ".ix"; }
};

/**
 * Data recordset.
 */
class recordset
{
public:

    BDB_EXPORT recordset  (table * tbl);
    BDB_EXPORT recordset  (index * idx);
    BDB_EXPORT recordset  (index * idx, const Message * key);
    BDB_EXPORT recordset  (table * tbl, const joinlist & list);
    BDB_EXPORT ~recordset () throw ();

    BDB_EXPORT bool fetch  (Message * key, Message * data);
    BDB_EXPORT void rewind ();

protected:

    DBC  * m_cursor;    /**< @private Cursor.                       */
    DBT  * m_key;       /**< @private Search key.                   */
    int    m_type;      /**< @private Type of recordset.            */
    bool   m_isset;     /**< @private Whether the recordset is set. */
};

}

#endif  // BDB_H
