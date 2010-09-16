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

/** @file bdb/src/index.cc
 * Contains implementation of class "bdb::index".
 * @author Artem Rodygin
 */

#include <bdb.h>

// Standard C/C++ Libraries
#include <exception>
#include <string>

// Protocol Buffers
#include <google/protobuf/message.h>

// Berkeley DB
#include <db.h>

// Check for Berkeley DB version
#if (DB_VERSION_MAJOR != 4) || (DB_VERSION_MINOR < 7)
#error Berkeley DB 4.7 or later is required.
#endif

// C++ Logging Library
#include <log4cplus/logger.h>

//--------------------------------------------------------------------------------------------------
//  Implementation of class "bdb::index".
//--------------------------------------------------------------------------------------------------

namespace bdb
{

using std::string;

using google::protobuf::Message;

//--------------------------------------------------------------------------------------------------
//  Constructors/destructors.
//--------------------------------------------------------------------------------------------------

/**
 * Opens the index, automatically creating it if required.
 *
 * @throw bdb::exception BDB_ERROR_UNKNOWN - unknown error.
 */
index::index (table            * tbl,       /**< [in] Master table.                                                  */
              const char       * name,      /**< [in] Name of the index.                                             */
              index_callback     fn_idx,    /**< [in] Indexing function (see "Db::associate()").                     */
              compare_callback   fn_cmp,    /**< [in] Index comparision function (see "Db::set_bt_compare()").       */
              compare_callback   fn_dup,    /**< [in] Duplicates comparision function (see "Db::set_dup_compare()"). */
              bool               unique)    /**< [in] Whether the index should contain unique keys only.             */
  : table(name)
{
    LOG4CPLUS_TRACE(logger, "[bdb::index::index] ENTER");
    LOG4CPLUS_DEBUG(logger, "[bdb::index::index] name = " << name);
    LOG4CPLUS_DEBUG(logger, "[bdb::index::index] unique = " << unique);

    m_database = tbl->m_database;

    int res = db_create(&m_db, tbl->m_db->get_env(tbl->m_db), 0);

    if (fn_cmp != NULL)
    {
        if (res == 0) res = m_db->set_bt_compare(m_db, fn_cmp);
    }

    if (!unique)
    {
        if (fn_dup != NULL)
        {
            if (res == 0) res = m_db->set_dup_compare(m_db, fn_dup);
        }

        if (res == 0) res = m_db->set_flags(m_db, DB_DUPSORT);
    }

    if (res == 0)
    {
        res = m_db->open(m_db,
                         m_database->m_txns.top(),
                         get_filename().c_str(),
                         m_name.c_str(),
                         DB_BTREE,
                         DB_THREAD | DB_CREATE,
                         0);
    }

    if (res == 0) res = tbl->m_db->associate(tbl->m_db, m_database->m_txns.top(), m_db, fn_idx, DB_CREATE);

    if (res != 0)
    {
        LOG4CPLUS_WARN(logger, "[bdb::index::index] " << db_strerror(res));
        if (m_db != NULL) m_db->close(m_db, 0);
        throw exception(BDB_ERROR_UNKNOWN);
    }

    LOG4CPLUS_TRACE(logger, "[bdb::index::index] EXIT");
}

//--------------------------------------------------------------------------------------------------
//  Public interface.
//--------------------------------------------------------------------------------------------------

/**
 * Adds a foreign key to specified table.
 * The foreign key means that any key, inserted into this index, must exist in the foreign table.
 *
 * @throw bdb::exception BDB_ERROR_UNKNOWN - unknown error.
 */
void index::add_foreign (table * foreign,   /**< [in] Master table. */
                         bool    cascade)   /**< [in] Whether a cascade removal should be enabled: \n
                                                      true  - key from this index will be deleted automatically
                                                              when related key in master table is being deleted, \n
                                                      false - it's unable to delete related key in master table
                                                              until same key is present in the index. */
{
    LOG4CPLUS_TRACE(logger, "[bdb::index::add_foreign] ENTER (cascade)");
    LOG4CPLUS_DEBUG(logger, "[bdb::index::add_foreign] cascade = " << cascade);

    int res = foreign->m_db->associate_foreign(foreign->m_db, m_db, NULL,
                                               cascade ? DB_FOREIGN_CASCADE : DB_FOREIGN_ABORT);

    if (res != 0)
    {
        LOG4CPLUS_WARN(logger, "[bdb::index::add_foreign] " << db_strerror(res));
        throw exception(BDB_ERROR_UNKNOWN);
    }

    LOG4CPLUS_TRACE(logger, "[bdb::index::add_foreign] EXIT");
}

/**
 * Adds a foreign key to specified table.
 * The foreign key means that any key, inserted into this index, must exist in the foreign table.
 *
 * @throw bdb::exception BDB_ERROR_UNKNOWN - unknown error.
 */
void index::add_foreign (table            * foreign,    /**< [in] Master table. */
                         nullify_callback   nullify)    /**< Nullification callback function, which is used
                                                             during cascade removal to update affected data. */
{
    LOG4CPLUS_TRACE(logger, "[bdb::index::add_foreign] ENTER (nullify)");

    int res = foreign->m_db->associate_foreign(foreign->m_db, m_db, nullify, DB_FOREIGN_NULLIFY);

    if (res != 0)
    {
        LOG4CPLUS_WARN(logger, "[bdb::index::add_foreign] " << db_strerror(res));
        throw exception(BDB_ERROR_UNKNOWN);
    }

    LOG4CPLUS_TRACE(logger, "[bdb::index::add_foreign] EXIT");
}

/**
 * Checks whether a record with specified key exists in the index.
 * <strong>NOTE:</strong> index can have duplicated keys.
 *
 * @return true  - at least one record exists, \n
 *         false - no record is found.
 */
bool index::exists (const Message * key)    /**< [in] Key of the record to be checked. */
{
    LOG4CPLUS_TRACE(logger, "[bdb::index::exists] ENTER");
    bool res = table::exists(key);
    LOG4CPLUS_TRACE(logger, "[bdb::index::exists] EXIT = " << res);

    return res;
}

}

//--------------------------------------------------------------------------------------------------
