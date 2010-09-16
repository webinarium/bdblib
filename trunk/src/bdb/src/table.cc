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

/** @file bdb/src/table.cc
 * Contains implementation of class "bdb::table".
 * @author Artem Rodygin
 */

#include <bdb.h>

// Standard C/C++ Libraries
#include <cerrno>
#include <exception>
#include <string>
#include <vector>

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
//  Implementation of class "bdb::table".
//--------------------------------------------------------------------------------------------------

namespace bdb
{

using std::string;
using std::vector;

using google::protobuf::Message;

//--------------------------------------------------------------------------------------------------
//  Constructors/destructors.
//--------------------------------------------------------------------------------------------------

/**
 * Default empty constructor - does nothing.
 * Purposed for usage in a child class, if one needs to rewrite
 * main parent constructor (see below) from scratch.
 */
table::table (const char * name)    /**< [in] Name of the table. */
  : m_name(string(name)),
    m_db(NULL)
{
    // do nothing
}

/**
 * Opens the table.
 * If "create" is "true" and table doesn't exist, then creates it.
 *
 * @throw bdb::exception BDB_ERROR_NOT_FOUND - the table is not found.
 * @throw bdb::exception BDB_ERROR_EXISTS    - the table already exists.
 * @throw bdb::exception BDB_ERROR_UNKNOWN   - unknown error.
 */
table::table (database         * db,        /**< [in] Database of the table.                                  */
              const char       * name,      /**< [in] Name of the table.                                      */
              compare_callback   fn_cmp,    /**< [in] Keys comparision function (see "Db::set_bt_compare()"). */
              bool               create)    /**< [in] Whether to create the table if it doesn't exist.        */
  : m_name(string(name)),
    m_db(NULL),
    m_database(db),
    m_callback(fn_cmp)
{
    LOG4CPLUS_TRACE(logger, "[bdb::table::table] ENTER");
    LOG4CPLUS_DEBUG(logger, "[bdb::table::table] name = " << name);
    LOG4CPLUS_DEBUG(logger, "[bdb::table::table] create = " << create);

    int res = db_create(&m_db, db->m_env, 0);

    if (fn_cmp != NULL)
    {
        if (res == 0) res = m_db->set_bt_compare(m_db, fn_cmp);
    }

    if (res == 0)
    {
        u_int32_t flags = DB_THREAD;
        if (create) flags |= DB_CREATE | DB_EXCL;

        res = m_db->open(m_db,
                         m_database->m_txns.top(),
                         get_filename().c_str(),
                         m_name.c_str(),
                         DB_BTREE,
                         flags,
                         0);
    }

    if (res != 0)
    {
        LOG4CPLUS_WARN(logger, "[bdb::table::table] " << db_strerror(res));

        if (m_db != NULL) m_db->close(m_db, 0);

        switch (res)
        {
            case ENOENT:    throw exception(BDB_ERROR_NOT_FOUND);
            case EEXIST:    throw exception(BDB_ERROR_EXISTS);
            default:        throw exception(BDB_ERROR_UNKNOWN);
        }
    }

    LOG4CPLUS_TRACE(logger, "[bdb::table::table] EXIT");
}

/**
 * Closes the table and all associated indexes.
 */
table::~table () throw ()
{
    LOG4CPLUS_TRACE(logger, "[bdb::table::~table] ENTER");

    // close all associated indexes
    for (unsigned int i = 0; i < m_indexes.size(); i++)
    {
        delete m_indexes[i];
    }

    m_db->close(m_db, 0);

    LOG4CPLUS_TRACE(logger, "[bdb::table::~table] EXIT");
}

//--------------------------------------------------------------------------------------------------
//  Public interface.
//--------------------------------------------------------------------------------------------------

/**
 * Adds specified index to the table and opens the index.
 * If index doesn't exist yet, then creates it.
 *
 * @throw bdb::exception BDB_ERROR_UNKNOWN - unknown error.
 */
index * table::add_index (const char       * name,      /**< [in] Name of the index.                                       */
                          index_callback     fn_idx,    /**< [in] Indexing function (see "Db::associate()").               */
                          compare_callback   fn_cmp,    /**< [in] Index comparision function (see "Db::set_bt_compare()"). */
                          bool               unique)    /**< [in] Whether new index should contain unique keys only.       */
{
    LOG4CPLUS_TRACE(logger, "[bdb::table::add_index] ENTER");
    LOG4CPLUS_DEBUG(logger, "[bdb::table::add_index] name = " << name);
    LOG4CPLUS_DEBUG(logger, "[bdb::table::add_index] unique = " << unique);

    index * i = NULL;

    try
    {
        i = new index(this, name, fn_idx, fn_cmp, m_callback, unique);
        assert(i != NULL);
        m_indexes.push_back(i);
    }
    catch (...)
    {
        throw;
    }

    LOG4CPLUS_TRACE(logger, "[bdb::table::add_index] EXIT");

    return i;
}

/**
 * Checks whether a record with specified key exists in the table.
 * <strong>NOTE:</strong> all keys in the table are unique.
 *
 * @return true  - the record exists, \n
 *         false - the record is not found.
 */
bool table::exists (const Message * key)    /**< [in] Key of the record to be checked. */
{
    LOG4CPLUS_TRACE(logger, "[bdb::table::exists] ENTER");

    DBT k;

    serialize(key, &k);
    int res = m_db->exists(m_db, m_database->m_txns.top(), &k, DB_READ_COMMITTED);
    release(&k);

    LOG4CPLUS_TRACE(logger, "[bdb::table::exists] EXIT = " << (res == 0));

    return (res == 0);
}

/**
 * Deletes specified existing record.
 * <strong>NOTE:</strong> all keys in the table are unique.
 *
 * @throw bdb::exception BDB_ERROR_NOT_FOUND   - the record is not found.
 * @throw bdb::exception BDB_ERROR_FOREIGN_KEY - this table has a foreign constraint to another one,
 *                                               and slave table contains specified key.
 * @throw bdb::exception BDB_ERROR_UNKNOWN     - unknown error.
 */
void table::remove (const Message * key)    /**< [in] Key of the record to be deleted. */
{
    LOG4CPLUS_TRACE(logger, "[bdb::table::remove] ENTER");

    DBT k;

    serialize(key, &k);
    int res = m_db->del(m_db, m_database->m_txns.top(), &k, 0);
    release(&k);

    if (res != 0)
    {
        LOG4CPLUS_WARN(logger, "[bdb::table::remove] " << db_strerror(res));

        switch (res)
        {
            case DB_NOTFOUND:           throw exception(BDB_ERROR_NOT_FOUND);
            case DB_FOREIGN_CONFLICT:   throw exception(BDB_ERROR_FOREIGN_KEY);
            default:                    throw exception(BDB_ERROR_UNKNOWN);
        }
    }

    LOG4CPLUS_TRACE(logger, "[bdb::table::remove] EXIT");
}

/**
 * Inserts specified new record into the table.
 * <strong>NOTE:</strong> all keys in the table are unique.
 *
 * @throw bdb::exception BDB_ERROR_EXISTS      - a record with specified key already exists,
 *                                               or unique index already contains specified value.
 * @throw bdb::exception BDB_ERROR_FOREIGN_KEY - this table has a foreign constraint to another one,
 *                                               and master table doesn't contain specified key.
 * @throw bdb::exception BDB_ERROR_UNKNOWN     - unknown error.
 */
void table::insert (const Message * key,    /**< [in] Key of new record.  */
                    const Message * data)   /**< [in] Data of new record. */
{
    LOG4CPLUS_TRACE(logger, "[bdb::table::insert] ENTER");

    DBT k, d;

    serialize(key,  &k);
    serialize(data, &d);

    int res = m_db->put(m_db, m_database->m_txns.top(), &k, &d, DB_NOOVERWRITE);

    release(&k);
    release(&d);

    if (res != 0)
    {
        LOG4CPLUS_WARN(logger, "[bdb::table::insert] " << db_strerror(res));

        switch (res)
        {
            case EINVAL:                throw exception(BDB_ERROR_EXISTS);
            case DB_KEYEXIST:           throw exception(BDB_ERROR_EXISTS);
            case DB_FOREIGN_CONFLICT:   throw exception(BDB_ERROR_FOREIGN_KEY);
            default:                    throw exception(BDB_ERROR_UNKNOWN);
        }
    }

    LOG4CPLUS_TRACE(logger, "[bdb::table::insert] EXIT");
}

/**
 * Updates specified existing record.
 * <strong>NOTE:</strong> all keys in the table are unique.
 *
 * @throw bdb::exception BDB_ERROR_NOT_FOUND   - the record is not found.
 * @throw bdb::exception BDB_ERROR_FOREIGN_KEY - this table has a foreign constraint to another one,
 *                                               and master table doesn't contain specified new key.
 * @throw bdb::exception BDB_ERROR_UNKNOWN     - unknown error.
 */
void table::update (const Message * key,    /**< [in] Key of the record to be updated. */
                    const Message * data)   /**< [in] New data of the record.          */
{
    LOG4CPLUS_TRACE(logger, "[bdb::table::update] ENTER");

    DBT k, d;

    serialize(key,  &k);
    serialize(data, &d);

    int res = m_db->exists(m_db, m_database->m_txns.top(), &k, DB_READ_COMMITTED);

    if (res != 0)
    {
        LOG4CPLUS_WARN(logger, "[bdb::table::update] " << db_strerror(res));

        release(&k);
        release(&d);

        throw exception(BDB_ERROR_NOT_FOUND);
    }

    res = m_db->put(m_db, m_database->m_txns.top(), &k, &d, 0);

    release(&k);
    release(&d);

    if (res != 0)
    {
        LOG4CPLUS_WARN(logger, "[bdb::table::update] " << db_strerror(res));

        switch (res)
        {
            case DB_FOREIGN_CONFLICT:   throw exception(BDB_ERROR_FOREIGN_KEY);
            default:                    throw exception(BDB_ERROR_UNKNOWN);
        }
    }

    LOG4CPLUS_TRACE(logger, "[bdb::table::update] EXIT");
}

/**
 * Finds a record with specified key and returns its data.
 * <strong>NOTE:</strong> all keys in the table are unique.
 *
 * @throw bdb::exception BDB_ERROR_NOT_FOUND - the record is not found.
 * @throw bdb::exception BDB_ERROR_UNKNOWN   - unknown error.
 */
void table::select (const Message * key,    /**< [in]  Key of the record to be retrieved. */
                    Message       * data)   /**< [out] Data of the found record.          */
{
    LOG4CPLUS_TRACE(logger, "[bdb::table::select] ENTER");

    DBT k, d;

    memset(&d, 0, sizeof(DBT));
    d.flags = DB_DBT_MALLOC;

    serialize(key, &k);
    int res = m_db->get(m_db, m_database->m_txns.top(), &k, &d, DB_READ_COMMITTED);
    release(&k);

    if (res != 0)
    {
        LOG4CPLUS_WARN(logger, "[bdb::table::select] " << db_strerror(res));

        switch (res)
        {
            case DB_NOTFOUND:   throw exception(BDB_ERROR_NOT_FOUND);
            default:            throw exception(BDB_ERROR_UNKNOWN);
        }
    }

    unserialize(&d, data);
    release(&d);

    LOG4CPLUS_TRACE(logger, "[bdb::table::select] EXIT");
}

}

//--------------------------------------------------------------------------------------------------
