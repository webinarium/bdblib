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
//  Artem Rodygin           2009-12-09      Initial creation.
//--------------------------------------------------------------------------------------------------

/** @file bdb/src/recordset.cc
 * Contains implementation of class "bdb::recordset".
 * @author Artem Rodygin
 */

#include <bdb.h>

// Standard C/C++ Libraries
#include <exception>
#include <vector>

// Protocol Buffers
#include <google/protobuf/message.h>

// Berkeley DB
#include <db.h>

// Check for Berkeley DB version
#if (DB_VERSION_MAJOR < 4) || (DB_VERSION_MAJOR == 4) && (DB_VERSION_MINOR < 7)
#error Berkeley DB 4.7 or later is required.
#endif

// C++ Logging Library
#include <log4cplus/logger.h>

//--------------------------------------------------------------------------------------------------
//  Implementation of class "bdb::recordset".
//--------------------------------------------------------------------------------------------------

/** @private Types of recordset. */
//@{
#define BDB_RS_TABLE        1   /**< Contains all records from specified table.                     */
#define BDB_RS_DUPLICATES   2   /**< Contains all records from specified index.                     */
#define BDB_RS_UNIQUE       3   /**< Contains all records with particular key from specified index. */
#define BDB_RS_JOIN         4   /**< Contains all records from natural join of several indexes.     */
//@}

namespace bdb
{

using std::vector;

using google::protobuf::Message;

//--------------------------------------------------------------------------------------------------
//  Constructors/destructors.
//--------------------------------------------------------------------------------------------------

/**
 * Opens the recordset, which contains all records from specified table.
 *
 * @throw bdb::exception BDB_ERROR_UNKNOWN - unknown error.
 */
recordset::recordset (table * tbl)      /**< [in] Table with source data. */
  : m_cursor(NULL),
    m_key(NULL),
    m_type(BDB_RS_TABLE),
    m_isset(false)
{
    LOG4CPLUS_TRACE(logger, "[bdb::recordset::recordset] ENTER = BDB_RS_TABLE");

    int res = tbl->m_db->cursor(tbl->m_db, tbl->m_database->m_txns.top(), &m_cursor, DB_READ_COMMITTED);

    if (res != 0)
    {
        LOG4CPLUS_WARN(logger, "[bdb::recordset::recordset] " << db_strerror(res));
        if (m_cursor != NULL) m_cursor->close(m_cursor);
        throw exception(BDB_ERROR_UNKNOWN);
    }

    LOG4CPLUS_TRACE(logger, "[bdb::recordset::recordset] EXIT");
}

/**
 * Opens the recordset, which contains all records from specified index.
 *
 * @throw bdb::exception BDB_ERROR_UNKNOWN - unknown error.
 */
recordset::recordset (index * idx)      /**< [in] Index with source data. */
  : m_cursor(NULL),
    m_key(NULL),
    m_type(BDB_RS_DUPLICATES),
    m_isset(false)
{
    LOG4CPLUS_TRACE(logger, "[bdb::recordset::recordset] ENTER = BDB_RS_DUPLICATES");

    int res = idx->m_db->cursor(idx->m_db, idx->m_database->m_txns.top(), &m_cursor, DB_READ_COMMITTED);

    if (res != 0)
    {
        LOG4CPLUS_WARN(logger, "[bdb::recordset::recordset] " << db_strerror(res));
        if (m_cursor != NULL) m_cursor->close(m_cursor);
        throw exception(BDB_ERROR_UNKNOWN);
    }

    LOG4CPLUS_TRACE(logger, "[bdb::recordset::recordset] EXIT");
}

/**
 * Opens the recordset, which contains all records with particular key from specified index.
 *
 * @throw bdb::exception BDB_ERROR_UNKNOWN - unknown error.
 */
recordset::recordset (index         * idx,      /**< [in] Index with source data. */
                      const Message * key)      /**< [in] Required index key.     */
  : m_cursor(NULL),
    m_key(NULL),
    m_type(BDB_RS_UNIQUE),
    m_isset(false)
{
    LOG4CPLUS_TRACE(logger, "[bdb::recordset::recordset] ENTER = BDB_RS_UNIQUE");

    int res = idx->m_db->cursor(idx->m_db, idx->m_database->m_txns.top(), &m_cursor, DB_READ_COMMITTED);

    if (res != 0)
    {
        LOG4CPLUS_WARN(logger, "[bdb::recordset::recordset] " << db_strerror(res));
        if (m_cursor != NULL) m_cursor->close(m_cursor);
        throw exception(BDB_ERROR_UNKNOWN);
    }

    m_key = new DBT;
    assert(m_key != NULL);
    serialize(key, m_key);

    LOG4CPLUS_TRACE(logger, "[bdb::recordset::recordset] EXIT");
}

/**
 * Opens the recordset, which is a natural join of all recordsets, provided in the specified list.
 * All provided recordsets must be gained from indexes, which belong to the same table.
 *
 * @throw bdb::exception BDB_ERROR_UNKNOWN - unknown error.
 */
recordset::recordset (table          * tbl,     /**< [in] Table with source data.     */
                      const joinlist & list)    /**< [in] List of recordsets to join. */
  : m_cursor(NULL),
    m_key(NULL),
    m_type(BDB_RS_JOIN),
    m_isset(true)
{
    LOG4CPLUS_TRACE(logger, "[bdb::recordset::recordset] ENTER = BDB_RS_JOIN");

    DBC ** curslist = NULL;

    int count = (int) list.size();
    curslist = (DBC **) malloc((count + 1) * sizeof(DBC));
    assert(curslist != NULL);
    curslist[count] = NULL;

    for (int i = 0; i < count; i++)
    {
        recordset * rs = list[i];

        if (rs->m_key == NULL)
        {
            LOG4CPLUS_WARN(logger, "[bdb::recordset::recordset] Source index is not unique.");
            free((void *) curslist);
            throw exception(BDB_ERROR_UNKNOWN);
        }

        DBT k, d;

        memset(&k, 0, sizeof(DBT));
        memset(&d, 0, sizeof(DBT));

        if (rs->m_cursor->pget(rs->m_cursor, rs->m_key, &k, &d, DB_SET) == 0)
        {
            rs->m_isset = true;
        }
        else
        {
            m_isset = false;
        }

        curslist[i] = rs->m_cursor;
    }

    int res = tbl->m_db->join(tbl->m_db, curslist, &m_cursor, 0);

    free((void *) curslist);

    if (res != 0)
    {
        LOG4CPLUS_WARN(logger, "[bdb::recordset::recordset] " << db_strerror(res));
        throw exception(BDB_ERROR_UNKNOWN);
    }

    LOG4CPLUS_TRACE(logger, "[bdb::recordset::recordset] EXIT");
}

/**
 * Closes the recordset.
 */
recordset::~recordset () throw ()
{
    LOG4CPLUS_TRACE(logger, "[bdb::recordset::~recordset] ENTER");

    if (m_key != NULL)
    {
        release(m_key);
        delete m_key;
    }

    if (m_cursor != NULL)
    {
        m_cursor->close(m_cursor);
    }

    LOG4CPLUS_TRACE(logger, "[bdb::recordset::~recordset] EXIT");
}

//--------------------------------------------------------------------------------------------------
//  Public interface.
//--------------------------------------------------------------------------------------------------

/**
 * Fetches the next record from the recordset.
 *
 * @throw bdb::exception BDB_ERROR_UNKNOWN - unknown error.
 *
 * @return true  - record is successfully fetched, \n
 *         false - no more records to fetch.
 */
bool recordset::fetch (Message * key,       /**< [out] Key of fetched record.  */
                       Message * data)      /**< [out] Data of fetched record. */
{
    LOG4CPLUS_TRACE(logger, "[bdb::recordset::fetch] ENTER");

    int res = DB_NOTFOUND;

    DBT k, d, sk;

    k.flags = DB_DBT_MALLOC;
    d.flags = DB_DBT_MALLOC;

    LOG4CPLUS_DEBUG(logger, "[bdb::recordset::fetch] type = " << m_type);

    switch (m_type)
    {
        case BDB_RS_TABLE:
            res = m_cursor->get(m_cursor, &k, &d, (!m_isset ? DB_FIRST : DB_NEXT));
            break;

        case BDB_RS_DUPLICATES:
            sk.flags = DB_DBT_MALLOC;
            res = m_cursor->pget(m_cursor, &sk, &k, &d, (!m_isset ? DB_FIRST : DB_NEXT));
            if (res == 0) release(&sk);
            break;

        case BDB_RS_UNIQUE:
            res = m_cursor->pget(m_cursor, m_key, &k, &d, (!m_isset ? DB_SET : DB_NEXT_DUP));
            break;

        case BDB_RS_JOIN:
            if (m_isset) res = m_cursor->get(m_cursor, &k, &d, 0);
            break;

        default:
            throw exception(BDB_ERROR_UNKNOWN);
    }

    if (res == 0)
    {
        m_isset = true;

        unserialize(&k, key);
        unserialize(&d, data);

        release(&k);
        release(&d);

        LOG4CPLUS_TRACE(logger, "[bdb::recordset::fetch] EXIT = true");

        return true;
    }

    LOG4CPLUS_TRACE(logger, "[bdb::recordset::fetch] EXIT = false");

    return false;
}

/**
 * Rewinds cursor of the recordset to the first record.
 * Cannot be used on joins (always throws an exception there).
 *
 * @throw bdb::exception BDB_ERROR_UNKNOWN - invalid recordset.
 */
void recordset::rewind ()
{
    LOG4CPLUS_TRACE(logger, "[bdb::recordset::rewind] ENTER");

    if (m_type == BDB_RS_JOIN)
    {
        LOG4CPLUS_WARN(logger, "[bdb::recordset::rewind] Rewind cannot be used on joins.");
        throw exception(BDB_ERROR_UNKNOWN);
    }

    m_isset = false;

    LOG4CPLUS_TRACE(logger, "[bdb::recordset::rewind] EXIT");
}

}

//--------------------------------------------------------------------------------------------------
