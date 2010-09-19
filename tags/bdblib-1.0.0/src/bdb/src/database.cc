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

/** @file bdb/src/database.cc
 * Contains implementation of class "bdb::database".
 * @author Artem Rodygin
 */

#include <bdb.h>

// Standard C/C++ Libraries
#include <cerrno>
#include <exception>
#include <stack>
#include <string>
#include <vector>

// Boost C++ Libraries
#include <boost/interprocess/sync/interprocess_mutex.hpp>
#include <boost/interprocess/sync/scoped_lock.hpp>

// Berkeley DB
#include <db.h>

// Check for Berkeley DB version
#if (DB_VERSION_MAJOR != 4) || (DB_VERSION_MINOR < 7)
#error Berkeley DB 4.7 or later is required.
#endif

// C++ Logging Library
#include <log4cplus/logger.h>

//--------------------------------------------------------------------------------------------------
//  Implementation of class "bdb::database".
//--------------------------------------------------------------------------------------------------

namespace bdb
{

using std::stack;
using std::string;
using std::vector;

using boost::interprocess::interprocess_mutex;
using boost::interprocess::scoped_lock;

//--------------------------------------------------------------------------------------------------
//  Constructors/destructors.
//--------------------------------------------------------------------------------------------------

/**
 * Opens the database.
 * If "create" is "true" and database doesn't exist, then creates it.
 *
 * @throw bdb::exception BDB_ERROR_NOT_FOUND - the database home is not found, or the database doesn't exist.
 * @throw bdb::exception BDB_ERROR_EXISTS    - the database already exists (cannot be created).
 * @throw bdb::exception BDB_ERROR_UNKNOWN   - unknown error.
 */
database::database (const char * home,      /**< [in] Home path of the database.                          */
                    bool         create)    /**< [in] Whether to create the database if it doesn't exist. */
  : m_home(string(home)),
    m_env(NULL),
    m_seq(NULL)
{
    LOG4CPLUS_TRACE(logger, "[bdb::database::database] ENTER");
    LOG4CPLUS_DEBUG(logger, "[bdb::database::database] home = " << home);
    LOG4CPLUS_DEBUG(logger, "[bdb::database::database] create = " << create);

    // database flags
    u_int32_t flags = DB_THREAD         // multi-threaded access
                    | DB_INIT_LOCK      // locking for concurrent applications
                    | DB_INIT_LOG       // recoverability
                    | DB_INIT_MPOOL     // in-memory cache
                    | DB_INIT_TXN;      // transaction subsystem

    if (create) flags |= DB_CREATE;

    // open/create the environment
    int res = db_env_create(&m_env, 0);

    if (res == 0) res = m_env->set_alloc(m_env, malloc, realloc, free);
    if (res == 0) res = m_env->open(m_env, m_home.c_str(), flags, 0);

    // create top-level transaction
    DB_TXN * txn = NULL;
    if (res == 0) res = m_env->txn_begin(m_env, NULL, &txn, DB_READ_COMMITTED | DB_TXN_SYNC);
    if (res == 0) m_txns.push(txn);

    // open/create the sequences database
    flags = DB_THREAD;

    if (create) flags |= DB_CREATE | DB_EXCL;

    if (res == 0) res = db_create(&m_seq, m_env, 0);
    if (res == 0) res = m_seq->open(m_seq, txn, "__seq.db", "__seq", DB_BTREE, flags, 0);

    // handle error, if any
    if (res != 0)
    {
        LOG4CPLUS_WARN(logger, "[bdb::database::database] " << db_strerror(res));

        if (m_seq != NULL) m_seq->close(m_seq, 0);
        if (txn   != NULL) txn->abort(txn);
        if (m_env != NULL) m_env->close(m_env, 0);

        switch (res)
        {
            case ENOENT:    throw exception(BDB_ERROR_NOT_FOUND);
            case EEXIST:    throw exception(BDB_ERROR_EXISTS);
            default:        throw exception(BDB_ERROR_UNKNOWN);
        }
    }

    LOG4CPLUS_TRACE(logger, "[bdb::database::database] EXIT");
}

/**
 * Closes the database and all associated sequences, tables, and indexes.
 * Any active transactions will be rolled back.
 */
database::~database () throw ()
{
    LOG4CPLUS_TRACE(logger, "[bdb::database::~database] ENTER");

    // lock stack of active transactions
    interprocess_mutex mutex;
    scoped_lock <interprocess_mutex> lock(mutex);

    // rollback non-completed transactions (besides top-level one)
    while (m_txns.size() != 1)
    {
        DB_TXN * txn = m_txns.top();
        m_txns.pop();
        txn->abort(txn);
    }

    // close all associated sequences
    for (unsigned int i = 0; i < m_sequences.size(); i++)
    {
        delete m_sequences[i];
    }

    // close all associated tables
    for (unsigned int i = 0; i < m_tables.size(); i++)
    {
        delete m_tables[i];
    }

    DB_TXN * txn = m_txns.top();

    if (m_seq != NULL) m_seq->close(m_seq, 0);
    txn->commit(txn, DB_TXN_SYNC);
    if (m_env != NULL) m_env->close(m_env, 0);

    LOG4CPLUS_TRACE(logger, "[bdb::database::~database] EXIT");
}

//--------------------------------------------------------------------------------------------------
//  Public interface.
//--------------------------------------------------------------------------------------------------

/**
 * Adds specified sequence to the database and opens the sequence.
 * If "create" is "true" and sequence doesn't exist, then creates it.
 *
 * @throw bdb::exception BDB_ERROR_NOT_FOUND - the sequence is not found.
 * @throw bdb::exception BDB_ERROR_EXISTS    - the sequence already exists (cannot be created).
 * @throw bdb::exception BDB_ERROR_UNKNOWN   - unknown error.
 */
sequence * database::add_sequence (const char * name,       /**< [in] Name of the sequence.                               */
                                   bool         create)     /**< [in] Whether to create the sequence if it doesn't exist. */
{
    LOG4CPLUS_TRACE(logger, "[bdb::database::add_sequence] ENTER");
    LOG4CPLUS_DEBUG(logger, "[bdb::database::add_sequence] name = " << name);
    LOG4CPLUS_DEBUG(logger, "[bdb::database::add_sequence] create = " << create);

    sequence * s = NULL;

    try
    {
        s = new sequence(this, name, create);
        assert(s != NULL);
        m_sequences.push_back(s);
    }
    catch (...)
    {
        throw;
    }

    LOG4CPLUS_TRACE(logger, "[bdb::database::add_sequence] EXIT");

    return s;
}

/**
 * Adds specified table to the database and opens the table.
 * If "create" is "true" and table doesn't exist, then creates it.
 *
 * @throw bdb::exception BDB_ERROR_NOT_FOUND - the table is not found.
 * @throw bdb::exception BDB_ERROR_EXISTS    - the table already exists (cannot be created).
 * @throw bdb::exception BDB_ERROR_UNKNOWN   - unknown error.
 */
table * database::add_table (const char       * name,       /**< [in] Name of the table.                                      */
                             compare_callback   fn_cmp,     /**< [in] Keys comparision function (see "Db::set_bt_compare()"). */
                             bool               create)     /**< [in] Whether to create the table if it doesn't exist.        */
{
    LOG4CPLUS_TRACE(logger, "[bdb::database::add_table] ENTER");
    LOG4CPLUS_DEBUG(logger, "[bdb::database::add_table] name = " << name);
    LOG4CPLUS_DEBUG(logger, "[bdb::database::add_table] create = " << create);

    table * t = NULL;

    try
    {
        t = new table(this, name, fn_cmp, create);
        assert(t != NULL);
        m_tables.push_back(t);
    }
    catch (...)
    {
        throw;
    }

    LOG4CPLUS_TRACE(logger, "[bdb::database::add_table] EXIT");

    return t;
}

/**
 * Begins new transaction.
 *
 * @throw bdb::exception BDB_ERROR_UNKNOWN - unknown error.
 */
void database::begin_transaction ()
{
    LOG4CPLUS_TRACE(logger, "[bdb::database::begin_transaction] ENTER");

    interprocess_mutex mutex;
    scoped_lock <interprocess_mutex> lock(mutex);

    DB_TXN * txn = NULL;

    int res = m_env->txn_begin(m_env, m_txns.top(), &txn, DB_READ_COMMITTED | DB_TXN_SYNC);

    if (res == 0)
    {
        m_txns.push(txn);
    }
    else
    {
        LOG4CPLUS_WARN(logger, "[bdb::database::begin_transaction] " << db_strerror(res));
        if (txn != NULL) txn->abort(txn);
        throw exception(BDB_ERROR_UNKNOWN);
    }

    LOG4CPLUS_TRACE(logger, "[bdb::database::begin_transaction] EXIT");
}

/**
 * Commits current transaction.
 *
 * @throw bdb::exception BDB_ERROR_NOT_FOUND - the database has no active transactions.
 * @throw bdb::exception BDB_ERROR_UNKNOWN   - unknown error.
 */
void database::commit_transaction ()
{
    LOG4CPLUS_TRACE(logger, "[bdb::database::commit_transaction] ENTER");

    interprocess_mutex mutex;
    scoped_lock <interprocess_mutex> lock(mutex);

    if (m_txns.size() == 1)
    {
        LOG4CPLUS_WARN(logger, "[bdb::database::commit_transaction] the database has no active transactions");
        throw exception(BDB_ERROR_NOT_FOUND);
    }

    DB_TXN * txn = m_txns.top();
    m_txns.pop();

    int res = txn->commit(txn, DB_TXN_SYNC);

    if (res != 0)
    {
        LOG4CPLUS_WARN(logger, "[bdb::database::commit_transaction] " << db_strerror(res));
        throw exception(BDB_ERROR_UNKNOWN);
    }

    LOG4CPLUS_TRACE(logger, "[bdb::database::commit_transaction] EXIT");
}

/**
 * Rolls back current transaction.
 *
 * @throw bdb::exception BDB_ERROR_NOT_FOUND - the database has no active transactions.
 * @throw bdb::exception BDB_ERROR_UNKNOWN   - unknown error.
 */
void database::rollback_transaction ()
{
    LOG4CPLUS_TRACE(logger, "[bdb::database::rollback_transaction] ENTER");

    interprocess_mutex mutex;
    scoped_lock <interprocess_mutex> lock(mutex);

    if (m_txns.size() == 1)
    {
        LOG4CPLUS_WARN(logger, "[bdb::database::rollback_transaction] the database has no active transactions");
        throw exception(BDB_ERROR_NOT_FOUND);
    }

    DB_TXN * txn = m_txns.top();
    m_txns.pop();

    int res = txn->abort(txn);

    if (res != 0)
    {
        LOG4CPLUS_WARN(logger, "[bdb::database::rollback_transaction] " << db_strerror(res));
        throw exception(BDB_ERROR_UNKNOWN);
    }

    LOG4CPLUS_TRACE(logger, "[bdb::database::rollback_transaction] EXIT");
}

}

//--------------------------------------------------------------------------------------------------
