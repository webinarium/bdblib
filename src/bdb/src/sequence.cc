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
//  Artem Rodygin           2009-12-24      Initial creation.
//--------------------------------------------------------------------------------------------------

/** @file bdb/src/sequence.cc
 * Contains implementation of class "bdb::sequence".
 * @author Artem Rodygin
 */

#include <bdb.h>

// Standard C/C++ Libraries
#include <cerrno>
#include <iostream>
#include <stdint.h>

// Berkeley DB
#include <db.h>

// Check for Berkeley DB version
#if (DB_VERSION_MAJOR < 4) || (DB_VERSION_MAJOR == 4) && (DB_VERSION_MINOR < 7)
#error Berkeley DB 4.7 or later is required.
#endif

// C++ Logging Library
#include <log4cplus/logger.h>

//--------------------------------------------------------------------------------------------------
//  Implementation of class "bdb::sequence".
//--------------------------------------------------------------------------------------------------

namespace bdb
{

//--------------------------------------------------------------------------------------------------
//  Constructors/destructors.
//--------------------------------------------------------------------------------------------------

/**
 * Opens the sequence.
 * If "create" is "true" and sequence doesn't exist, then creates it.
 *
 * @throw bdb::exception BDB_ERROR_NOT_FOUND - the sequence is not found.
 * @throw bdb::exception BDB_ERROR_EXISTS    - the sequence already exists.
 * @throw bdb::exception BDB_ERROR_UNKNOWN   - unknown error.
 */
sequence::sequence (database   * db,        /**< [in] Database of the sequence.                           */
                    const char * name,      /**< [in] Name of the sequence.                               */
                    bool         create)    /**< [in] Whether to create the sequence if it doesn't exist. */
  : m_database(db),
    m_seq(NULL)
{
    LOG4CPLUS_TRACE(logger, "[bdb::sequence::sequence] ENTER");
    LOG4CPLUS_DEBUG(logger, "[bdb::sequence::sequence] name = " << name);
    LOG4CPLUS_DEBUG(logger, "[bdb::sequence::sequence] create = " << create);

    int res = db_sequence_create(&m_seq, m_database->m_seq, 0);

    if (res == 0) res = m_seq->set_flags(m_seq, DB_SEQ_INC);
    if (res == 0) res = m_seq->set_cachesize(m_seq, 0);
    if (res == 0) res = m_seq->initial_value(m_seq, 1);

    u_int32_t flags = DB_THREAD;

    if (create)
    {
        flags |= DB_CREATE | DB_EXCL;
    }

    DBT key;

    memset(&key, 0, sizeof(DBT));
    key.data = (void *) name;
    key.size = (u_int32_t) strlen(name);

    if (res == 0) res = m_seq->open(m_seq, m_database->m_txns.top(), &key, flags);

    if (res != 0)
    {
        LOG4CPLUS_WARN(logger, "[bdb::sequence::sequence] " << db_strerror(res));

        if (m_seq != NULL) m_seq->close(m_seq, 0);

        switch (res)
        {
            case ENOENT:    throw exception(BDB_ERROR_NOT_FOUND);
            case EEXIST:    throw exception(BDB_ERROR_EXISTS);
            default:        throw exception(BDB_ERROR_UNKNOWN);
        }
    }

    LOG4CPLUS_TRACE(logger, "[bdb::sequence::sequence] EXIT");
}

/**
 * Closes the sequence.
 */
sequence::~sequence () throw ()
{
    LOG4CPLUS_TRACE(logger, "[bdb::sequence::~sequence] ENTER");

    if (m_seq != NULL)
    {
        m_seq->close(m_seq, 0);
    }

    LOG4CPLUS_TRACE(logger, "[bdb::sequence::~sequence] EXIT");
}

//--------------------------------------------------------------------------------------------------
//  Public interface.
//--------------------------------------------------------------------------------------------------

/**
 * Generates next unique identifier and returns it.
 *
 * @return 64-bit integer value.
 *
 * @throw bdb::exception BDB_ERROR_UNKNOWN - unknown error.
 */
int64_t sequence::id ()
{
    LOG4CPLUS_TRACE(logger, "[bdb::sequence::id] ENTER");

    db_seq_t id;
    int res = m_seq->get(m_seq, m_database->m_txns.top(), 1, &id, 0);

    if (res != 0)
    {
        LOG4CPLUS_WARN(logger, "[bdb::sequence::id] " << db_strerror(res));
        throw exception(BDB_ERROR_UNKNOWN);
    }

    LOG4CPLUS_TRACE(logger, "[bdb::sequence::id] EXIT = " << id);

    return (int64_t)id;
}

}

//--------------------------------------------------------------------------------------------------
