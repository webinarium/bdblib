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

/** @file bdb/src/bdb.cc
 * Contains implementation of global BDB Library functions.
 * @author Artem Rodygin
 */

#include <bdb.h>

// Standard C/C++ Libraries
#include <cstdlib>
#include <cstring>
#include <string>

// Protocol Buffers
#include <google/protobuf/message.h>

// Berkeley DB
#include <db.h>

// Check for Berkeley DB version
#if (DB_VERSION_MAJOR != 4) || (DB_VERSION_MINOR < 7)
#error Berkeley DB 4.7 or later is required.
#endif

//--------------------------------------------------------------------------------------------------
//  Implementation of global functions.
//--------------------------------------------------------------------------------------------------

namespace bdb
{

using std::string;
using google::protobuf::Message;

//--------------------------------------------------------------------------------------------------
//  Berkeley DB version.
//--------------------------------------------------------------------------------------------------

/**
 * Returns Berkeley DB version.
 *
 * @param [out] major Berkeley DB major version (can be "NULL" to be skipped).
 * @param [out] minor Berkeley DB minor version (can be "NULL" to be skipped).
 * @param [out] patch Berkeley DB patch version (can be "NULL" to be skipped).
 */
void version (int * major, int * minor, int * patch)
{
    char * ver = db_version(major, minor, patch);
    LOG4CPLUS_DEBUG(logger, "[bdb::version] " << ver);
}

//--------------------------------------------------------------------------------------------------
//  Marshaling btw Berkeley DB and Protocol Buffers.
//--------------------------------------------------------------------------------------------------

/**
 * Serializes specified "google::protobuf::Message" into "DBT" object.
 * The function allocates required amount of memory, and the caller is
 * responsible to free this memory by "bdb::release" function.
 *
 * Note: if the function is called from index's callback function, you
 * don't have to call "bdb::release", since Berkeley DB will free this
 * memory itself.
 *
 * @see release
 */
void serialize (const Message * from,   /**< [in]  Source ProtoBuf message. */
                DBT           * to)     /**< [out] Resulted "DBT" object.   */
{
    string str;
    from->SerializeToString(&str);

    memset(to, 0, sizeof(DBT));

    to->data = malloc(str.size());
    memcpy(to->data, str.c_str(), str.size());

    to->flags = DB_DBT_USERMEM | DB_DBT_APPMALLOC;
    to->size = to->ulen = str.size();
}

/**
 * Unserializes specified "DBT" object into "google::protobuf::Message".
 */
void unserialize (const DBT * from,     /**< [in]  Source "DBT" object.       */
                  Message   * to)       /**< [out] Resulted ProtoBuf message. */
{
    string str((char *) from->data, from->size);
    to->ParseFromString(str);
}

/**
 * Releases memory, occupied by serialized "DBT" object.
 */
void release (DBT * dbt)    /**< [in/out] Serialized "DBT" object. */
{
    if (dbt->data != NULL)
    {
        free(dbt->data);
        memset(dbt, 0, sizeof(DBT));
    }
}

//--------------------------------------------------------------------------------------------------
//  Memory allocation functions.
//--------------------------------------------------------------------------------------------------

/**
 * Allocate memory block.
 * The function is to be internally used by Berkeley DB API.
 */
void * malloc (size_t size)
{
    return ::malloc(size);
}

/**
 * Reallocate memory block.
 * The function is to be internally used by Berkeley DB API.
 */
void * realloc (void * ptr, size_t size)
{
    return ::realloc(ptr, size);
}

/**
 * Deallocate space in memory.
 * The function is to be internally used by Berkeley DB API.
 */
void free (void * ptr)
{
    ::free(ptr);
}

//--------------------------------------------------------------------------------------------------
//  Implementation of class "bdb::exception".
//--------------------------------------------------------------------------------------------------

exception::exception (int error) : m_error(error)
{ }

exception::~exception () throw ()
{ }

}

//--------------------------------------------------------------------------------------------------
