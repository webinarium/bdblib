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
//  Artem Rodygin           2009-11-22      Initial creation.
//--------------------------------------------------------------------------------------------------

#include <bdb.h>

// Standard C/C++ Libraries
#include <iostream>

// C++ Logging Library
#include <log4cplus/logger.h>
#include <log4cplus/configurator.h>

// Namespaces in use.
using namespace std;

// Data schema.
#include <month.pb.h>
#include <season.pb.h>

// Name of database.
#define DATABASE_NAME   "testdb"

// Test results.
int passed = 0, failed = 0, blocked = 0;

// Test marcos.
#define TEST(desc)      (cout << "[TEST #" << (passed + failed + blocked + 1) << "] " << desc << "\n")
#define BLOCK()         (cout << "[BLOCK]\n\n", blocked++)
#define CHECK(expr)     (expr ? (cout << "[PASS]\n\n", passed++) \
                              : (cout << "[FAIL]\n\n", failed++))

//--------------------------------------------------------------------------------------------------
// "season" table.
//--------------------------------------------------------------------------------------------------

// Sort callback function.
int season_compare (DB *, const DBT * dbt1, const DBT * dbt2)
{
    season::key key1, key2;

    bdb::unserialize(dbt1, &key1);
    bdb::unserialize(dbt2, &key2);

    return key1.season().compare(key2.season());
}

//--------------------------------------------------------------------------------------------------
// "month" table.
//--------------------------------------------------------------------------------------------------

// Sort callback function.
int month_compare (DB *, const DBT * dbt1, const DBT * dbt2)
{
    month::key key1, key2;

    bdb::unserialize(dbt1, &key1);
    bdb::unserialize(dbt2, &key2);

    return key1.month().compare(key2.month());
}

//--------------------------------------------------------------------------------------------------
// "season_ix" index.
//--------------------------------------------------------------------------------------------------

// Sort callback function.
int season_ix_compare (DB *, const DBT * dbt1, const DBT * dbt2)
{
    month::season_ix key1, key2;

    bdb::unserialize(dbt1, &key1);
    bdb::unserialize(dbt2, &key2);

    return key1.season().compare(key2.season());
}

// Index callback function.
int season_ix_index (DB *, const DBT *, const DBT * data, DBT * result)
{
    month::data      d;
    month::season_ix k;

    bdb::unserialize(data, &d);
    k.set_season(d.season());
    bdb::serialize(&k, result);

    return 0;
}

//--------------------------------------------------------------------------------------------------
// "days_ix" index.
//--------------------------------------------------------------------------------------------------

// Sort callback function.
int days_ix_compare (DB *, const DBT * dbt1, const DBT * dbt2)
{
    month::days_ix key1, key2;

    bdb::unserialize(dbt1, &key1);
    bdb::unserialize(dbt2, &key2);

    return key1.days() - key2.days();
}

// Index callback function.
int days_ix_index (DB *, const DBT *, const DBT * data, DBT * result)
{
    month::data    d;
    month::days_ix k;

    bdb::unserialize(data, &d);
    k.set_days(d.days());
    bdb::serialize(&k, result);

    return 0;
}

//--------------------------------------------------------------------------------------------------
// "ordnum_ix" index.
//--------------------------------------------------------------------------------------------------

// Sort callback function.
int ordnum_ix_compare (DB *, const DBT * dbt1, const DBT * dbt2)
{
    month::ordnum_ix key1, key2;

    bdb::unserialize(dbt1, &key1);
    bdb::unserialize(dbt2, &key2);

    return (int)(key1.ordnum() - key2.ordnum());
}

// Index callback function.
int ordnum_ix_index (DB *, const DBT *, const DBT * data, DBT * result)
{
    month::data      d;
    month::ordnum_ix k;

    bdb::unserialize(data, &d);
    k.set_ordnum(d.ordnum());
    bdb::serialize(&k, result);

    return 0;
}

//--------------------------------------------------------------------------------------------------

// Main routine.
int main ()
{
    log4cplus::BasicConfigurator::doConfigure();
    log4cplus::Logger::getInstance(BDB_LOGGER_PORT).setLogLevel(log4cplus::OFF_LOG_LEVEL);

    bdb::database * db = NULL;

    bdb::sequence * seq = NULL;

    bdb::table * tseason = NULL;
    bdb::table * tmonth  = NULL;

    bdb::index * iseason = NULL;
    bdb::index * idays   = NULL;
    bdb::index * iordnum = NULL;

    bdb::recordset * rs = NULL;

    // 1 //-----------------------------------------------------------------------------------------
    try
    {
        TEST("Try to open absent database.");

        db = new bdb::database(DATABASE_NAME);
        delete db;

        CHECK(false);
    }
    catch (bdb::exception &e)
    {
        CHECK(BDB_ERROR_NOT_FOUND == e.error());
    }

    // 2 //-----------------------------------------------------------------------------------------
    try
    {
        TEST("Create new database, tables, and indexes.");

        db = new bdb::database(DATABASE_NAME, true);

        seq = db->add_sequence("month", true);

        tseason = db->add_table("season", season_compare, true);
        tmonth  = db->add_table("month",  month_compare,  true);

        iseason = tmonth->add_index("season", season_ix_index, season_ix_compare);
        idays   = tmonth->add_index("days",   days_ix_index,   days_ix_compare);
        iordnum = tmonth->add_index("ordnum", ordnum_ix_index, ordnum_ix_compare, true);

        iseason->add_foreign(tseason);

        delete db;

        CHECK(true);
    }
    catch (bdb::exception &)
    {
        CHECK(false);
    }

    // 3 //-----------------------------------------------------------------------------------------
    try
    {
        TEST("Open existing database, tables, and indexes.");

        db = new bdb::database(DATABASE_NAME);

        seq = db->add_sequence("month");

        tseason = db->add_table("season", season_compare);
        tmonth  = db->add_table("month",  month_compare );

        iseason = tmonth->add_index("season", season_ix_index, season_ix_compare);
        idays   = tmonth->add_index("days",   days_ix_index,   days_ix_compare);
        iordnum = tmonth->add_index("ordnum", ordnum_ix_index, ordnum_ix_compare, true);

        iseason->add_foreign(tseason, false);   // disable cascade removal

        // keep database opened

        CHECK(true);
    }
    catch (bdb::exception &)
    {
        CHECK(false);
        db = NULL;
    }

    // 4 //-----------------------------------------------------------------------------------------
    try
    {
        TEST("Try to open absent table.");

        if (db == NULL) BLOCK();
        else
        {
            db->add_table("keyser_soze", season_compare);

            CHECK(false);
        }
    }
    catch (bdb::exception &e)
    {
        CHECK(BDB_ERROR_NOT_FOUND == e.error());
    }

    // 5 //-----------------------------------------------------------------------------------------
    try
    {
        TEST("Try to create existing table.");

        if (db == NULL) BLOCK();
        else
        {
            db->add_table("season", season_compare, true);

            CHECK(false);
        }
    }
    catch (bdb::exception &e)
    {
        CHECK(BDB_ERROR_EXISTS == e.error());
    }

    // 6 //-----------------------------------------------------------------------------------------
    try
    {
        TEST("Check that record doesn't exist.");

        if (db == NULL) BLOCK();
        else
        {
            season::key key;
            key.set_season("Fall");

            CHECK(!tseason->exists(&key));
        }
    }
    catch (bdb::exception &)
    {
        CHECK(false);
    }

    // 7 //-----------------------------------------------------------------------------------------
    try
    {
        TEST("Try to insert new record with absent foreign key.");

        if (db == NULL) BLOCK();
        else
        {
            month::key  key;
            month::data data;

            key.set_month("September");

            data.set_season("Fall");
            data.set_days(30);
            data.set_ordnum(9);

            tmonth->insert(&key, &data);

            CHECK(false);
        }
    }
    catch (bdb::exception &e)
    {
        CHECK(BDB_ERROR_FOREIGN_KEY == e.error());
    }

    // 8 //-----------------------------------------------------------------------------------------
    try
    {
        TEST("Insert new record.");

        if (db == NULL) BLOCK();
        else
        {
            season::key  key;
            season::data data;

            key.set_season("Fall");
            tseason->insert(&key, &data);

            CHECK(true);
        }
    }
    catch (bdb::exception &)
    {
        CHECK(false);
    }

    // 9 //-----------------------------------------------------------------------------------------
    try
    {
        TEST("Check that record exists.");

        if (db == NULL) BLOCK();
        else
        {
            season::key key;
            key.set_season("Fall");

            CHECK(tseason->exists(&key));
        }
    }
    catch (bdb::exception &)
    {
        CHECK(false);
    }

    // 10 //----------------------------------------------------------------------------------------
    try
    {
        TEST("Try to insert existing record.");

        if (db == NULL) BLOCK();
        else
        {
            season::key  key;
            season::data data;

            key.set_season("Fall");
            tseason->insert(&key, &data);

            CHECK(false);
        }
    }
    catch (bdb::exception &e)
    {
        CHECK(BDB_ERROR_EXISTS == e.error());
    }

    // 11 //----------------------------------------------------------------------------------------
    try
    {
        TEST("Insert new record with foreign key.");

        if (db == NULL) BLOCK();
        else
        {
            month::key  key;
            month::data data;

            key.set_month("September");

            data.set_season("Fall");
            data.set_days(30);
            data.set_ordnum(9);

            tmonth->insert(&key, &data);

            CHECK(true);
        }
    }
    catch (bdb::exception &)
    {
        CHECK(false);
    }

    // 12 //----------------------------------------------------------------------------------------
    try
    {
        TEST("Select just inserted record.");

        if (db == NULL) BLOCK();
        else
        {
            month::key  key;
            month::data data;

            key.set_month("September");

            tmonth->select(&key, &data);

            CHECK(data.season() == "Fall" &&
                  data.days()   == 30     &&
                  data.ordnum() == 9);
        }
    }
    catch (bdb::exception &)
    {
        CHECK(false);
    }

    // 13 //----------------------------------------------------------------------------------------
    try
    {
        TEST("Check that record in the first index exists.");

        if (db == NULL) BLOCK();
        else
        {
            month::season_ix key;
            key.set_season("Fall");

            CHECK(iseason->exists(&key));
        }
    }
    catch (bdb::exception &)
    {
        CHECK(false);
    }

    // 14 //----------------------------------------------------------------------------------------
    try
    {
        TEST("Check that record in the second index exists.");

        if (db == NULL) BLOCK();
        else
        {
            month::days_ix key;
            key.set_days(30);

            CHECK(idays->exists(&key));
        }
    }
    catch (bdb::exception &)
    {
        CHECK(false);
    }

    // 15 //----------------------------------------------------------------------------------------
    try
    {
        TEST("Check that record in the third index exists.");

        if (db == NULL) BLOCK();
        else
        {
            month::ordnum_ix key;
            key.set_ordnum(9);

            CHECK(iordnum->exists(&key));
        }
    }
    catch (bdb::exception &)
    {
        CHECK(false);
    }

    // 16 //----------------------------------------------------------------------------------------
    try
    {
        TEST("Try to update existing record with absent foreign key.");

        if (db == NULL) BLOCK();
        else
        {
            month::key  key;
            month::data data;

            key.set_month("September");

            data.set_season("Autumn");
            data.set_days(30);
            data.set_ordnum(9);

            tmonth->update(&key, &data);

            CHECK(false);
        }
    }
    catch (bdb::exception &e)
    {
        CHECK(BDB_ERROR_FOREIGN_KEY == e.error());
    }

    // 17 //----------------------------------------------------------------------------------------
    try
    {
        TEST("Try to delete foreign key which is in use, while cascade removal is disabled.");

        if (db == NULL) BLOCK();
        else
        {
            season::key key;
            key.set_season("Fall");

            tseason->remove(&key);

            CHECK(false);
        }
    }
    catch (bdb::exception &e)
    {
        CHECK(BDB_ERROR_FOREIGN_KEY == e.error());
    }

    // 18 //----------------------------------------------------------------------------------------
    try
    {
        TEST("Update existing record with valid foreign key.");

        if (db == NULL) BLOCK();
        else
        {
            season::key  skey;
            season::data sdata;

            skey.set_season("Autumn");
            tseason->insert(&skey, &sdata);

            month::key  mkey;
            month::data mdata;

            mkey.set_month("September");

            mdata.set_season("Autumn");
            mdata.set_days(30);
            mdata.set_ordnum(9);

            tmonth->update(&mkey, &mdata);

            CHECK(true);
        }
    }
    catch (bdb::exception &)
    {
        CHECK(false);
    }

    // 19 //----------------------------------------------------------------------------------------
    try
    {
        TEST("Delete foreign key which was in use, but is not anymore.");

        if (db == NULL) BLOCK();
        else
        {
            season::key key;
            key.set_season("Fall");

            tseason->remove(&key);

            CHECK(true);
        }
    }
    catch (bdb::exception &)
    {
        CHECK(false);
    }

    // 20 //----------------------------------------------------------------------------------------
    try
    {
        TEST("Update existing record and check that record in the second and third indexes are changed.");

        if (db == NULL) BLOCK();
        else
        {
            month::key  key;
            month::data data;

            key.set_month("September");

            data.set_season("Autumn");
            data.set_days(31);
            data.set_ordnum(10);

            tmonth->update(&key, &data);

            month::days_ix   days_old,   days_new;
            month::ordnum_ix ordnum_old, ordnum_new;

            days_old.set_days(30);
            days_new.set_days(31);

            ordnum_old.set_ordnum(9);
            ordnum_new.set_ordnum(10);

            CHECK(!idays->exists(&days_old) && !iordnum->exists(&ordnum_old) &&
                   idays->exists(&days_new) &&  iordnum->exists(&ordnum_new));
        }
    }
    catch (bdb::exception &)
    {
        CHECK(false);
    }

    // 21 //----------------------------------------------------------------------------------------
    try
    {
        TEST("Try to insert new record with conflicting key in unique index.");

        if (db == NULL) BLOCK();
        else
        {
            month::key  key;
            month::data data;

            key.set_month("October");

            data.set_season("Fall");
            data.set_days(31);
            data.set_ordnum(10);

            tmonth->insert(&key, &data);

            CHECK(false);
        }
    }
    catch (bdb::exception &e)
    {
        CHECK(BDB_ERROR_EXISTS == e.error());
    }

    // 22 //----------------------------------------------------------------------------------------
    try
    {
        TEST("Delete foreign key which is in use, while cascade removal is enabled.");

        if (db == NULL) BLOCK();
        else
        {
            // reopen database
            delete db;

            db = new bdb::database(DATABASE_NAME);

            seq = db->add_sequence("month");

            tseason = db->add_table("season", season_compare);
            tmonth  = db->add_table("month",  month_compare );

            iseason = tmonth->add_index("season", season_ix_index, season_ix_compare);
            idays   = tmonth->add_index("days",   days_ix_index,   days_ix_compare);
            iordnum = tmonth->add_index("ordnum", ordnum_ix_index, ordnum_ix_compare, true);

            iseason->add_foreign(tseason, true);   // enable cascade removal

            season::key key;
            key.set_season("Autumn");

            tseason->remove(&key);

            CHECK(true);
        }
    }
    catch (bdb::exception &)
    {
        CHECK(false);
    }

    //----------------------------------------------------------------------------------------------
    if (db != NULL)
    {
        season::key  key;
        season::data data;

        key.set_season("Winter");   tseason->insert(&key, &data);
        key.set_season("Spring");   tseason->insert(&key, &data);
        key.set_season("Summer");   tseason->insert(&key, &data);
        key.set_season("Autumn");   tseason->insert(&key, &data);
    }

    //----------------------------------------------------------------------------------------------
    if (db != NULL)
    {
        struct
        {
            string  month;
            string  season;
            int     days;
        } test_data [12] = { {"January",   "Winter", 31},
                             {"February",  "Winter", 28},
                             {"March",     "Spring", 31},
                             {"April",     "Spring", 30},
                             {"May",       "Spring", 31},
                             {"June",      "Summer", 30},
                             {"July",      "Summer", 31},
                             {"August",    "Summer", 31},
                             {"September", "Autumn", 30},
                             {"October",   "Autumn", 31},
                             {"November",  "Autumn", 30},
                             {"December",  "Winter", 31}
                           };

        month::key  key;
        month::data data;

        for (int i = 0; i < 12; i++)
        {
            key.set_month(test_data[i].month);

            data.set_season(test_data[i].season);
            data.set_days  (test_data[i].days  );
            data.set_ordnum(seq->id());

            tmonth->insert(&key, &data);
        }
    }

    // 23 //----------------------------------------------------------------------------------------
    try
    {
        TEST("Check recordset gained from table.");

        struct
        {
            string  month;
            string  season;
            int     days;
            int     ordnum;
        } test_data [12] = { {"April",     "Spring", 30, 4},
                             {"August",    "Summer", 31, 8},
                             {"December",  "Winter", 31, 12},
                             {"February",  "Winter", 28, 2},
                             {"January",   "Winter", 31, 1},
                             {"July",      "Summer", 31, 7},
                             {"June",      "Summer", 30, 6},
                             {"March",     "Spring", 31, 3},
                             {"May",       "Spring", 31, 5},
                             {"November",  "Autumn", 30, 11},
                             {"October",   "Autumn", 31, 10},
                             {"September", "Autumn", 30, 9}
                           };

        if (db == NULL) BLOCK();
        else
        {
            month::key  key;
            month::data data;

            rs = new bdb::recordset(tmonth);

            bool res = true;
            int  i   = 0;

            for (int j = 0; j < 6; j++)
            {
                res = res && rs->fetch(&key, &data);

                res = res &&
                      key.month()   == test_data[j].month  &&
                      data.season() == test_data[j].season &&
                      data.days()   == test_data[j].days   &&
                      data.ordnum() == test_data[j].ordnum;
            }

            rs->rewind();

            while (rs->fetch(&key, &data))
            {
                res = res &&
                      key.month()   == test_data[i].month  &&
                      data.season() == test_data[i].season &&
                      data.days()   == test_data[i].days   &&
                      data.ordnum() == test_data[i].ordnum;

                i++;
            }

            delete rs;

            CHECK(res && i == 12);
        }
    }
    catch (bdb::exception &)
    {
        CHECK(false);
    }

    // 24 //----------------------------------------------------------------------------------------
    try
    {
        TEST("Check recordset gained from index.");

        struct
        {
            string  month;
            string  season;
            int     days;
            int     ordnum;
        } test_data [12] = { {"November",  "Autumn", 30, 11},
                             {"October",   "Autumn", 31, 10},
                             {"September", "Autumn", 30, 9},
                             {"April",     "Spring", 30, 4},
                             {"March",     "Spring", 31, 3},
                             {"May",       "Spring", 31, 5},
                             {"August",    "Summer", 31, 8},
                             {"July",      "Summer", 31, 7},
                             {"June",      "Summer", 30, 6},
                             {"December",  "Winter", 31, 12},
                             {"February",  "Winter", 28, 2},
                             {"January",   "Winter", 31, 1}
                           };

        if (db == NULL) BLOCK();
        else
        {
            month::key  key;
            month::data data;

            rs = new bdb::recordset(iseason);

            bool res = true;
            int  i   = 0;

            for (int j = 0; j < 6; j++)
            {
                res = res && rs->fetch(&key, &data);

                res = res &&
                      key.month()   == test_data[j].month  &&
                      data.season() == test_data[j].season &&
                      data.days()   == test_data[j].days   &&
                      data.ordnum() == test_data[j].ordnum;
            }

            rs->rewind();

            while (rs->fetch(&key, &data))
            {
                res = res &&
                      key.month()   == test_data[i].month  &&
                      data.season() == test_data[i].season &&
                      data.days()   == test_data[i].days   &&
                      data.ordnum() == test_data[i].ordnum;

                i++;
            }

            delete rs;

            CHECK(res && i == 12);
        }
    }
    catch (bdb::exception &)
    {
        CHECK(false);
    }

    // 25 //----------------------------------------------------------------------------------------
    try
    {
        TEST("Check recordset gained from index with filter.");

        struct
        {
            string  month;
            string  season;
            int     days;
            int     ordnum;
        } test_data [4] = { {"April",     "Spring", 30, 4},
                            {"June",      "Summer", 30, 6},
                            {"November",  "Autumn", 30, 11},
                            {"September", "Autumn", 30, 9}
                          };

        if (db == NULL) BLOCK();
        else
        {
            month::days_ix ikey;

            month::key  key;
            month::data data;

            ikey.set_days(30);

            rs = new bdb::recordset(idays, &ikey);

            bool res = true;
            int  i   = 0;

            for (int j = 0; j < 2; j++)
            {
                res = res && rs->fetch(&key, &data);

                res = res &&
                      key.month()   == test_data[j].month  &&
                      data.season() == test_data[j].season &&
                      data.days()   == test_data[j].days   &&
                      data.ordnum() == test_data[j].ordnum;
            }

            rs->rewind();

            while (rs->fetch(&key, &data))
            {
                res = res &&
                      key.month()   == test_data[i].month  &&
                      data.season() == test_data[i].season &&
                      data.days()   == test_data[i].days   &&
                      data.ordnum() == test_data[i].ordnum;

                i++;
            }

            delete rs;

            CHECK(res && i == 4);
        }
    }
    catch (bdb::exception &)
    {
        CHECK(false);
    }

    // 26 //----------------------------------------------------------------------------------------
    try
    {
        TEST("Check empty recordset gained from index with filter.");

        if (db == NULL) BLOCK();
        else
        {
            month::days_ix ikey;

            month::key  key;
            month::data data;

            ikey.set_days(3);

            rs = new bdb::recordset(idays, &ikey);

            int i = 0;
            while (rs->fetch(&key, &data)) i++;

            delete rs;

            CHECK(i == 0);
        }
    }
    catch (bdb::exception &)
    {
        CHECK(false);
    }

    // 27 //----------------------------------------------------------------------------------------
    try
    {
        TEST("Join several recordsets.");

        struct
        {
            string  month;
            string  season;
            int     days;
            int     ordnum;
        } test_data [2] = { {"November",  "Autumn", 30, 11},
                            {"September", "Autumn", 30, 9}
                          };

        if (db == NULL) BLOCK();
        else
        {
            month::season_ix skey;
            month::days_ix   dkey;

            month::key  key;
            month::data data;

            skey.set_season("Autumn");
            dkey.set_days(30);

            bdb::joinlist r;
            r.push_back(new bdb::recordset(iseason, &skey));
            r.push_back(new bdb::recordset(idays,   &dkey));

            rs = new bdb::recordset(tmonth, r);

            bool res = true;
            int  i   = 0;

            while (rs->fetch(&key, &data))
            {
                res = res &&
                      key.month()   == test_data[i].month  &&
                      data.season() == test_data[i].season &&
                      data.days()   == test_data[i].days   &&
                      data.ordnum() == test_data[i].ordnum;

                i++;
            }

            delete rs;

            for (unsigned i = 0; i < r.size(); i++)
            {
                delete r[i];
            }

            CHECK(res && i == 2);
        }
    }
    catch (bdb::exception &)
    {
        CHECK(false);
    }

    // 28 //----------------------------------------------------------------------------------------
    try
    {
        TEST("Try to join when at least one recordset is empty.");

        if (db == NULL) BLOCK();
        else
        {
            month::season_ix skey;
            month::days_ix   dkey;

            month::key  key;
            month::data data;

            skey.set_season("Autumn");
            dkey.set_days(32);

            bdb::joinlist r;
            r.push_back(new bdb::recordset(iseason, &skey));
            r.push_back(new bdb::recordset(idays,   &dkey));

            rs = new bdb::recordset(tmonth, r);

            int i = 0;
            while (rs->fetch(&key, &data)) i++;

            delete rs;

            for (unsigned i = 0; i < r.size(); i++)
            {
                delete r[i];
            }

            CHECK(i == 0);
        }
    }
    catch (bdb::exception &)
    {
        CHECK(false);
    }

    // 29 //----------------------------------------------------------------------------------------
    try
    {
        TEST("Rollback single transaction.");

        if (db == NULL) BLOCK();
        else
        {
            db->begin_transaction();

            month::key  key;
            month::data data;

            key.set_month("Remember");
            data.set_season("Winter");
            data.set_days(10);
            data.set_ordnum(0);
            tmonth->insert(&key, &data);

            key.set_month("January");
            tmonth->remove(&key);

            key.set_month("February");
            data.set_season("Winter");
            data.set_days(29);
            data.set_ordnum(2);
            tmonth->update(&key, &data);

            key.set_month("Remember");
            bool res = tmonth->exists(&key);

            key.set_month("January");
            res = res && !tmonth->exists(&key);

            key.set_month("February");
            tmonth->select(&key, &data);
            res = res && (data.days() == 29);

            db->rollback_transaction();

            key.set_month("Remember");
            res = res && !tmonth->exists(&key);

            key.set_month("January");
            res = res && tmonth->exists(&key);

            key.set_month("February");
            tmonth->select(&key, &data);
            res = res && (data.days() == 28);

            CHECK(res);
        }
    }
    catch (bdb::exception &)
    {
        CHECK(false);
    }

    // 30 //----------------------------------------------------------------------------------------
    try
    {
        TEST("Commit single transaction.");

        if (db == NULL) BLOCK();
        else
        {
            db->begin_transaction();

            month::key  key;
            month::data data;

            key.set_month("Remember");
            data.set_season("Winter");
            data.set_days(10);
            data.set_ordnum(0);
            tmonth->insert(&key, &data);

            key.set_month("January");
            tmonth->remove(&key);

            key.set_month("February");
            data.set_season("Winter");
            data.set_days(29);
            data.set_ordnum(2);
            tmonth->update(&key, &data);

            key.set_month("Remember");
            bool res = tmonth->exists(&key);

            key.set_month("January");
            res = res && !tmonth->exists(&key);

            key.set_month("February");
            tmonth->select(&key, &data);
            res = res && (data.days() == 29);

            db->commit_transaction();

            key.set_month("Remember");
            res = res && tmonth->exists(&key);

            key.set_month("January");
            res = res && !tmonth->exists(&key);

            key.set_month("February");
            tmonth->select(&key, &data);
            res = res && (data.days() == 29);

            CHECK(res);
        }
    }
    catch (bdb::exception &)
    {
        CHECK(false);
    }

    // 31 //----------------------------------------------------------------------------------------
    try
    {
        TEST("Rollback nested transactions.");

        if (db == NULL) BLOCK();
        else
        {
            db->begin_transaction();

            month::key  key;
            month::data data;

            key.set_month("January");
            data.set_season("Winter");
            data.set_days(31);
            data.set_ordnum(1);
            tmonth->insert(&key, &data);

            db->begin_transaction();

            key.set_month("Remember");
            tmonth->remove(&key);

            db->commit_transaction();

            key.set_month("January");
            bool res = tmonth->exists(&key);

            key.set_month("Remember");
            res = res && !tmonth->exists(&key);

            db->rollback_transaction();

            key.set_month("January");
            res = res && !tmonth->exists(&key);

            key.set_month("Remember");
            res = res && tmonth->exists(&key);

            CHECK(res);
        }
    }
    catch (bdb::exception &)
    {
        CHECK(false);
    }

    // 32 //----------------------------------------------------------------------------------------
    try
    {
        TEST("Commit nested transactions.");

        if (db == NULL) BLOCK();
        else
        {
            db->begin_transaction();

            month::key  key;
            month::data data;

            key.set_month("January");
            data.set_season("Winter");
            data.set_days(31);
            data.set_ordnum(1);
            tmonth->insert(&key, &data);

            db->begin_transaction();

            key.set_month("Remember");
            tmonth->remove(&key);

            key.set_month("Remember");
            bool res = !tmonth->exists(&key);

            db->rollback_transaction();

            key.set_month("January");
            res = res && tmonth->exists(&key);

            key.set_month("Remember");
            res = res && tmonth->exists(&key);

            db->commit_transaction();

            key.set_month("January");
            res = res && tmonth->exists(&key);

            key.set_month("Remember");
            res = res && tmonth->exists(&key);

            CHECK(res);
        }
    }
    catch (bdb::exception &)
    {
        CHECK(false);
    }
    //----------------------------------------------------------------------------------------------

    if (db != NULL) delete db;

    #define PLANNED 32

    cout << "PLANNED:  " << PLANNED << "\n";
    cout << "EXECUTED: " << (passed + failed + blocked) << "\n";
    cout << "PASSED:   " << passed  << "\n";
    cout << "FAILED:   " << failed  << "\n";
    cout << "BLOCKED:  " << blocked << "\n";

    return (passed == PLANNED && failed == 0 && blocked == 0 ? 0 : -1);
}
