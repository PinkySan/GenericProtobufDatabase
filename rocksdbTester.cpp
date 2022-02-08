#include <catch2/catch.hpp>
#include <filesystem>
#include <future>
#include <iostream>
#include <numeric>
#include <rocksdb/db.h>


struct databaseTester
{
    const std::string filepath      = "./tmp.db";
    rocksdb::DB *db                 = nullptr;
    rocksdb::ColumnFamilyHandle *cf = nullptr;
    databaseTester()
    {
        if (std::filesystem::exists(filepath))
        {
            REQUIRE(0 < std::filesystem::remove_all(filepath));
        }
    }

    ~databaseTester()
    {
        if (db)
        {
            if (cf)
            {
                db->DestroyColumnFamilyHandle(cf);
            }
            db->Close();
            delete db;
        }
    }
};

TEST_CASE_METHOD(databaseTester, "Reading non existing key")
{
    rocksdb::Options options;
    options.create_if_missing = true;
    REQUIRE(rocksdb::DB::Open(options, filepath, &db).ok());

    WHEN("We try to read a non existing key")
    {
        std::string strValue;
        std::string key      = "notExisting";
        rocksdb::Status stat = db->Get(rocksdb::ReadOptions(), key, &strValue);
        THEN("We get an error")
        {
            CHECK_FALSE(stat.ok());
        }
    }
}

TEST_CASE_METHOD(databaseTester, "Reading and Writing string representation")
{
    rocksdb::Options options;
    options.create_if_missing = true;
    REQUIRE(rocksdb::DB::Open(options, filepath, &db).ok());
    WHEN("We write a value in string representation")
    {
        const int numValue    = 42;
        const std::string key = "1";
        REQUIRE(db->Put(rocksdb::WriteOptions(), key, std::to_string(numValue)).ok());
        THEN("We can read the value afterwards")
        {
            std::string strValue;
            CHECK(db->Get(rocksdb::ReadOptions(), key, &strValue).ok());
            CHECK(numValue == stoi(strValue));
        }
    }
}

TEST_CASE_METHOD(databaseTester, "Reading and Writing binary representation")
{
    rocksdb::Options options;
    options.create_if_missing = true;
    REQUIRE(rocksdb::DB::Open(options, filepath, &db).ok());

    WHEN("We write a value in binary representation")
    {
        int numValue          = 17;
        const std::string key = "2";
        REQUIRE(db->Put(rocksdb::WriteOptions(), key, reinterpret_cast<const char *>(&numValue)).ok());
        THEN("We can read the value afterwards")
        {
            std::string strValue;
            CHECK(db->Get(rocksdb::ReadOptions(), key, &strValue).ok());
            CHECK(numValue == *reinterpret_cast<int *>(strValue.data()));
            AND_WHEN("We override the existing key")
            {
                numValue = 27;
                REQUIRE(db->Put(rocksdb::WriteOptions(), key, reinterpret_cast<const char *>(&numValue)).ok());
                THEN("We will get the new value instead")
                {
                    CHECK(db->Get(rocksdb::ReadOptions(), key, &strValue).ok());
                    CHECK(numValue == *reinterpret_cast<int *>(strValue.data()));
                }
            }
        }
    }
}

TEST_CASE_METHOD(databaseTester, "Reading and Writing binary representation max value")
{
    rocksdb::Options options;
    options.create_if_missing = true;
    REQUIRE(rocksdb::DB::Open(options, filepath, &db).ok());
    WHEN("We write a max value in binary representation")
    {
        const int numValue    = std::numeric_limits<int>::max();
        const std::string key = "3";
        REQUIRE(db->Put(rocksdb::WriteOptions(), key, reinterpret_cast<const char *>(&numValue)).ok());
        THEN("We can read the value afterwards")
        {
            std::string strValue;
            CHECK(db->Get(rocksdb::ReadOptions(), key, &strValue).ok());
            CHECK(numValue == *reinterpret_cast<int *>(strValue.data()));
        }
    }
}

struct values
{
    int intValue     = 42;
    float floatValue = 0.3f;
};

TEST_CASE_METHOD(databaseTester, "Reading and Writing binary representation - own numeric struct")
{
    rocksdb::Options options;
    options.create_if_missing = true;
    REQUIRE(rocksdb::DB::Open(options, filepath, &db).ok());
    WHEN("We write a struct in binary representation")
    {
        values structValue;
        const std::string key = "3";
        std::stringstream ss;
        ss.write(reinterpret_cast<const char *>(&structValue), sizeof(structValue));
        REQUIRE(db->Put(rocksdb::WriteOptions(), key, ss.str()).ok());

        THEN("We can read the value afterwards")
        {
            std::string strValue;
            CHECK(db->Get(rocksdb::ReadOptions(), key, &strValue).ok());
            values getValues = *reinterpret_cast<values *>(strValue.data());
            CHECK(structValue.intValue == getValues.intValue);
            CHECK(structValue.floatValue == getValues.floatValue);
        }
    }
}

TEST_CASE_METHOD(databaseTester, "Reading and Writing binary representation - string")
{
    rocksdb::Options options;
    options.create_if_missing = true;
    REQUIRE(rocksdb::DB::Open(options, filepath, &db).ok());
    WHEN("We write a string value in binary representation")
    {
        const std::string strValue = "Gandalf";
        const std::string key      = "1";
        REQUIRE(db->Put(rocksdb::WriteOptions(), key, strValue.data()).ok());
        THEN("We can read the value afterwards")
        {
            std::string strGetValue;
            CHECK(db->Get(rocksdb::ReadOptions(), key, &strGetValue).ok());
            CHECK(strGetValue == strValue);
        }
    }
}

struct valuesWithString
{
    int intValue         = 17;
    float floatValue     = 42.3f;
    std::string strValue = "lorem";

    void serialize(std::ostream &ss)
    {
        ss.write(reinterpret_cast<char *>(&intValue), sizeof(intValue));
        ss.write(reinterpret_cast<char *>(&floatValue), sizeof(floatValue));
        ss << strValue;
    }

    void deserialize(std::istream &ss)
    {
        ss.read(reinterpret_cast<char *>(&intValue), sizeof(intValue));
        ss.read(reinterpret_cast<char *>(&floatValue), sizeof(floatValue));
        ss >> strValue;
    }
};

TEST_CASE_METHOD(databaseTester, "Reading and Writing binary representation - own numeric struct with strings")
{
    rocksdb::Options options;
    options.create_if_missing = true;
    REQUIRE(rocksdb::DB::Open(options, filepath, &db).ok());
    WHEN("We write a struct in binary representation")
    {
        valuesWithString structValue;
        const std::string key = "3";
        std::stringstream output;
        structValue.serialize(output);
        REQUIRE(db->Put(rocksdb::WriteOptions(), key, output.str()).ok());

        THEN("We can read the value afterwards")
        {
            std::string strValue;
            CHECK(db->Get(rocksdb::ReadOptions(), key, &strValue).ok());
            valuesWithString getValues;
            getValues.intValue   = 42;
            getValues.floatValue = 100.0f;
            getValues.strValue   = "max";
            std::stringstream input{strValue};
            getValues.deserialize(input);
            CHECK(structValue.intValue == getValues.intValue);
            CHECK(structValue.floatValue == getValues.floatValue);
            CHECK(structValue.strValue == getValues.strValue);
        }
    }
}

struct valuesWithMultipleString
{
    std::string strValue_1 = "lorem";
    std::string strValue_2 = "ipsum";

    void serialize(std::ostream &ss)
    {
        ss << strValue_1 << " " << strValue_2;
    }

    void deserialize(std::istream &ss)
    {
        ss >> strValue_1 >> strValue_2;
    }
};

/**
 * @brief This testcase only works because of the delimiter ' '.
 * If the string itself contains a whitespace character, the test will fail
 * Not satisfactorily
 *
 */
TEST_CASE_METHOD(databaseTester, "Reading and Writing binary representation - own numeric struct with multiple strings")
{
    rocksdb::Options options;
    options.create_if_missing = true;
    REQUIRE(rocksdb::DB::Open(options, filepath, &db).ok());
    WHEN("We write a struct in binary representation")
    {
        valuesWithMultipleString structValue;
        const std::string key = "3";
        std::stringstream output;
        structValue.serialize(output);
        REQUIRE(db->Put(rocksdb::WriteOptions(), key, output.str()).ok());

        THEN("We can read the value afterwards")
        {
            std::string strValue;
            CHECK(db->Get(rocksdb::ReadOptions(), key, &strValue).ok());
            valuesWithMultipleString getValues;
            getValues.strValue_1 = "Max";
            getValues.strValue_2 = "Mustermann";
            std::stringstream input{strValue};
            getValues.deserialize(input);
            CHECK(structValue.strValue_1 == getValues.strValue_1);
            CHECK(structValue.strValue_2 == getValues.strValue_2);
        }
    }
}

TEST_CASE_METHOD(databaseTester, "ColumnFamily")
{
    WHEN("I create a dabase with an additional columnfamily")
    {
        rocksdb::Options options;
        options.create_if_missing = true;
        REQUIRE(rocksdb::DB::Open(options, filepath, &db).ok());

        // create column family
        REQUIRE(db->CreateColumnFamily(rocksdb::ColumnFamilyOptions(), "description", &cf).ok());
        THEN("I would get two handles")
        {
            CHECK(rocksdb::kDefaultColumnFamilyName == db->DefaultColumnFamily()->GetName());
            CHECK("description" == cf->GetName());
        }
        WHEN("I write a key value pair to the default column")
        {
            const std::string key = "key_1";
            REQUIRE(db->Put(rocksdb::WriteOptions(), "key_1", "42").ok());
            THEN("I cannot read it from the other column")
            {
                std::string value;
                CHECK_FALSE(db->Get(rocksdb::ReadOptions(), cf, key, &value).ok());
                CHECK(db->Get(rocksdb::ReadOptions(), db->DefaultColumnFamily(), key, &value).ok());
                CHECK(value == "42");
            }
        }
        WHEN("I write a key value pair to the other column")
        {
            const std::string key = "key_1";
            REQUIRE(db->Put(rocksdb::WriteOptions(), cf, "key_1", "42").ok());
            THEN("I cannot read it from the other column")
            {
                std::string value;
                CHECK(db->Get(rocksdb::ReadOptions(), cf, key, &value).ok());
                CHECK_FALSE(db->Get(rocksdb::ReadOptions(), db->DefaultColumnFamily(), key, &value).ok());
                CHECK(value == "42");
            }
        }
    }
}

class dbWriter
{
private:
    rocksdb::DB *_db = nullptr;

public:
    ~dbWriter()
    {
        if (_db)
        {
            _db->Close();
            delete _db;
        }
    }

    bool init(const std::filesystem::path &path)
    {
        rocksdb::Options opts;
        opts.create_if_missing = true;
        return rocksdb::DB::Open(opts, path.string(), &_db).ok();
    }

    template <int ctr, int numTrigger>
    bool write(std::condition_variable &waitTrigger)
    {
        std::cout << std::this_thread::get_id() << "\tstarted writing" << std::endl;
        for (size_t idx = 0; idx < ctr; ++idx)
        {
            if (!_db->Put(rocksdb::WriteOptions(), std::to_string(idx), std::to_string(rand())).ok())
            {
                return false;
            }
            if (numTrigger == idx)
            {
                std::cout << "trigger" << std::endl;
                //_db->Flush(rocksdb::FlushOptions());
                waitTrigger.notify_all();
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
        std::cout << std::this_thread::get_id() << "\tfinished writing" << std::endl;
        return true;
    }
};

class dbReader
{
private:
    rocksdb::DB *_db = nullptr;
    rocksdb::Iterator *_iter = nullptr;
public:
    ~dbReader()
    {
        if (_db)
        {
            if(_iter)
            {
                delete _iter;
            }
            _db->Close();
            delete _db;
        }
    }

    bool init(const std::filesystem::path &path)
    {
        rocksdb::Options opts;
        opts.create_if_missing = true;
        return rocksdb::DB::OpenForReadOnly(opts, path.string(), &_db).ok();
    }

    bool read()
    {
        std::cout << std::this_thread::get_id() << "\tstarted reading" << std::endl;
        _iter = _db->NewIterator(rocksdb::ReadOptions());
        _iter->SeekToLast();
        if(!_iter->Valid())
        {
            return false;
        }
        std::cout << _iter->key().ToString() << "\t" << _iter->value().ToString() << std::endl;
        std::cout << std::this_thread::get_id() << "\tfinished reading" << std::endl;
        return true;
    }
};

TEST_CASE_METHOD(databaseTester, "multithreading")
{
    WHEN("I create a writing thread")
    {
        dbWriter writer;
        AND_WHEN("I write some values and then create a reading thread")
        {
            REQUIRE(writer.init(filepath));
            dbReader reader;
            THEN("I should run both threads in parallel")
            {
                std::mutex mtx;
                std::condition_variable cv;
                auto writerThread = std::async(&dbWriter::write<100, 20>, &writer, std::ref(cv));
                std::unique_lock<std::mutex> lk(mtx);
                CHECK(std::cv_status::no_timeout == cv.wait_for(lk, std::chrono::seconds(1)));
                REQUIRE(reader.init(filepath));
                CHECK(reader.read());
                CHECK(writerThread.get());
            }
        }
    }
}