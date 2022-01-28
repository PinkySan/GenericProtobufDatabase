#include <filesystem>
#include <iostream>
#include <memory>
#include <random>
#include <rocksdb/db.h>

#include <google/protobuf/descriptor.h>
#include <google/protobuf/dynamic_message.h>
#include <google/protobuf/io/tokenizer.h>
#include <google/protobuf/io/zero_copy_stream_impl.h>

#include <google/protobuf/compiler/parser.h>

#include <desc.pb.h>

namespace
{
constexpr const char *text         = R"(syntax = "proto3";
message recorder_1 
{
    uint32 oltc = 1;
    int32 voltage = 2;
    int32 current = 3;
})";
constexpr const char *message_type = "recorder_1";

bool doesDBAlreadyExists(const std::filesystem::path &path)
{
    return std::filesystem::exists(path);
}

template <typename T, typename Generator>
T createRandomValue(Generator &gen)
{
    std::uniform_int_distribution<T> distrib(std::numeric_limits<T>::min(), std::numeric_limits<T>::max());
    return distrib(gen);
}

void setValues(const google::protobuf::Descriptor *message_desc, google::protobuf::Message *mutable_msg)
{
    static std::random_device rd;
    static std::mt19937 gen(rd());

    using namespace google::protobuf;
    const Reflection *reflection = mutable_msg->GetReflection();
    const FieldDescriptor *desc  = message_desc->FindFieldByName("oltc");
    reflection->SetUInt32(mutable_msg, desc, createRandomValue<uint32_t>(gen));
    desc = message_desc->FindFieldByName("voltage");
    reflection->SetInt32(mutable_msg, desc, createRandomValue<int32_t>(gen));
    desc = message_desc->FindFieldByName("current");
    reflection->SetInt32(mutable_msg, desc, createRandomValue<int32_t>(gen));
}

} // namespace

class DBCreator
{
private:
    rocksdb::DB *_db                         = nullptr;
    rocksdb::ColumnFamilyHandle *_descHandle = nullptr;

public:
    ~DBCreator()
    {
        if (_db)
        {
            if (_descHandle)
            {
                _db->DestroyColumnFamilyHandle(_descHandle);
            }
            _db->Close();
            delete _db;
        }
    }

    void create(const std::filesystem::path &path)
    {
        rocksdb::Options opts;
        opts.error_if_exists      = true;
        opts.create_if_missing    = true;
        opts.recycle_log_file_num = 1;
        opts.info_log_level       = rocksdb::FATAL_LEVEL;
        rocksdb::DB::Open(opts, path.string(), &_db);
    }

    void createNewColumn(const char *name)
    {
        _db->CreateColumnFamily(rocksdb::ColumnFamilyOptions(), name, &_descHandle);
    }

    void writeDesc(const char *key, const msgDesc &desc)
    {
        std::string output;
        desc.SerializeToString(&output);
        rocksdb::WriteOptions options;
        options.disableWAL = true;
        _db->Put(options, _descHandle, key, output);
    }

    void writeMsg(const char *key, const google::protobuf::Message *msg)
    {
        std::string output;
        msg->SerializeToString(&output);
        rocksdb::WriteOptions options;
        options.disableWAL = true;
        _db->Put(options, key, output);
    }
};

class MessageCreator
{
private:
    const google::protobuf::FileDescriptor *_file_desc = nullptr;
    google::protobuf::DynamicMessageFactory _factory;
    google::protobuf::DescriptorPool _pool;

public:
    const google::protobuf::Descriptor *createMessageDesc(const char *text, const char *message_type)
    {
        using namespace google::protobuf;
        using namespace google::protobuf::io;
        using namespace google::protobuf::compiler;

        ArrayInputStream raw_input(text, static_cast<int>(strlen(text)));
        Tokenizer input(&raw_input, NULL);

        // Proto definition to a representation as used by the protobuf lib:
        /* FileDescriptorProto documentation:
         * A valid .proto file can be translated directly to a FileDescriptorProto
         * without any other information (e.g. without reading its imports).
         * */
        FileDescriptorProto file_desc_proto;
        Parser parser;
        parser.Parse(&input, &file_desc_proto);

        // Set the name in file_desc_proto as Parser::Parse does not do this:
        if (!file_desc_proto.has_name())
        {
            file_desc_proto.set_name(message_type);
        }

        // Construct our own FileDescriptor for the proto file:
        /* FileDescriptor documentation:
         * Describes a whole .proto file.  To get the FileDescriptor for a compiled-in
         * file, get the descriptor for something defined in that file and call
         * descriptor->file().  Use DescriptorPool to construct your own descriptors.
         * */

        _file_desc = _pool.BuildFile(file_desc_proto);

        // As a .proto definition can contain more than one message Type,
        // select the message type that we are interested in
        return _file_desc->FindMessageTypeByName(message_type);
    }

    google::protobuf::Message *createNewMessage(const google::protobuf::Descriptor *msgDesc)
    {
        return _factory.GetPrototype(msgDesc)->New();
    }
};

class DBReader
{
private:
    rocksdb::DB *_db = nullptr;
    std::vector<rocksdb::ColumnFamilyHandle *> _vecHandle;

public:
    ~DBReader()
    {
        if (_db)
        {
            for (auto &it : _vecHandle)
            {
                delete it;
            }
            _db->Close();
            delete _db;
        }
    }

    void Open(const std::filesystem::path &path)
    {
        rocksdb::Options options;
        options.create_if_missing    = false;
        options.info_log_level       = rocksdb::FATAL_LEVEL;
        options.keep_log_file_num    = 1;
        options.recycle_log_file_num = 1;
        std::vector<rocksdb::ColumnFamilyDescriptor> vecOptions;
        vecOptions.emplace_back(rocksdb::kDefaultColumnFamilyName, rocksdb::ColumnFamilyOptions());
        vecOptions.emplace_back("desc", rocksdb::ColumnFamilyOptions());

        rocksdb::Status status = rocksdb::DB::Open(options, path.string(), vecOptions, &_vecHandle, &_db);
        if (!status.ok())
        {
            throw std::invalid_argument(status.ToString());
        }
    }

    msgDesc ReadDesc(const char *key)
    {
        rocksdb::Status status;
        msgDesc msg;
        std::string value;
        status = _db->Get(rocksdb::ReadOptions(), _vecHandle[1], key, &value);
        if (!status.ok())
        {
            throw std::invalid_argument(status.ToString());
        }
        if (!msg.ParseFromString(value))
        {
            throw std::invalid_argument("Error while parsing");
        }
        return msg;
    }

    std::string ReadMsg(const char *key)
    {
        rocksdb::Status status;
        std::string value;
        status = _db->Get(rocksdb::ReadOptions(), key, &value);
        if (!status.ok())
        {
            throw std::invalid_argument(status.ToString());
        }
        return value;
    }
};

int main()
{
    constexpr const char *dbName = "xmpl.db";

    try
    {

        if (doesDBAlreadyExists(dbName))
        {
            DBReader reader;
            reader.Open(dbName);
            msgDesc msg = reader.ReadDesc("desc1");
            std::cout << msg.measdescription() << std::endl;
            MessageCreator msgCreator;
            const google::protobuf::Descriptor *msg_Desc = msgCreator.createMessageDesc(msg.measdescription().c_str(), "recorder_1");
            google::protobuf::Message *mutable_msg       = msgCreator.createNewMessage(msg_Desc);
            for (uint16_t ctr = 0; ctr < 10; ctr++)
            {
                mutable_msg->ParseFromString(reader.ReadMsg(std::to_string(ctr).c_str()));
                mutable_msg->PrintDebugString();
            }
        }
        else
        {
            DBCreator dbCreator;
            MessageCreator msgCreator;
            dbCreator.create(dbName);
            dbCreator.createNewColumn("desc");
            const google::protobuf::Descriptor *msg_Desc = msgCreator.createMessageDesc(text, message_type);
            google::protobuf::Message *mutable_msg       = msgCreator.createNewMessage(msg_Desc);

            msgDesc desc;
            desc.set_startindex(0);
            desc.set_endindex(100);
            desc.set_starttimestamp(142);
            desc.set_endtimestamp(200);
            desc.set_measdescription(text);
            dbCreator.writeDesc("desc1", desc);

            for (uint16_t ctr = 0; ctr < 1000; ctr++)
            {
                setValues(msg_Desc, mutable_msg);
                // mutable_msg->PrintDebugString();
                dbCreator.writeMsg(std::to_string(ctr).c_str(), mutable_msg);
            }
        }
    }
    catch (const std::exception &e)
    {
        std::cerr << e.what() << '\n';
    }
    return 0;
}