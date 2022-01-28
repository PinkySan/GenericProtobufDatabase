/* At run-time deserialization of a protobuf buffer to a C++ object example
 * Based on https://cxwangyi.wordpress.com/2010/06/29/google-protocol-buffers-online-parsing-of-proto-file-and-related-data-files/
 * @author: Floris Van den Abeele <floris@vdna.be>
 *
 * Starting from a protobuf definition, main() does the following:
 * 1) Translate protobuf definition to FileDescriptorProto object using the
 * Parser from protoc. FileDescriptorProto seems to be nothing more than an
 * in-memory representation of the proto definition.
 * 2) Use a DescriptorPool to construct a FileDescriptor. FileDescriptor
 * seems to contain all necessary meta data to describe all the members of a
 * message that adheres to the proto definition. DescriptorPool can be used to
 * 'resolve' any other proto types that might be used by our proto definition.
 * 3) Print the parsed proto definition.
 * 4) Use DynamicMessageFactory and ParseFromArray to deserialize a binary
 * buffer to a Message that follows the proto definition
 * 5) Use Reflection to print the data fields present in the deserialized
 * object
 * Note that this example code does not look at error handling.
 */

/*
 * Creating desc at runtime https://gist.github.com/hobo0cn/08a6f48205246d5e5bef47e401b8ed35
 * Setting value at runtime
 * https://stackoverflow.com/questions/11996557/how-to-dynamically-build-a-new-protobuf-from-a-set-of-already-defined-descriptor
 */
#include <catch2/catch.hpp>
#include <iostream>

#include <google/protobuf/descriptor.h>
#include <google/protobuf/dynamic_message.h>
#include <google/protobuf/io/tokenizer.h>
#include <google/protobuf/io/zero_copy_stream_impl.h>

#include <google/protobuf/compiler/parser.h>



TEST_CASE("Creating dynamic messages")
{
    constexpr const char *text = R"(syntax = "proto3";
message APIPort3 
{
    uint32 value1 = 1;
    uint32 value2 = 2;
    uint32 value3 = 3;
})";
    constexpr const char *message_type("APIPort3");

    std::cout << text << std::endl;
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
    REQUIRE(parser.Parse(&input, &file_desc_proto));

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
    google::protobuf::DescriptorPool pool;
    const google::protobuf::FileDescriptor *file_desc = pool.BuildFile(file_desc_proto);
    REQUIRE(file_desc);

    // As a .proto definition can contain more than one message Type,
    // select the message type that we are interested in
    const google::protobuf::Descriptor *message_desc = file_desc->FindMessageTypeByName(message_type);
    REQUIRE(message_desc);

    // Create an empty Message object that will hold the result of deserializing
    // a byte array for the proto definition:
    google::protobuf::DynamicMessageFactory factory;
    const google::protobuf::Message *prototype_msg = factory.GetPrototype(message_desc); // prototype_msg is immutable
    REQUIRE(prototype_msg);

    google::protobuf::Message *mutable_msg = prototype_msg->New();
    REQUIRE(mutable_msg);

    // Use the reflection interface to setting the contents.
    const Reflection *reflection = mutable_msg->GetReflection();
    const FieldDescriptor *desc  = message_desc->FindFieldByName("value1");
    CHECK(desc);
    CHECK_NOTHROW(reflection->SetUInt32(mutable_msg, desc, 42));
    desc = message_desc->FindFieldByName("value2");
    CHECK(desc);
    CHECK_NOTHROW(reflection->SetUInt32(mutable_msg, desc, 17));
    desc = message_desc->FindFieldByName("value3");
    CHECK(desc);
    CHECK_NOTHROW(reflection->SetUInt32(mutable_msg, desc, 255));

    std::cout << mutable_msg->DebugString();
}