
macro(addCatchTest targetname)
    if(${EXPORT_TESTRESULTS})
        file(MAKE_DIRECTORY ${CMAKE_BINARY_DIR}/testresult)
        set(ARGS -r junit -o ${CMAKE_BINARY_DIR}/testresult/${targetname}.xml)
    endif()
    cmake_parse_arguments(TEST_PARSE "${options}" ""
                          "" ${ARGN})

    add_executable(${targetname} ${TEST_PARSE_UNPARSED_ARGUMENTS})
    target_link_libraries(${targetname} PRIVATE Catch2::Catch2)
    target_compile_definitions(${targetname} PRIVATE CATCH_CONFIG_MAIN)
    add_test(NAME ${targetname} COMMAND ${targetname} ${ARGS})
endmacro()