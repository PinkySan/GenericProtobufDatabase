
function(addCatchTest targetname)
    add_executable(${targetname} ${ARGN})
    target_link_libraries(${targetname} PRIVATE Catch2::Catch2)
    target_compile_definitions(${targetname} PRIVATE CATCH_CONFIG_MAIN)
    add_test(NAME ${targetname} COMMAND ${targetname})
endfunction()