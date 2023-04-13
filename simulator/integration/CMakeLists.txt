set(SOURCE_FILES webcachesim_cli.cpp ${WEBCACHESIM_HEADER_DIR}/simulation.h)
add_executable(webcachesim_cli ${SOURCE_FILES})

target_include_directories(webcachesim_cli PUBLIC ${WEBCACHESIM_HEADER_DIR})

target_link_libraries(webcachesim_cli PRIVATE webcachesim)

find_package(mongocxx REQUIRED)
find_package(bsoncxx REQUIRED)
target_include_directories(webcachesim_cli PRIVATE ${LIBMONGOCXX_INCLUDE_DIR})
target_include_directories(webcachesim_cli PRIVATE ${LIBBSONCXX_INCLUDE_DIR})
target_link_libraries(webcachesim_cli PRIVATE mongo::bsoncxx_shared)
target_link_libraries(webcachesim_cli PRIVATE mongo::mongocxx_shared)

target_compile_definitions(webcachesim_cli PRIVATE ${LIBMONGOCXX_DEFINITIONS})
