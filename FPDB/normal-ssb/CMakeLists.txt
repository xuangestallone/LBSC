project(normal-ssb VERSION "${PROJECT_VERSION}" LANGUAGES C CXX)

# need to add and link libraries on mac
LINK_DIRECTORIES(/usr/local/opt/llvm@11/lib)
LINK_DIRECTORIES(/usr/local/opt/binutils/lib)
LINK_DIRECTORIES(/usr/local/opt/libdeflate/lib)
LINK_DIRECTORIES(/usr/local/lib)
INCLUDE_DIRECTORIES(/usr/local/include)

#-----------------------------------------------------------------------------------------------------------------------
# SSB common classes
#-----------------------------------------------------------------------------------------------------------------------

add_library(normal-ssb
        include/normal/ssb/Globals.h
        src/TestUtil.cpp include/normal/ssb/TestUtil.h
        src/SqlGenerator.cpp include/normal/ssb/SqlGenerator.h
        src/SSBSchema.cpp include/normal/ssb/SSBSchema.h src/GenerateSqlFile.cpp
        src/common/Operators.cpp include/normal/ssb/common/Operators.h )

target_include_directories(normal-ssb PUBLIC include)

target_link_libraries(normal-ssb PRIVATE doctest::doctest)
target_link_libraries(normal-ssb PRIVATE LLVM-filesystem)
target_link_libraries(normal-ssb PRIVATE fmt::fmt)
target_link_libraries(normal-ssb PRIVATE normal-plan)
target_link_libraries(normal-ssb PRIVATE sqlite3_static)
target_link_libraries(normal-ssb PRIVATE sqlite3_csv_static)
target_link_libraries(normal-ssb PUBLIC normal-pushdown)
target_link_libraries(normal-ssb PUBLIC spdlog::spdlog)
target_link_libraries(normal-ssb PRIVATE ssl crypto)


#-----------------------------------------------------------------------------------------------------------------------
# Benchmark
#-----------------------------------------------------------------------------------------------------------------------


#-----------------------------------------------------------------------------------------------------------------------
# Test
#-----------------------------------------------------------------------------------------------------------------------


#-----------------------------------------------------------------------------------------------------------------------
# Tool
#-----------------------------------------------------------------------------------------------------------------------

add_executable(normal-ssb-tool
        tool/Main.cpp)

target_link_libraries(normal-ssb-tool PRIVATE spdlog::spdlog)
target_link_libraries(normal-ssb-tool PRIVATE normal-ssb)
target_link_libraries(normal-ssb-tool PRIVATE normal-tuple)


#-----------------------------------------------------------------------------------------------------------------------
# Data
#-----------------------------------------------------------------------------------------------------------------------

# Generate SSB SF=0.01 data set
#-----------------------------------------------------------------------------------------------------------------------
# Query generator
#-----------------------------------------------------------------------------------------------------------------------

add_library(normal-ssb-query-generate
        src/SqlGenerator.cpp include/normal/ssb/SqlGenerator.h
        src/SqlTransformer.cpp include/normal/ssb/SqlTransformer.h)

target_link_libraries(normal-ssb-query-generate PUBLIC spdlog::spdlog)
target_link_libraries(normal-ssb-query-generate PUBLIC fmt::fmt)
target_include_directories(normal-ssb-query-generate PUBLIC include)

add_executable(normal-ssb-query-generate-file
        src/GenerateSqlFile.cpp)

target_link_libraries(normal-ssb-query-generate-file PRIVATE normal-ssb-query-generate)

#-----------------------------------------------------------------------------------------------------------------------
# Experiment
#-----------------------------------------------------------------------------------------------------------------------

set(OPENSSL_USE_STATIC_LIBS TRUE)
find_package(OpenSSL REQUIRED)

add_executable(normal-ssb-experiment
        experiment/ExperimentMain.cpp
        experiment/Tests.cpp
        experiment/ExperimentUtil.cpp   experiment/ExperimentUtil.h
        experiment/Tests.h
        experiment/MathModelTest.cpp experiment/MathModelTest.h)

target_link_libraries(normal-ssb-experiment doctest::doctest)
target_link_libraries(normal-ssb-experiment Backward::Backward)
target_link_libraries(normal-ssb-experiment normal-ssb)
target_link_libraries(normal-ssb-experiment normal-sql)
target_link_libraries(normal-ssb-experiment normal-ssb-query-generate)
#target_link_libraries(normal-ssb-experiment PRIVATE ssl crypto)

# Copy ssb query files to runtime directory

#-----------------------------------------------------------------------------------------------------------------------
# Metadata
#-----------------------------------------------------------------------------------------------------------------------

#-----------------------------------------------------------------------------------------------------------------------
# Scripts
#-----------------------------------------------------------------------------------------------------------------------

configure_file(${CMAKE_CURRENT_LIST_DIR}/math_model.py ${CMAKE_CURRENT_BINARY_DIR}/math_model.py COPYONLY)
file(COPY scripts DESTINATION ${CMAKE_CURRENT_BINARY_DIR} FILE_PERMISSIONS OWNER_EXECUTE OWNER_WRITE OWNER_READ)


#-----------------------------------------------------------------------------------------------------------------------
# Diagnostics
#-----------------------------------------------------------------------------------------------------------------------
