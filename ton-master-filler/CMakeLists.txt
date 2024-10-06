cmake_minimum_required(VERSION 3.16)

add_executable(ton-master-filler
    src/main.cpp
    src/InsertManagerPostgres.cpp
    src/IndexScheduler.cpp
)

target_include_directories(ton-master-filler
    PUBLIC external/ton
    PUBLIC external/libpqxx
    PUBLIC tondb-scanner/src
    PUBLIC external/jsoncpp/include
    PUBLIC src/
)

target_link_directories(ton-master-filler
    PUBLIC external/ton
    PUBLIC external/libpqxx
)

target_compile_features(ton-master-filler PRIVATE cxx_std_17)
target_link_libraries(ton-master-filler tondb-scanner overlay tdutils tdactor adnl tl_api dht ton_crypto
        catchain validatorsession validator-disk ton_validator validator-disk smc-envelope pqxx
        ${CMAKE_SOURCE_DIR}/external/jsoncpp/build/lib/libjsoncpp.a)
target_link_options(ton-master-filler PUBLIC -rdynamic)

install(TARGETS ton-master-filler RUNTIME DESTINATION bin)
