SET(EXECUTABLE_OUTPUT_PATH ${PROJECT_BINARY_DIR}/bin)
SET(LIBRARY_OUTPUT_PATH ${PROJECT_BINARY_DIR}/lib)

INCLUDE_DIRECTORIES(${PROJECT_SOURCE_DIR}/src)

#ADD_EXECUTABLE(cedar cedar.cpp)
ADD_EXECUTABLE(wordmaker wordmaker.cpp)
TARGET_LINK_LIBRARIES(wordmaker pthread)

FILE(GLOB_RECURSE MARISA_SRC ${PROJECT_SOURCE_DIR}/src/marisa/*.cc)
ADD_LIBRARY(marisa STATIC ${MARISA_SRC})

#ADD_EXECUTABLE(sample sample.cc)
#TARGET_LINK_LIBRARIES(sample marisa)

ADD_EXECUTABLE(hugemaker hugemaker.cpp)
TARGET_LINK_LIBRARIES(hugemaker marisa pthread)
