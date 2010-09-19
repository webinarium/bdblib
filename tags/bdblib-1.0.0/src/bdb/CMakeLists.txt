#---------------------------------------------------------------------------------------------------
#
#  BDB Library
#  Copyright (C) 2009  Artem Rodygin
#
#  This program is free software: you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation, either version 3 of the License, or
#  (at your option) any later version.
#
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#
#  You should have received a copy of the GNU General Public License
#  along with this program.  If not, see <http://www.gnu.org/licenses/>.
#
#---------------------------------------------------------------------------------------------------

project(bdb)
cmake_minimum_required(VERSION 2.6)

find_package(BerkeleyDB 4.7 REQUIRED)

set(Boost_USE_MULTITHREADED ON)
set(Boost_USE_STATIC_LIBS   ON)
set(Boost_ADDITIONAL_VERSIONS "1.37" "1.38" "1.39" "1.40" "1.41" "1.42" "1.43" "1.44")

find_package(Boost 1.37.0 REQUIRED COMPONENTS date_time)

if (MSVC)

    include(CheckIncludeFile)
    CHECK_INCLUDE_FILE(stdint.h HAVE_STDINT_H)

    if (NOT HAVE_STDINT_H)
    configure_file("${CMAKE_BINARY_DIR}/cmake/stdint.h" "${CMAKE_BINARY_DIR}/include/stdint.h" COPYONLY)
    endif (NOT HAVE_STDINT_H)

endif (MSVC)

aux_source_directory(src ${PROJECT_NAME}_SRC)
list(APPEND ${PROJECT_NAME}_SRC ${CMAKE_BINARY_DIR}/include/bdb.h)

include_directories(${BerkeleyDB_INCLUDE_DIR}
                    ${Boost_INCLUDE_DIRS}
                    ${CMAKE_BINARY_DIR}/include)

add_library(${PROJECT_NAME} SHARED ${${PROJECT_NAME}_SRC})

add_dependencies(${PROJECT_NAME}
                 log4cplus
                 protobuf)

target_link_libraries(${PROJECT_NAME}
                      ${BerkeleyDB_LIBRARIES}
                      ${Boost_LIBRARIES}
                      log4cplus
                      protobuf)

if (MSVC)
add_definitions(-W3)
else (MSVC)
add_definitions(-Wall -Wextra -Wno-sign-compare -ansi)
endif (MSVC)

message(STATUS "Target '${PROJECT_NAME}' is configured")
message("---------------------------------------------")
