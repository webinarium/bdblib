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

project(test)
cmake_minimum_required(VERSION 2.6)

aux_source_directory(src ${PROJECT_NAME}_SRC)

include_directories(${CMAKE_BINARY_DIR}/include
                    ${CMAKE_BINARY_DIR}/build/include
                    hdr)

link_directories(${CMAKE_LIBRARY_OUTPUT_DIRECTORY})

add_executable(${PROJECT_NAME} ${${PROJECT_NAME}_SRC})

add_dependencies(${PROJECT_NAME}
                 log4cplus
                 protobuf
                 bdb)

target_link_libraries(${PROJECT_NAME}
                      log4cplus
                      protobuf
                      bdb)

if (MSVC)
add_definitions(-W3)
else (MSVC)
add_definitions(-Wall -Wextra -Wno-sign-compare -ansi)
endif (MSVC)

message(STATUS "Target '${PROJECT_NAME}' is configured")
message("---------------------------------------------")
