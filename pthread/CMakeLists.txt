cmake_minimum_required(VERSION 3.20)
project(progettoMulticorePthreadFINALE11 C)

set(CMAKE_C_STANDARD 11)

add_executable(progettoMulticorePthreadFINALE11 main.c)
set(GCC_COVERAGE_COMPILE_FLAGS "-pthread")
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} ${GCC_COVERAGE_COMPILE_FLAGS}" )