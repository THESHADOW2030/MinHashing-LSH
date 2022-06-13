cmake_minimum_required(VERSION 3.20)
project(progettoMULTICOREopenMP11 C)

set(CMAKE_C_STANDARD 11)

set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -std=c11 -fopenmp")


add_executable(progettoMULTICOREopenMP11 main.c)
