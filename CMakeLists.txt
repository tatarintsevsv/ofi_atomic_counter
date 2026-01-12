cmake_minimum_required(VERSION 3.5)

project(ofi_atomic LANGUAGES C)

add_executable(ofi_atomic main.c)
target_link_libraries(ofi_atomic pthread)
target_link_libraries(ofi_atomic fabric)
