# Distributed under the OSI-approved BSD 3-Clause License.  See accompanying
# file Copyright.txt or https://cmake.org/licensing for details.

cmake_minimum_required(VERSION 3.5)

file(MAKE_DIRECTORY
  "/home/angelo/Documentos/GitHub/KEEPLY/build/_deps/blake3_src-src"
  "/home/angelo/Documentos/GitHub/KEEPLY/build/_deps/blake3_src-build"
  "/home/angelo/Documentos/GitHub/KEEPLY/build/_deps/blake3_src-subbuild/blake3_src-populate-prefix"
  "/home/angelo/Documentos/GitHub/KEEPLY/build/_deps/blake3_src-subbuild/blake3_src-populate-prefix/tmp"
  "/home/angelo/Documentos/GitHub/KEEPLY/build/_deps/blake3_src-subbuild/blake3_src-populate-prefix/src/blake3_src-populate-stamp"
  "/home/angelo/Documentos/GitHub/KEEPLY/build/_deps/blake3_src-subbuild/blake3_src-populate-prefix/src"
  "/home/angelo/Documentos/GitHub/KEEPLY/build/_deps/blake3_src-subbuild/blake3_src-populate-prefix/src/blake3_src-populate-stamp"
)

set(configSubDirs )
foreach(subDir IN LISTS configSubDirs)
    file(MAKE_DIRECTORY "/home/angelo/Documentos/GitHub/KEEPLY/build/_deps/blake3_src-subbuild/blake3_src-populate-prefix/src/blake3_src-populate-stamp/${subDir}")
endforeach()
if(cfgdir)
  file(MAKE_DIRECTORY "/home/angelo/Documentos/GitHub/KEEPLY/build/_deps/blake3_src-subbuild/blake3_src-populate-prefix/src/blake3_src-populate-stamp${cfgdir}") # cfgdir has leading slash
endif()
