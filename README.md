# simple-db
A Simple and Efficient Implementation for Small Databases. http://birrell.org/andrew/papers/024-DatabasesPaper-SOSP.pdf

## C++ scaffold
- Build: `cmake -S . -B build && cmake --build build`
- Run CLI demo (creates `simple.db` in cwd): `./build/simpledb_cli`
- Tests: `ctest --test-dir build`
- Core layout starts with pager, fixed-size pages, and a slotted page helper for variable-length records.
