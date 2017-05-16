#ifndef INCLUDE_REGIST_H
#define INCLUDE_REGIST_H

#include <cstdlib>
#include <string>

using std::string;


void setRegistPath(const char* path);
bool registInPerfmon(const char* filename);

#endif
