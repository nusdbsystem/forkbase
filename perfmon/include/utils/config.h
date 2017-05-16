#ifndef INCLUDE_CONFIG_H
#define INCLUDE_CONFIG_H

#include <cstdio>
#include <cstdlib>
#include <string>
#include <map>

using std::string;
using std::map;

class Config{
  private:
    map<string,string> dic;
  public:
    void loadConfigFile(string file);
    string get(string feature);
};

#endif
