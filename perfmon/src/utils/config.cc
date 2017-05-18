// Copyright (c) 2017 The Ustore Authors.
// Original Author: caiqc
// Modified by: zl

#include <cstdio>
#include <cstdlib>
#include <iostream>
#include <fstream>
#include <sstream>

#include "utils/config.h"

void Config::loadConfigFile(string file) {
  string line;
  std::ifstream is_file(file);

  while (std::getline(is_file, line)) {
    std::istringstream is_line(line);
    if (line[0] == '#') continue;
    string key;
    if (std::getline(is_line, key, '=')) {
      string value;
      if (std::getline(is_line, value))
        dic[key] = value;
    }
  }

  is_file.close();
}

string Config::get(string feature) {
  if (dic.count(feature) > 0) {
    std::cout << "use configure: " << feature << " = " << dic[feature] << "\n";
    return dic[feature];
  }
  return "";
}
