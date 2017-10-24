%module ustore
%include "std_string.i"

%{
#include "write_batch.h"
%}

namespace ustore_kvdb {

class WriteBatch {
 public:

  void Put(const std::string& key, const std::string& value);
  void Delete(const std::string& key);
  void Clear();
};

}
