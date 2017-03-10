// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_TYPE_TRAITS_H_
#define USTORE_TYPE_TRAITS_H_

#include <type_traits>

namespace ustore {

template <bool B, typename T>
  using enable_if_t = typename std::enable_if<B, T>::type;

template <typename T>
  using is_integral_t = typename ustore::enable_if_t<std::is_integral<T>::value, T>;

}
#endif
