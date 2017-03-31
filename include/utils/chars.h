// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_UTILIS_CHARS_H
#define USTORE_UTILIS_CHARS_H

#include <type_traits>

#include "utils/logging.h"

static inline int appendInteger(char* buf) {return 0;}
static inline int readInteger(char* buf) {return 0;}

template<typename Type1, typename ... Types,
    typename = typename std::enable_if<std::is_integral<Type1>::value || std::is_pointer<Type1>::value>::type>
static int appendInteger(char* buf, Type1 value, Types ... values) noexcept {
    // make sure alignment
    CHECK((uintptr_t)buf % sizeof(Type1) == 0);
    *(reinterpret_cast<Type1*>(buf)) = value;
    return sizeof(Type1) + appendInteger(buf + sizeof(Type1), values...);
}

template<typename Type1, typename ... Types,
    typename = typename std::enable_if<std::is_integral<Type1>::value || std::is_pointer<Type1>::value>::type>
static int readInteger(char* buf, Type1& value, Types&... values) noexcept {
    // make sure alignment
    CHECK((uintptr_t)buf % sizeof(Type1) == 0);
    value = *reinterpret_cast<Type1*>(buf);
    return sizeof(Type1) + readInteger(buf + sizeof(Type1), values...);
}

#endif

