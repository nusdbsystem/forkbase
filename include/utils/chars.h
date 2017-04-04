// Copyright (c) 2017 The Ustore Authors.

#ifndef USTORE_UTILIS_CHARS_H
#define USTORE_UTILIS_CHARS_H

#include <type_traits>

#include "utils/type_traits.h"
#include "utils/logging.h"

static inline size_t appendInteger(char* buf) {return 0;}
static inline size_t readInteger(const char* buf) {return 0;}

template<typename Type1, typename ... Types,
    typename = typename ::ustore::is_integral_t<Type1> >
static size_t appendInteger(char* buf, Type1 value, Types ... values) noexcept {
    // make sure alignment
    CHECK((uintptr_t)buf % sizeof(Type1) == 0);
    *(reinterpret_cast<Type1*>(buf)) = value;
    return sizeof(Type1) + appendInteger(buf + sizeof(Type1), values...);
}

template<typename Type1, typename ... Types,
    typename = typename ::ustore::is_integral_t<Type1> >
static size_t readInteger(const char* buf, Type1& value, Types&... values) noexcept {
    // make sure alignment
    CHECK((uintptr_t)buf % sizeof(Type1) == 0);
    value = *reinterpret_cast<Type1*>(const_cast<char*>(buf));
    return sizeof(Type1) + readInteger(const_cast<char*>(buf) + sizeof(Type1), values...);
}

template<typename T, int N>
static size_t readInteger(const char* buf, T* array) noexcept;

template<typename T, int N>
static size_t readInteger(const char* buf, T (&array)[N] ) noexcept {
    return readInteger<T, N>(buf, reinterpret_cast<T*>(array));
}

template<typename T, int N>
static size_t readInteger(const char* buf, T* array, std::true_type) noexcept {
    return readInteger<T, 1>(buf, array) + readInteger<T, N-1>(buf + sizeof(T), array + 1);
}

template<typename T, int N>
static size_t readInteger(const char* buf, T* array, std::false_type) noexcept {
    return readInteger(buf, *array);
}

template<typename T, int N>
static size_t readInteger(const char* buf, T* array) noexcept {
    return readInteger<T,N>(buf, array, std::integral_constant<bool, (N>1)>());
}

#endif

