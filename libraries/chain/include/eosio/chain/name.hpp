#pragma once
#include <string>
#include <fc/reflect/reflect.hpp>
#include <iosfwd>

namespace eosio { namespace chain {
   using std::string;
   using uint128_t           = unsigned __int128;

   // static constexpr uint64_t char_to_symbol( char c ) {
   //    if( c >= 'a' && c <= 'z' )
   //       return (c - 'a') + 6;
   //    if( c >= '1' && c <= '5' )
   //       return (c - '1') + 1;
   //    return 0;
   // }

   static constexpr uint128_t char_to_symbol( char c ) {
      if( c >= 'a' && c <= 'z' )
         return (c - 'a') + 10;
      if( c >= '0' && c <= '9' )
         return c - '0';
      if( c == '_' )
         return 36;
      if( c == '@' )
         return 37;
      if( c == '.' )
         return 38;
      return 0;
   }

   // // Each char of the string is encoded into 5-bit chunk and left-shifted
   // // to its 5-bit slot starting with the highest slot for the first char.
   // // The 13th char, if str is long enough, is encoded into 4-bit chunk
   // // and placed in the lowest 4 bits. 64 = 12 * 5 + 4
   // static constexpr uint64_t string_to_name( const char* str )
   // {
   //    uint64_t name = 0;
   //    int i = 0;
   //    for ( ; str[i] && i < 12; ++i) {
   //        // NOTE: char_to_symbol() returns char type, and without this explicit
   //        // expansion to uint64 type, the compilation fails at the point of usage
   //        // of string_to_name(), where the usage requires constant (compile time) expression.
   //         name |= (char_to_symbol(str[i]) & 0x1f) << (64 - 5 * (i + 1));
   //     }

   //    // The for-loop encoded up to 60 high bits into uint64 'name' variable,
   //    // if (strlen(str) > 12) then encode str[12] into the low (remaining)
   //    // 4 bits of 'name'
   //    if (i == 12)
   //        name |= char_to_symbol(str[12]) & 0x0F;
   //    return name;
   // }

   // Each char of the string is encoded into 6-bit chunk and left-shifted
   // to its 6-bit slot starting with the highest slot for the first char.
   // 128 = 21 * 6 + 2
   static constexpr uint128_t string_to_name( const char* str )
   {
      uint128_t name = 0;
      int i = 0;
      for ( ; str[i] && i < 21; ++i) {
          // NOTE: char_to_symbol() returns char type, and without this explicit
          // expansion to uint128 type, the compilation fails at the point of usage
          // of string_to_name(), where the usage requires constant (compile time) expression.
           name |= (char_to_symbol(str[i]) & 0x3f) << (128 - 6 * (i + 1));
       }

      // The for-loop encoded up to 126 high bits into uint128 'name' variable,
      // the last 2 bits are fill with 1.
      name |= 0x03;
      return name;
   }

#define N(X) eosio::chain::string_to_name(#X)

   struct name {
      uint128_t value = 0;
      bool empty()const { return 0 == value; }
      bool good()const  { return !empty();   }

      name( const char* str )   { set(str);           } 
      name( const string& str ) { set( str.c_str() ); }

      void set( const char* str );

      template<typename T>
      name( T v ):value(v){}
      name(){}

      explicit operator string()const;

      string to_string() const { return string(*this); }

      name& operator=( uint128_t v ) {
         value = v;
         return *this;
      }

      name& operator=( const string& n ) {
         value = name(n).value;
         return *this;
      }
      name& operator=( const char* n ) {
         value = name(n).value;
         return *this;
      }

      friend std::ostream& operator << ( std::ostream& out, const name& n ) {
         return out << string(n);
      }

      friend bool operator < ( const name& a, const name& b ) { return a.value < b.value; }
      friend bool operator <= ( const name& a, const name& b ) { return a.value <= b.value; }
      friend bool operator > ( const name& a, const name& b ) { return a.value > b.value; }
      friend bool operator >=( const name& a, const name& b ) { return a.value >= b.value; }
      friend bool operator == ( const name& a, const name& b ) { return a.value == b.value; }

      friend bool operator == ( const name& a, uint128_t b ) { return a.value == b; }
      friend bool operator != ( const name& a, uint128_t b ) { return a.value != b; }

      friend bool operator != ( const name& a, const name& b ) { return a.value != b.value; }

      operator bool()const            { return value; }
      operator uint128_t()const        { return value; }
      // operator unsigned __int128()const       { return value; }
   };

} } // eosio::chain

namespace std {
   using eosio::chain::uint128_t;

   template<> struct hash<eosio::chain::name> : private hash<uint128_t> {
      typedef eosio::chain::name argument_type;
      typedef typename hash<uint128_t>::result_type result_type;
      result_type operator()(const argument_type& name) const noexcept
      {
         return hash<uint128_t>::operator()(name.value);
      }
   };
};

namespace fc {
  class variant;
  void to_variant(const eosio::chain::name& c, fc::variant& v);
  void from_variant(const fc::variant& v, eosio::chain::name& check);
} // fc


FC_REFLECT( eosio::chain::name, (value) )
