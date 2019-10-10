#include <eosio/chain/name.hpp>
#include <fc/variant.hpp>
#include <boost/algorithm/string.hpp>
#include <fc/exception/exception.hpp>
#include <eosio/chain/exceptions.hpp>

namespace eosio { namespace chain { 

   void name::set( const char* str ) {
      const auto len = strnlen(str, 22);
      EOS_ASSERT(len <= 21, name_type_exception, "Name is longer than 21 characters (${name}) ", ("name", string(str)));
      value = string_to_name(str);
      EOS_ASSERT(to_string() == string(str), name_type_exception,
                 "Name not properly normalized (name: ${name}, normalized: ${normalized}) ",
                 ("name", string(str))("normalized", to_string()));
   }

   // keep in sync with name::to_string() in contract definition for name
   // name::operator string()const {
   //   static const char* charmap = ".12345abcdefghijklmnopqrstuvwxyz";

   //    string str(13,'.');

   //    uint64_t tmp = value;
   //    for( uint32_t i = 0; i <= 12; ++i ) {
   //       char c = charmap[tmp & (i == 0 ? 0x0f : 0x1f)];
   //       str[12-i] = c;
   //       tmp >>= (i == 0 ? 4 : 5);
   //    }

   //    boost::algorithm::trim_right_if( str, []( char c ){ return c == '.'; } );
   //    return str;
   // }

   name::operator string()const {
     static const char* charmap = "0123456789abcdefghijklmnopqrstuvwxyz_@.";

      string str(21,'.');

      uint64_t tmp = value;
      for( uint32_t i = 0; i <= 21; ++i ) {
         if (i > 0) {
            char c = charmap[tmp & 0x3f];
            str[21-i] = c;
         }
         tmp >>= (i == 0 ? 2 : 6);
      }

      boost::algorithm::trim_right_if( str, []( char c ){ return c == '.'; } );
      return str;
   }

} } /// eosio::chain

namespace fc {
  void to_variant(const eosio::chain::name& c, fc::variant& v) { v = std::string(c); }
  void from_variant(const fc::variant& v, eosio::chain::name& check) { check = v.get_string(); }
} // fc
