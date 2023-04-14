//
//  on 3/12/20.
//

#ifndef WEBCACHESIM_FILE_HASH_H
#define WEBCACHESIM_FILE_HASH_H

//  Boost CRC example program file  ------------------------------------------//

//  Copyright 2003 Daryle Walker.  Use, modification, and distribution are
//  subject to the Boost Software License, Version 1.0.  (See accompanying file
//  LICENSE_1_0.txt or a copy at <http://www.boost.org/LICENSE_1_0.txt>.)

//  See <http://www.boost.org/libs/crc/> for the library's home page.

//  Revision History
//  17 Jun 2003  Initial version (Daryle Walker)

#include <boost/crc.hpp>  // for boost::crc_32_type

#include <fstream>    // for std::ifstream
#include <ios>        // for std::ios_base, etc.

// Redefine this to change to processing buffer size
#ifndef PRIVATE_BUFFER_SIZE
#define PRIVATE_BUFFER_SIZE  4ULL*1024ULL*1024ULL;
#endif

// Global objects
std::streamsize const buffer_size = PRIVATE_BUFFER_SIZE;

// Main program
inline uint32_t get_crc32(const std::string & filename){
    boost::crc_32_type  result;
    std::ifstream  ifs(filename, std::ios_base::binary );
    if (ifs) {
        do {
            char  buffer[ buffer_size ];

            ifs.read( buffer, buffer_size );
            result.process_bytes( buffer, ifs.gcount() );
        } while ( ifs );
    }
    else {
        throw std::runtime_error("Failed to open file " + filename);
    }
    return result.checksum();
}

#endif //WEBCACHESIM_FILE_HASH_H
