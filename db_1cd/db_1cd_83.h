/*
   Library for low-level access to 1CD file database.
   Copyright (C) 2021 Denis Matveev (denm.mmm@gmail.com).

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

#pragma once

#include <string>
#include <cassert>
#include <typeinfo>

#include "db_1cd_8x.h"


class db_1cd_83 : public db_1cd_8x
{
public:
    db_1cd_83() = delete;


public:
    static constexpr std::uint32_t VERSION = 0x00080308;    // Suppoted format version.


public:

    class object
    {
    public:
        using size_type = std::uint64_t;                    // 'obj_hdr::length'.

    private:
#pragma pack(push, 1)
        struct obj_hdr
        {
            std::uint16_t type;                             // Object type (0xFD1C).
            std::uint16_t pmt_type;                         // Placement table type (0x0000/0x0001).
            std::uint32_t v1;
            std::uint32_t v2;
            std::uint32_t v3;
            std::uint64_t length;                           // Data size (byte).
            std::uint32_t blocks[0];                        // Indexes of the pages with data/'pmt_hdr'.
        };

        struct pmt_hdr
        {
            std::uint32_t data_blocks[0];                   // Indexes of the pages with object data.
        };
#pragma pack(pop)

        pages& pages_iface;                                 // Interface to read data.
        pages::buffer_type hdr_page;                        // Buffer for the object header.

        pages::index_type page_num_to_index(pages::index_type page_num_);
        pages::index_type page_num_to_index_lite(pages::index_type page_num_) const;

    public:
        object(pages& pages_, pages::index_type index_);

        object::size_type size() const noexcept
        {
            auto* hdr = reinterpret_cast<const obj_hdr*>(hdr_page.data());
            return hdr->length;
        }

        void read(
            void* dst_buff_,
            std::size_t count_, object::size_type pos_);
    };


    using blob = db_1cd_8x::blob<object>;
    using records = db_1cd_8x::records<object>;


    class root : public db_1cd_8x::root
    {
    public:
        using index_type = std::uint32_t;                   // 'root_hdr::numtables'.

    private:
#pragma pack(push, 1)
        struct root_hdr
        {
            char lang[32];                                  // Language label (as example 'ru_RU').
            std::uint32_t numtables;                        // Tables count in database.
            std::uint32_t tables[0];                        // Indexes of the tables descriptions in BLOB.
        };
#pragma pack(pop)

        blob blob_iface;                                    // BLOB-object to read descriptions of tables.
        pages::buffer_type hdr_data;                        // Buffer with root-object header.

    public:
        root(pages& pages_);

        root::index_type size() const noexcept
        {
            auto* hdr = reinterpret_cast<const root_hdr*>(hdr_data.data());
            return hdr->numtables;
        }

        std::wstring read(root::index_type num_);

        table::params get(root::index_type num_)
        {
            const std::wstring descr = read(num_);
            return parse_params(descr);
        }
    };

};
