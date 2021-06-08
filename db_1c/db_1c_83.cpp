/*
   Library for low-level access to 1C8 file database.
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

#include "db_1c_83.h"


db_1c_83::object::object(pages& pages_, pages::index_type index_) :
    pages_iface(pages_)
{
    if (pages_iface.version() != VERSION)
    {
        throw exception(
            "Unsupported database format version.");
    }

    const std::size_t page_size = pages_iface.page_size();

    if (page_size < sizeof(obj_hdr))
    {
        throw exception(
            "Page size less object header size.");
    }

    hdr_page.resize(page_size);
    pages_iface.read(hdr_page.data(), index_, page_size, 0);

    auto* hdr = reinterpret_cast <const obj_hdr*>(hdr_page.data());

    if (hdr->type != 0xFD1C ||
        (hdr->pmt_type != 0x00 && hdr->pmt_type != 0x01))
    {
        throw exception(
            "Invalid object type.");
    }

    const auto pages_count =
        hdr->length / page_size +
        (hdr->length % page_size == 0 ? 0 : 1);

    if (pages_count > pages_iface.size())
    {
        throw exception(
            "Object size greater of database size.");
    }
}


db_1c_83::pages::index_type
db_1c_83::object::page_num_to_index(pages::index_type page_num_)
{
    const auto page_size = pages_iface.page_size();
    const auto records_in_hdr = (page_size - sizeof(obj_hdr)) / sizeof(pages::index_type);
    const auto records_in_pmt = page_size / sizeof(pages::index_type);

    const auto pmt_page_num = page_num_ / records_in_pmt;

    if (pmt_page_num >= records_in_hdr)
    {
        throw exception(
            "Page number exceeds limitations of the object placement table.");
    }

    auto *hdr = reinterpret_cast<const obj_hdr*>(hdr_page.data());
    const pages::index_type pmt_page_index = hdr->blocks[pmt_page_num];

    auto* pmt = reinterpret_cast<const pmt_hdr*>(
        pages_iface.view(
            pmt_page_index,
            page_size, 0));

    const auto pmt_record_num = page_num_ % records_in_pmt;
    return pmt->data_blocks[pmt_record_num];
}


db_1c_83::pages::index_type
db_1c_83::object::page_num_to_index_lite(pages::index_type page_num_) const
{
    const auto page_size = pages_iface.page_size();
    const auto records_in_hdr = (page_size - sizeof(obj_hdr)) / sizeof(pages::index_type);

    if (page_num_ >= records_in_hdr)
    {
        throw exception(
            "Page number exceeds limitations of the object placement table.");
    }

    auto *hdr = reinterpret_cast<const obj_hdr*>(hdr_page.data());
    return hdr->blocks[page_num_];
}


void db_1c_83::object::read(void* dst_buff_, std::size_t count_, object::size_type pos_)
{
    auto *hdr = reinterpret_cast<const obj_hdr*>(hdr_page.data());

    if (pos_ >= hdr->length ||
        (pos_ + count_) > hdr->length ||
        (pos_ + count_) < pos_)                             // Overflow checking.
    {
        throw exception(
            "Requested interval to read exceeds object size.");
    }

    const std::size_t page_size = pages_iface.page_size();
    pages::index_type page_num = static_cast<pages::index_type>(pos_ / page_size);
    std::size_t pos_in_page = pos_ % page_size;
    auto* dst_buff__ = reinterpret_cast<unsigned char*>(dst_buff_);

    while (count_ != 0)
    {
        std::size_t to_read = page_size - pos_in_page;
        
        if (count_ < to_read)
            to_read = count_;

        const pages::index_type page_index =
            hdr->pmt_type == 0x01 ?
            page_num_to_index(page_num) :
            page_num_to_index_lite(page_num);

        pages_iface.read(
            dst_buff__, page_index,
            to_read, pos_in_page);

        count_ -= to_read;
        dst_buff__ += to_read;
        pos_in_page = 0;
        ++page_num;
    }
}


db_1c_83::root::root(pages& pages_) :
    blob_iface(pages_, 2)
{
    hdr_data = blob_iface.get(1);

    const auto blob_size = hdr_data.size();
    const auto tables_count =
        (blob_size - sizeof(root_hdr)) /
        sizeof(root::index_type);

    auto* hdr = reinterpret_cast<const root_hdr*>(hdr_data.data());

    if (blob_size < sizeof(root_hdr) ||
        tables_count != hdr->numtables)
    {
        throw exception(
            "Invalid root-object.");
    }
}


std::wstring db_1c_83::root::read(root::index_type num_)
{
    if (num_ >= size())
    {
        throw exception(
            "Requested table index exceeds tables count in database.");
    }

    auto* hdr = reinterpret_cast<const root_hdr*>(hdr_data.data());
    const blob::index_type index = hdr->tables[num_];

    pages::buffer_type res_blob(blob_iface.get(index));

    std::wstring result;
    result.reserve(res_blob.size());

    for (const auto v : res_blob)
        result.push_back(static_cast<wchar_t>(v));          /// Bad solution !
    
    return result;
}
