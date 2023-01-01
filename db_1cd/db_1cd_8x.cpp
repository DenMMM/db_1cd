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

#include <utility>
#include <regex>

#include "db_1cd_8x.h"


std::string db_1cd_8x::file::error::to_string() const
{
    std::string result(1024, ' ');
    
    const DWORD len = ::FormatMessageA(
        FORMAT_MESSAGE_FROM_SYSTEM | FORMAT_MESSAGE_IGNORE_INSERTS,
        nullptr, mycode, MAKELANGID(LANG_NEUTRAL, SUBLANG_DEFAULT),
        &result[0], static_cast<DWORD>(result.size()), nullptr);

    if (len == 0)
        result = "Unknown system error.";

    return result;
}


db_1cd_8x::file::error
db_1cd_8x::file::open(const std::wstring& path_name_)
{
    assert(!is_valid());                                    // File already opened.

    file_handle = ::CreateFileW(
        path_name_.c_str(),
        GENERIC_READ,
        FILE_SHARE_READ,
        nullptr,
        OPEN_EXISTING,
        FILE_FLAG_RANDOM_ACCESS,
        nullptr);

    if (file_handle == INVALID_HANDLE_VALUE)
        return error(::GetLastError());

    LARGE_INTEGER li_size = { 0 };

    if (!::GetFileSizeEx(file_handle, &li_size))
    {
        const DWORD le = ::GetLastError();

        ::CloseHandle(file_handle);
        file_handle = INVALID_HANDLE_VALUE;

        return error(le);
    }

    file_size = li_size.QuadPart;

    return {};
}


db_1cd_8x::file::error
db_1cd_8x::file::read(void* dst_buff_, std::size_t count_, file::size_type pos_) const
{
    assert(is_valid());                                     // File not opened.

    LARGE_INTEGER li_pos = { 0 };
    li_pos.QuadPart = pos_;

    if (!::SetFilePointerEx(file_handle, li_pos, &li_pos, FILE_BEGIN))
    {
        return error(::GetLastError());
    }

    assert(count_ <= std::numeric_limits<DWORD>::max());    // Limitation of the one request size.

    DWORD readed_size = 0;

    if (!::ReadFile(
        file_handle, dst_buff_,
        static_cast<DWORD>(count_), &readed_size,
        nullptr) ||
        readed_size != count_)
    {
        return error(::GetLastError());
    }
    
    return {};
}


db_1cd_8x::file&
db_1cd_8x::file::operator=(file&& src_) noexcept
{
    if (file_handle != INVALID_HANDLE_VALUE)                /// assert() ?
        ::CloseHandle(file_handle);

    file_handle = src_.file_handle;
    src_.file_handle = INVALID_HANDLE_VALUE;

    file_size = src_.file_size;

    return *this;
}


db_1cd_8x::file::~file()
{
    if (file_handle != INVALID_HANDLE_VALUE)
        ::CloseHandle(file_handle);
}


std::string db_1cd_8x::pages::error::to_string() const
{
    switch (mycode)
    {
    case errors::none:                  return "";
    case errors::file_system:           return file_error.to_string();
    case errors::bad_file:              return "Wrong file format.";
    case errors::version:               return "Unsupported version.";
    default:
        throw exception(
            "Unknown error code on DB pages operation.");
    }
}


void db_1cd_8x::pages::cache_init(std::size_t page_size_)
{
    cache_data.clear();
    cache_pool.clear();
    cache_queue.clear();

    cache_data.resize(page_size_ * (cache_size + 1));
    cache_pool.reserve(cache_size + 1);

    for (auto i_data = cache_data.begin(), i_end = cache_data.end();
        i_data != i_end; i_data += page_size_)
    {
        cache_pool.push_back(&(*i_data));
    }
}


db_1cd_8x::pages::error
db_1cd_8x::pages::open(const std::wstring& path_name_)
{
    assert(!is_valid());                                    // File already opened.

    file tmp_iface;
    file::error fe;

    if (!(fe = tmp_iface.open(path_name_)) ||
        !(fe = tmp_iface.read(&db_hdr, sizeof(db_hdr), 0)))
    {
        return error(fe);
    }

    if (std::memcmp("1CDBMSV8", db_hdr.sig, 8) != 0)
    {
        return error(errors::bad_file);
    }

    if (db_hdr.version != 0x000E0208 &&
        db_hdr.version != 0x00080308)
    {
        return error(errors::version);
    }

    if (db_hdr.version == 0x000E0208)                       // In version 8.2.14 pages has fixed size - 4k.
    {
        db_hdr.page_size = 4096;
    }
    else if (
        db_hdr.page_size != 4096 &&
        db_hdr.page_size != 8192 &&
        db_hdr.page_size != 16384 &&
        db_hdr.page_size != 32768 &&
        db_hdr.page_size != 65536)
    {
        return error(errors::bad_file);
    }

    const auto file_size = tmp_iface.size();

    if ((file_size % db_hdr.page_size) != 0 ||
        (file_size / db_hdr.page_size) != db_hdr.length ||
        db_hdr.length == 0)
    {
        return error(errors::bad_file);
    }

    cache_init(db_hdr.page_size);

    file_iface = std::move(tmp_iface);

    return {};
}


const void* db_1cd_8x::pages::view(
    pages::index_type index_,
    std::size_t count_, std::size_t pos_)
{
    assert(is_valid());                                     // File not opened.

    if (index_ == 0 ||
        index_ >= db_hdr.length)
    {
        throw exception(
            "Invalid page index to view.");
    }

    if (pos_ >= db_hdr.page_size ||
        (pos_ + count_) > db_hdr.page_size ||
        (pos_ + count_) < pos_)                             // Overflow checking.
    {
        throw exception(
            "Requested data interval to view exceeds page size.");
    }

    std::optional<void*> cached_page = cache_queue.find(index_);

    if (cached_page.has_value())
    {
        void* ptr = *cached_page;
        return reinterpret_cast<unsigned char*>(ptr) + pos_;
    }

    assert(cache_pool.size() >= 1);                         // No pages in cache pool.

    void* page_from_pool = *cache_pool.rbegin();            // try ...

    const file::size_type pos_in_file =
        static_cast<file::size_type>(db_hdr.page_size) * index_;

    const file::error fe =
        file_iface.read(page_from_pool, db_hdr.page_size, pos_in_file);

    if (!fe)
    {
        throw exception(std::string(
            "Error while reading page from file: ") +
            fe.to_string());
    }

    std::optional<std::pair<pages::index_type, void*>> freed_page =
        cache_queue.push(
            std::make_pair(index_, page_from_pool));

    cache_pool.pop_back();                                  // ... catch

    if (freed_page.has_value())
    {
        assert(cache_pool.size() < cache_size);             // Cache pool overflow.

        cache_pool.push_back(freed_page->second);
    }

    return reinterpret_cast<unsigned char*>(page_from_pool) + pos_;
}


db_1cd_8x::pages::buffer_type db_1cd_8x::blob_base::decompress(
    const pages::buffer_type& src_, std::size_t max_size_)
{
    pages::buffer_type dst;

    if (src_.size() == 0)
        return dst;

    if (max_size_ > std::numeric_limits<uInt>::max())       // ZLIB internal limitation.
        max_size_ = std::numeric_limits<uInt>::max();

    if (src_.size() > max_size_)
    {
        throw exception(
            "Size of data to decompress by ZLIB too large.");
    }

    dst.resize(src_.size());

    z_stream strm = { 0 };
    strm.zalloc = Z_NULL;
    strm.zfree = Z_NULL;
    strm.opaque = Z_NULL;
    strm.total_in = 0;
    strm.avail_in = static_cast<uInt>(src_.size());
    strm.next_in = src_.data();
    strm.total_out = 0;
    strm.avail_out = static_cast<uInt>(dst.size());
    strm.next_out = dst.data();

    try
    {
        int res = inflateInit2(&strm, -MAX_WBITS);

        if (res != Z_OK)
            goto zlib_error;

        do
        {
            res = inflate(&strm, Z_NO_FLUSH);

            if (res == Z_STREAM_END)
            {
                res = inflateEnd(&strm);

                if (res != Z_OK)
                    goto zlib_error;

                dst.resize(strm.total_out);
                return dst;
            }
            else if (res != Z_OK)
                goto zlib_error;

            strm.avail_in = static_cast<uInt>(src_.size() - strm.total_in);
            strm.next_in = src_.data() + strm.total_in;

            if (dst.size() >= max_size_)
            {
                throw exception(
                    "Decompressed by ZLIB data too large.");
            }

            const std::size_t max_increment = max_size_ - dst.size();
            
            if (max_increment < dst.size())
                dst.resize(dst.size() + max_increment);
            else
                dst.resize(dst.size() * 2);

            strm.avail_out = static_cast<uInt>(dst.size() - strm.total_out);
            strm.next_out = dst.data() + strm.total_out;
        } while (strm.avail_in != 0);

        throw exception(
            "Data flow ended before it was decompressed by ZLIB.");

    zlib_error:
        throw exception(std::string(
            "ZLIB error code: ") + std::to_string(res));
    }
    catch (...)
    {
        inflateEnd(&strm);
        throw;
    }
}


std::wstring db_1cd_8x::blob_base::utf8to16(const pages::buffer_type& src_)
{
    std::size_t src_size = src_.size();

    if (src_size < 3 ||
        src_[0] != 0xEF ||
        src_[1] != 0xBB ||
        src_[2] != 0xBF)
    {
        throw exception(
            "The buffer data does not contain UTF-8 string to UTF-16 conversion.");
    }

    if (src_size == 3)
        return {};

    src_size -= 3;

    if (src_size > std::numeric_limits<int>::max())
    {
        throw exception(
            "Length of source string to conversion from UTF-8 to UTF-16 too large.");
    }

    auto src_ptr = reinterpret_cast<LPCCH>(&src_[3]);

    int size = ::MultiByteToWideChar(
        CP_UTF8, 0,
        src_ptr, static_cast<int>(src_size),
        nullptr, 0);

    if (size == 0)
    {
        throw exception(
            "Error while UTF-16 string size calculation.");
    }

    std::wstring result;
    result.resize(size);

    size = ::MultiByteToWideChar(
        CP_UTF8, 0,
        src_ptr, static_cast<int>(src_size),
        &result[0], static_cast<int>(result.size()));

    if (size != result.size())
    {
        throw exception(
            "Error while string conversion UTF-8 to UTF-16.");
    }

    return result;
}


db_1cd_8x::field::binary::binary(
    const fparams& params_, const void* buff_, std::size_t size_) :
    any(params_)
{
    assert(size_ == size(params.length));                   // Buffer size not equal field length

    pages::buffer_type tmp;
    tmp.resize(size_);
    std::memcpy(tmp.data(), buff_, size_);

    exists = std::move(tmp);
}


db_1cd_8x::field::boolean::boolean(
    const fparams& params_, const void* buff_, std::size_t size_) :
    any(params_)
{
    assert(size_ == size(params.length));                   // Buffer size not equal field length

    exists = *reinterpret_cast<const char*>(buff_) == 0 ? false : true;
}


db_1cd_8x::field::digit::digit(
    const fparams& params_, const void* buff_, std::size_t size_) :
    any(params_)
{
    assert(size_ == size(params.length));                   // Buffer size not equal field length

    pages::buffer_type tmp;
    tmp.resize(size_);
    std::memcpy(tmp.data(), buff_, size_);

    exists = std::move(tmp);
}


db_1cd_8x::field::str_fix::str_fix(
    const fparams& params_, const void* buff_, std::size_t size_) :
    any(params_)
{
    assert(size_ == size(params.length));                   // Buffer size not equal field length

    std::wstring tmp(
        reinterpret_cast<const wchar_t*>(buff_),
        this->params.length);

    exists = std::move(tmp);
}


db_1cd_8x::field::str_var::str_var(
    const fparams& params_, const void* buff_, std::size_t size_) :
    any(params_)
{
    assert(size_ == size(params.length));                   // Buffer size not equal field length

    std::uint16_t real_len = 0;
    buff_ = mem_get(buff_, real_len);

    if (real_len > params.length)
    {
        throw exception(
            "String length stored in table record more of field size.");
    }

    std::wstring tmp(
        reinterpret_cast<const wchar_t*>(buff_),
        real_len);

    exists = std::move(tmp);
}


db_1cd_8x::field::version::version(
    const fparams& params_, const void* buff_, std::size_t size_) :
    any(params_)
{
    assert(size_ == size(params.length));                   // Buffer size not equal field length

    value_type tmp;
    buff_ = mem_get(buff_, tmp.v1);
    buff_ = mem_get(buff_, tmp.v2);
    buff_ = mem_get(buff_, tmp.v3);
    buff_ = mem_get(buff_, tmp.v4);

    exists = std::move(tmp);
}


db_1cd_8x::field::str_blob::str_blob(
    const fparams& params_, const void* buff_, std::size_t size_) :
    any(params_)
{
    assert(size_ == size(params.length));                   // Buffer size not equal field length

    value_type tmp;
    buff_ = mem_get(buff_, tmp.index);
    buff_ = mem_get(buff_, tmp.size);

    exists = std::move(tmp);
}


db_1cd_8x::field::bin_blob::bin_blob(
    const fparams& params_, const void* buff_, std::size_t size_) :
    any(params_)
{
    assert(size_ == size(params.length));                   // Buffer size not equal field length

    value_type tmp;
    buff_ = mem_get(buff_, tmp.index);
    buff_ = mem_get(buff_, tmp.size);

    exists = std::move(tmp);
}


db_1cd_8x::field::datetime::datetime(
    const fparams& params_, const void* buff_, std::size_t size_) :
    any(params_)
{
    assert(size_ == size(params.length));                   // Buffer size not equal field length

    value_type tmp;
    buff_ = mem_get(buff_, tmp.year);
    buff_ = mem_get(buff_, tmp.month);
    buff_ = mem_get(buff_, tmp.day);
    buff_ = mem_get(buff_, tmp.hour);
    buff_ = mem_get(buff_, tmp.minute);
    buff_ = mem_get(buff_, tmp.second);

    exists = std::move(tmp);
}


std::wstring db_1cd_8x::root::parse_name(const std::wstring& descr_)
{
    thread_local const std::wregex rgxp_name(LR"_(^\{"([^"]+)")_");
    std::wsmatch name_match;

    if (!std::regex_search(descr_, name_match, rgxp_name))
    {
        throw exception(
            "Table name not found in table description.");
    }

    return name_match[1].str();
}


std::vector<db_1cd_8x::field::fparams>
    db_1cd_8x::root::parse_fields(const std::wstring& descr_)
{
    const auto i_end = std::wsregex_iterator();
    thread_local const std::wregex rgxp_fields(LR"_(^\{"([^"]+)","([^"]+)",([0-9]+),([0-9]+),([0-9]+),"([^"]+)"\})_");
    thread_local const std::map<std::wstring, field::ftype> value_types{
        {L"B",          field::ftype::binary},
        {L"L",          field::ftype::boolean},
        {L"N",          field::ftype::digit},
        {L"NC",         field::ftype::str_fix},
        {L"NVC",        field::ftype::str_var},
        {L"RV",         field::ftype::version},
        {L"NT",         field::ftype::str_blob},
        {L"I",          field::ftype::bin_blob},
        {L"DT",         field::ftype::datetime} };
    thread_local const std::map<std::wstring, bool> case_sens{
        {L"CS",         true},
        {L"CI",         false} };

    std::vector<field::fparams> result;

    for (auto i_field = std::wsregex_iterator(descr_.begin(), descr_.end(), rgxp_fields);
        i_field != i_end; ++i_field)
    {
        auto& field = result.emplace_back();

        try
        {
            field.name = (*i_field)[1].str();
            field.type = value_types.at((*i_field)[2].str());
            field.null_exists = (std::stoul((*i_field)[3].str()) == 0) ? false : true;
            field.length = std::stoul((*i_field)[4].str());
            field.precision = std::stoul((*i_field)[5].str());
            field.case_sens = case_sens.at((*i_field)[6].str());
        }
        catch (std::out_of_range&)
        {
            goto err_bad_fld;
        }
        catch (std::invalid_argument&)
        {
            goto err_bad_fld;
        }
    }

    return result;

err_bad_fld:
    throw exception(
        "Unknown table field format in table description.");
}


bool db_1cd_8x::root::parse_lock(const std::wstring& descr_)
{
    thread_local const std::wregex rgxp_lock(LR"_(^\{"Recordlock","([0-9])"\})_");
    std::wsmatch lock_match;

    if (!std::regex_search(descr_, lock_match, rgxp_lock))
    {
        throw exception(
            "Not found 'Recordlock' parameter in table description.");
    }

    return lock_match[1].str() == L"1" ? true : false;
}


std::array<db_1cd_8x::pages::index_type, 3>
db_1cd_8x::root::parse_files(const std::wstring& descr_)
{
    std::array<pages::index_type, 3> result;

    thread_local const std::wregex rgxp_files(LR"_(^\{"Files",([0-9]+),([0-9]+),([0-9]+)\})_");
    std::wsmatch files_match;

    if (!std::regex_search(descr_, files_match, rgxp_files))
    {
        throw exception(
            "Not found table files parameters in table description.");
    }

    try
    {
        result[0] = std::stoul(files_match[1].str());
        result[1] = std::stoul(files_match[2].str());
        result[2] = std::stoul(files_match[3].str());
    }
    catch (std::out_of_range&)
    {
        goto err_bad_file;
    }
    catch (std::invalid_argument&)
    {
        goto err_bad_file;
    }

    return result;

err_bad_file:
    throw exception(
        "Unknown table files parameters format in table description.");
}


db_1cd_8x::table::params db_1cd_8x::root::parse_params(const std::wstring& descr_)
{
    table::params result;

    result.name = parse_name(descr_);
    result.columns = parse_fields(descr_);
    result.record_lock = parse_lock(descr_);

    const std::array<pages::index_type, 3> files = parse_files(descr_);
    result.i_records = files[0];
    result.i_blob = files[1];
    result.i_indexes = files[2];

    return result;
}
