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
/*
   This is basic solution like mini driver. Windows only: wchar_t, WinAPI,
Microsoft Visual Studio 2019. No multithreading support.
   Supported format versions 8.2.14 and 8.3.8.

   This resources used in the development:
http://infostart.ru/public/19734/
http://infostart.ru/public/187832/
http://infostart.ru/article/format-baz-1cd-klassicheskie-i-8-3-8-536343/
https://github.com/madler/zlib


   Implemented objects:

pages
   Provides access to database file. It is used by all objects
   to reading data from database.

   Has internal cache with '2Q' queue management. It is created on call 'open()'
   and frees only after object destroyed. Implements two main access methods:
   'read()' and 'view()'. The second is used for access to page in cache without
   copying data. Pointer returned by 'view()' invalidate after next call
   'read()' or 'view()'.
   One instance for both versions of the database.

object
   Implements reading data from internal database streams.

   Uses placement tables while access to data. This logic depends from version
   of database (differents formats).

blob
   Database stream that stores data outside tables: binary data and long UTF-8
   strings.

   Some data compressed by ZLIB algorithm. Implemented the data decompression
   and conversion UTF-8 strings to UTF-16.

field
   Describes database table fields.

   Each field linked with its parameters in table. Can be created from RAW-data
   (used by 'records').
   Field value can be 'NULL' if 'field::fparams::null_exists' is 'true'. Always
   check the attribute 'exists.has_value()'.
   Some fields referenced to objects in BLOB. Which BLOB to use for reading
   data depends from table parameters.
   'Fields' same as for both versions of the database.

records
   Table entries. Each record it's set of 'field'.

   Use 'seek()' to select the record (load from DB). Record can be deleted -
   check it before access to fields.

table
   In this version desribes parameters only: name, set of fields and data
   objects.

root
   Stores table parameters of the database in text-form.

   Has methods for basic parse table parameters.
*/

#pragma once

#include <string>
#include <vector>
#include <array>
#include <map>
#include <memory>
#include <limits>
#include <optional>
#include <stdexcept>
#include <cassert>
#include <typeinfo>

#define NOMINMAX
#include <windows.h>

#define ZLIB_CONST
#include "zlib.h"

#include "cache.h"


class db_1c_8x
{
public:
    db_1c_8x() = delete;


public:

    class exception : public std::runtime_error
    {
    public:
        explicit exception(const std::string& what_arg) : std::runtime_error(what_arg) {}
        explicit exception(const char* what_arg) : std::runtime_error(what_arg) {}
        explicit exception(const exception& other) noexcept = default;
    };


private:

    class file
    {
    public:
        using size_type = std::uint64_t;

        class error
        {
        private:
            DWORD mycode;                                   // WinAPI error code.

        public:
            std::string to_string() const;

            error() : mycode(NO_ERROR) {}
            error(DWORD code_) : mycode(code_) {}

            operator bool() const noexcept
            {
                return mycode == NO_ERROR;
            }
        };

    private:
        HANDLE file_handle;                                 // WinAPI handle of the opened file.
        file::size_type file_size;                          // Size of this file (bytes).

    public:
        bool is_valid() const noexcept
        {
            return file_handle != INVALID_HANDLE_VALUE;
        }

        error open(const std::wstring &path_name_);
        error read(void *dst_buff_, std::size_t count_, file::size_type pos_) const;
        error close();

        file::size_type size() const noexcept
        {
            assert(is_valid());                             // File not opened.
            return file_size;
        }

        file() :
            file_handle(INVALID_HANDLE_VALUE),
            file_size(0)
        {
        }
        
        file(const file&) = delete;
        file(file&&) = delete;
        file& operator=(const file&) = delete;
        file& operator=(file&&) = delete;

        ~file();
    };


public:

    class pages
    {
    public:
        using index_type = std::uint32_t;                   // 'db_hdr::length'.
        using buffer_type = std::vector<unsigned char>;

        enum class errors
        {
            none = 0,                                       // No error - database successfully opened.
            file_system,                                    // File system error.
            bad_file,                                       // File format not recognised.
            version,                                        // Unknown DB format version.
            LAST
        };

        class error
        {
        private:
            errors mycode;                                  // Pages opening error code.
            file::error file_error;                         // Filesystem level error.

        public:
            errors code() const noexcept
            {
                return mycode;
            }

            std::string to_string() const;

            error() : mycode(errors::none) {}

            error(errors code_) : mycode(code_)
            {
                assert(code_ != errors::none);
                assert(code_ != errors::file_system);
            }

            error(const file::error& fe_) :
                mycode(errors::file_system),
                file_error(fe_)
            {
            }

            operator bool() const noexcept
            {
                return mycode == errors::none;
            }
        };

    private:
#pragma pack(push, 1)
        struct
        {
            char sig[8];                                    // Constant string "1CDBMSV8".
            std::uint32_t version;                          // 0x000E0208 / 0x00080308 (8.2.14.0 / 8.3.8.0).
            std::uint32_t length;                           // Length of DB file (pages).
            std::uint32_t unknown;
            std::uint32_t page_size;                        // Page size, bytes (optionally).
        } db_hdr;                                           // Database header.
#pragma pack(pop)

        file file_iface;                                    // Interface to DB file.

        const std::size_t cache_size;                       // Count of pages in the cache.
        pages::buffer_type cache_data;                      // RAW cache data.
        std::vector<void*> cache_pool;                      // Set of pointers to pages in the cache.
        cache::twoq<pages::index_type, void*> cache_queue;  // Actually cached pages (pointers by index).

        void cache_init(std::size_t page_size_);

    public:
        bool is_valid() const noexcept 
        { 
            return
                file_iface.is_valid() &&
                !cache_data.empty() &&
                !cache_pool.empty();
        }

        error open(const std::wstring& path_name_);

        auto version() const noexcept
        {
            assert(is_valid());                             // File not opened.
            return db_hdr.version;
        }

        std::size_t page_size() const noexcept
        {
            assert(is_valid());                             // File not opened.
            return db_hdr.page_size;
        }

        pages::index_type size() const noexcept
        {
            assert(is_valid());                             // File not opened.
            return db_hdr.length;
        }

        const void* view(
            pages::index_type index_,
            std::size_t count_, std::size_t pos_);

        void read(
            void* dst_buff_,
            pages::index_type index_,
            std::size_t count_, std::size_t pos_)
        {
            std::memcpy(
                dst_buff_,
                view(index_, count_, pos_),
                count_);
        }

        pages(std::size_t cached_) :
            cache_size(cached_),
            cache_queue(cached_)
        {
            std::memset(&db_hdr, 0, sizeof(db_hdr));
        }

        pages(const pages&) = delete;
        pages(pages&&) = delete;
        pages& operator=(const pages&) = delete;
        pages& operator=(pages&&) = delete;
    };


private:

    class blob_base
    {
    public:
        static pages::buffer_type decompress(
            const pages::buffer_type& src_,
            std::size_t max_size_ = std::numeric_limits<uInt>::max());
        static std::wstring utf8to16(const pages::buffer_type& src_);
    };


protected:

    template <typename Tobject_type>
    class blob : public blob_base
    {
    public:
        using index_type = std::uint32_t;                   // 'blob_blk::nextblock'.

    private:
#pragma pack(push, 1)
        struct blob_blk
        {
            std::uint32_t nextblock;                        // Index of the next blob_blk with data.
            std::uint16_t length;                           // How many bytes used in the block.
            unsigned char data[250];                        // Data for read.
        };
#pragma pack(pop)

        Tobject_type obj_iface;                             // Interface of DB object to read blocks.

    public:
        blob(pages& pages_, pages::index_type index_);
        pages::buffer_type get(blob::index_type index_, std::size_t size_ = 0);
    };


protected:

    template <typename type>
    static void const* mem_get(const void* buff_, type& value_) noexcept
    {
        value_ = *reinterpret_cast<const type*>(buff_);
        return reinterpret_cast<const type*>(buff_) + 1;
    }


public:

    class field
    {
    public:
        field() = delete;

    public:
        using index_type = std::uint32_t;

        enum class ftype
        {
            unknown,
            binary,                                         // Binary data in table.
            boolean,                                        // Bool value.
            digit,                                          // Digit (int or float, depends from 'params::precision').
            str_fix,                                        // Fixed-length string (look 'params::length').
            str_var,                                        // Variable-length string (look 'params.length').
            version,                                        // Some version.
            str_blob,                                       // Unlimited-length string (stored in BLOB).
            bin_blob,                                       // Unlimited-size binary data (stored in BLOB).
            datetime                                        // Date-time (from seconds up to years).
        };

        struct fparams
        {
            std::wstring name;                              // Table field name.
            ftype type = ftype::unknown;                    // Its value type.
            bool null_exists = false;                       // NULL-value allowed ?
            std::size_t length = 0;                         // Length of value (for binary, digit, line_fix, line_var).
            std::size_t precision = 0;                      // Digit value precision.
            bool case_sens = false;                         // Case sensitivity.
        };

    public:
        class any
        {
        public:
            const fparams& params;                          // Reference to field parameters passed from table.

        protected:
            any(const fparams& params_) : params(params_) {}

        public:
            virtual ~any() = default;
        };

        class binary : public any
        {
        public:
            using value_type = pages::buffer_type;          // Binary data of the field.
            std::optional<value_type> exists;

        public:
            static constexpr std::size_t size(std::size_t length_) noexcept
            {
                return length_;
            }

            static constexpr ftype type() noexcept
            {
                return ftype::binary;
            }

            binary(const fparams& params_) : any(params_) {}
            binary(const fparams& params_, const void* buff_, std::size_t size_);
        };

        class boolean : public any
        {
        public:
            using value_type = bool;                        // true/false
            std::optional<value_type> exists;

        public:
            static constexpr std::size_t size(std::size_t length_) noexcept
            {
                return 1;
            }

            static constexpr ftype type() noexcept
            {
                return ftype::boolean;
            }

            boolean(const fparams& params_) : any(params_) {}
            boolean(const fparams& params_, const void* buff_, std::size_t size_);
        };

        class digit : public any
        {
        public:
            using value_type = pages::buffer_type;          // Packed digit.
            std::optional<value_type> exists;

        public:
            static constexpr std::size_t size(std::size_t length_) noexcept
            {
                return (length_ + 2) / 2;
            }

            static constexpr ftype type() noexcept
            {
                return ftype::digit;
            }

            digit(const fparams& params_) : any(params_) {}
            digit(const fparams& params_, const void* buff_, std::size_t size_);
        };

        class str_fix : public any
        {
        public:
            using value_type = std::wstring;                // Fixed-size string.
            std::optional<value_type> exists;

        public:
            static constexpr std::size_t size(std::size_t length_) noexcept
            {
                return length_ * sizeof(wchar_t);
            }

            static constexpr ftype type() noexcept
            {
                return ftype::str_fix;
            }

            str_fix(const fparams& params_) : any(params_) {}
            str_fix(const fparams& params_, const void* buff_, std::size_t size_);
        };

        class str_var : public any
        {
        public:
            using value_type = std::wstring;                // Basic string.
            std::optional<value_type> exists;

        public:
            static constexpr std::size_t size(std::size_t length_) noexcept
            {
                return length_ * sizeof(wchar_t) + 2;
            }

            static constexpr ftype type() noexcept
            {
                return ftype::str_var;
            }

            str_var(const fparams& params_) : any(params_) {}
            str_var(const fparams& params_, const void* buff_, std::size_t size_);
        };

        class version : public any
        {
        public:
            struct value_type
            {
                std::uint32_t v1 = 0;                       // Unknown-purpose version.
                std::uint32_t v2 = 0;
                std::uint32_t v3 = 0;
                std::uint32_t v4 = 0;
            };
            std::optional<value_type> exists;

        public:
            static constexpr std::size_t size(std::size_t length_) noexcept
            {
                return 16;
            }

            static constexpr ftype type() noexcept
            {
                return ftype::version;
            }

            version(const fparams& params_) : any(params_) {}
            version(const fparams& params_, const void* buff_, std::size_t size_);
        };

        class str_blob : public any
        {
        public:
            struct value_type
            {
                std::uint32_t index = 0;                    // Index of the first data block in BLOB.
                std::uint32_t size = 0;                     // Size of data (bytes).
            };
            std::optional<value_type> exists;

        public:
            static constexpr std::size_t size(std::size_t length_) noexcept
            {
                return 8;
            }

            static constexpr ftype type() noexcept
            {
                return ftype::str_blob;
            }

            str_blob(const fparams& params_) : any(params_) {}
            str_blob(const fparams& params_, const void* buff_, std::size_t size_);
        };

        class bin_blob : public any
        {
        public:
            struct value_type
            {
                std::uint32_t index = 0;                    // Index of the first data block in BLOB.
                std::uint32_t size = 0;                     // Size of data (bytes).
            };
            std::optional<value_type> exists;

        public:
            static constexpr std::size_t size(std::size_t length_) noexcept
            {
                return 8;
            }

            static constexpr ftype type() noexcept
            {
                return ftype::bin_blob;
            }

            bin_blob(const fparams& params_) : any(params_) {}
            bin_blob(const fparams& params_, const void* buff_, std::size_t size_);
        };

        class datetime : public any
        {
        public:
            struct value_type
            {
                std::uint16_t year = 0;                     // Fours digits of the year.
                std::uint8_t month = 0;                     // Two digits of the month.
                std::uint8_t day = 0;                       // ... day.
                std::uint8_t hour = 0;                      // ... hour.
                std::uint8_t minute = 0;                    // ... minutes.
                std::uint8_t second = 0;                    // ... seconds.
            };
            std::optional<value_type> exists;

        public:
            static constexpr std::size_t size(std::size_t length_) noexcept
            {
                return 7;
            }

            static constexpr ftype type() noexcept
            {
                return ftype::datetime;
            }

            datetime(const fparams& params_) : any(params_) {}
            datetime(const fparams& params_, const void* buff_, std::size_t size_);
        };
    };


protected:

    template <typename Tobject_type>
    class records
    {
    public:
        using index_type = std::uint32_t;

    private:
        struct helper
        {
            field::fparams params;                          // Field format description.
            std::size_t shift = -1;                         // Shift of the field from begin of the record (bytes).
            std::size_t size = 0;                           // Length of the field with special attributes.
        };

        std::vector<helper> fields;                         // Set of data for fast search
        std::map<std::wstring, field::index_type> indexes;  // of the field parameters.

        std::size_t prepare_fields(const std::vector<field::fparams>& params_);

        Tobject_type obj_iface;                             // DB object to read table records.
        pages::buffer_type record;                          // Buffer that stores one table record after call 'seek()'.
        records::index_type records_count;                  // Records count in the table.
        std::optional<records::index_type> last_index;      // Index of the last sucesfully readed table record.

        bool seek_success() const noexcept
        {
            return last_index.has_value();
        }

    public:
        records(
            pages& pages_,
            pages::index_type index_,
            const std::vector<field::fparams>& params_);

        records::index_type size() const noexcept
        {
            return records_count;
        }

        field::index_type field_index(const std::wstring& name_) const;
        void seek(records::index_type index_);
        bool is_deleted() const;

        template <typename Tvalue_type>
        Tvalue_type get_field(field::index_type index_) const;
    };


public:

    class table
    {
    public:
        struct params
        {
            std::wstring name;                              // Table name.
            std::vector<field::fparams> columns;            // Fields parameters.
            bool record_lock = false;                       // Special flag (unknown purpose).
            pages::index_type i_records = 0;                // Index of the object with table records.
            pages::index_type i_blob = 0;                   // ... with long data.
            pages::index_type i_indexes = 0;                // ... with indexes (not implemented).
        };
    };


protected:

    class root
    {
    public:
        static std::wstring parse_name(const std::wstring& descr_);
        static std::vector<field::fparams> parse_fields(const std::wstring& descr_);
        static bool parse_lock(const std::wstring& descr_);
        static std::array<pages::index_type, 3> parse_files(const std::wstring& descr_);
        static table::params parse_params(const std::wstring& descr_);
    };

};


template <typename Tobject_type>
db_1c_8x::blob<Tobject_type>::blob(
    pages& pages_, pages::index_type index_) :
    obj_iface(pages_, index_)
{
    const auto size = obj_iface.size();
    auto blk_count = size / sizeof(blob_blk);

    if ((size % sizeof(blob_blk)) != 0 ||
        blk_count > std::numeric_limits<blob::index_type>::max())
    {
        throw exception(
            "Invalid BLOB-object size.");
    }
}


template <typename Tobject_type>
db_1c_8x::pages::buffer_type
db_1c_8x::blob<Tobject_type>::get(
    blob::index_type index_, std::size_t size_)
{
    if (index_ == 0)
    {
        throw exception(
            "Invalid BLOB index parameter.");
    }

    thread_local blob_blk buffer;
    pages::buffer_type result;

    if (size_ != 0)
        result.reserve(size_);

    const auto blk_count = static_cast<blob::index_type>(
        obj_iface.size() / sizeof(blob_blk));
    blob::index_type loop_prot = blk_count;

    do
    {
        if (index_ >= blk_count)
        {
            throw exception(
                "Index of next BLOB block exceeds object size.");
        }

        obj_iface.read(
            &buffer, sizeof(buffer),
            static_cast<Tobject_type::size_type>(sizeof(blob_blk)) * index_);

        if (buffer.length > sizeof(buffer.data) ||
            (buffer.length == 0 && buffer.nextblock != 0))
        {
            throw exception(
                "Wrong 'length' value in BLOB block.");
        }

        if (size_ != 0 &&
            (result.capacity() - result.size()) < buffer.length)
        {
            throw exception(
                "Not enough destination buffer size for BLOB.");
        }

        auto ibegin = std::begin(buffer.data);
        result.insert(result.end(), ibegin, ibegin + buffer.length);

        if (buffer.nextblock == 0)
        {
            if (size_ != 0 &&
                size_ != result.size())
            {
                throw exception(
                    "Size of BLOB not equal requested value.");
            }

            return result;
        }

        index_ = buffer.nextblock;
    } while (--loop_prot);

    throw exception(
        "Loop detected while BLOB reading.");
}


template <typename Tobject_type>
std::size_t db_1c_8x::records<Tobject_type>::prepare_fields(
    const std::vector<field::fparams>& params_)
{
    if (params_.size() > std::numeric_limits<field::index_type>::max())
    {
        throw exception(
            "Table fields count exceeds maximum allowed value.");
    }

    indexes.clear();
    fields.clear();
    fields.reserve(params_.size());

    field::index_type index = 0;
    std::size_t shift = 1;                                  // First byte - record deletion flag.
    for (const auto& prm : params_)
    {
        std::size_t size =
            prm.null_exists ? 1 : 0;

        switch (prm.type)
        {
        case field::ftype::binary:              size += field::binary::size(prm.length); break;
        case field::ftype::boolean:             size += field::boolean::size(prm.length); break;
        case field::ftype::digit:               size += field::digit::size(prm.length); break;
        case field::ftype::str_fix:             size += field::str_fix::size(prm.length); break;
        case field::ftype::str_var:             size += field::str_var::size(prm.length); break;
        case field::ftype::version:             size += field::version::size(prm.length); break;
        case field::ftype::str_blob:            size += field::str_blob::size(prm.length); break;
        case field::ftype::bin_blob:            size += field::bin_blob::size(prm.length); break;
        case field::ftype::datetime:            size += field::datetime::size(prm.length); break;
        default:
            throw exception(
                "Unknown table field type in table record.");
        }

        helper& hlp = fields.emplace_back();

        hlp.params = prm;
        hlp.shift = shift;
        hlp.size = size;

        indexes[prm.name] = index++;

        shift += size;
    }

    // Table records can't be shortest of the element of the free records chain.
    constexpr std::size_t min_rec_size =
        1 + sizeof(records::index_type);
    if (shift < min_rec_size)
        shift = min_rec_size;

    return shift;
}


template <typename Tobject_type>
db_1c_8x::records<Tobject_type>::records(
    pages& pages_,
    pages::index_type index_,
    const std::vector<field::fparams>& params_) :
    obj_iface(pages_, index_)
{
    const auto rec_size = prepare_fields(params_);
    const auto obj_size = obj_iface.size();
    const auto rec_cnt = obj_size / rec_size;

    if ((obj_size % rec_size) != 0 ||
        rec_cnt > std::numeric_limits<records::index_type>::max())
    {
        throw exception(
            "Invalid table records object size.");
    }

    record.resize(rec_size);
    records_count = static_cast<records::index_type>(rec_cnt);
}


template <typename Tobject_type>
db_1c_8x::field::index_type db_1c_8x::records<Tobject_type>::field_index(
    const std::wstring& name_) const
{
    try
    {
        return indexes.at(name_);
    }
    catch (std::out_of_range&)
    {
        throw exception(
            "Table field by name not found.");
    }
}


template <typename Tobject_type>
void db_1c_8x::records<Tobject_type>::seek(records::index_type index_)
{
    if (index_ >= size())
    {
        throw exception(
            "Requested table record number exceeds object size.");
    }

    if (last_index.has_value() &&
        *last_index == index_)
    {
        return;
    }

    last_index.reset();                                     // try ...
    obj_iface.read(
        record.data(),
        record.size(),
        static_cast<Tobject_type::size_type>(record.size()) * index_);
    last_index = index_;                                    // ... catch
}


template <typename Tobject_type>
bool db_1c_8x::records<Tobject_type>::is_deleted() const
{
    if (!seek_success())                                    /// assert() ?
    {
        throw exception(
            "Attempting to access a table entry before reading it.");
    }

    std::uint8_t deleted = 0;
    mem_get(record.data(), deleted);

    return deleted == 1 ? true : false;
}


template <typename Tobject_type>
template <typename Tvalue_type>
Tvalue_type db_1c_8x::records<Tobject_type>::get_field(field::index_type index_) const
{
    if (!seek_success())                                    /// assert() ?
    {
        throw exception(
            "Attempting to access a table entry before reading it.");
    }

    assert(!is_deleted());                                  // Record does not have data (deleted).

    const auto& helper = fields.at(index_);

    if (helper.params.type != Tvalue_type::type())
    {
        throw exception(
            "Attempting reads table field with wrong type.");
    }

    const void* buff = &record[helper.shift];
    auto size = helper.size;

    if (helper.params.null_exists)
    {
        std::uint8_t has_value = 0;
        buff = mem_get(buff, has_value);

        if (has_value == 0)
            return Tvalue_type(helper.params);

        size -= sizeof(has_value);
    }

    return Tvalue_type(helper.params, buff, size);
}
