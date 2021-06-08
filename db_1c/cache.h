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
   Class templates implemented simple cache algorithms:
   FIFO, LRU, 2Q (http://www.vldb.org/conf/1994/P439.PDF).

   To reduce CPU overhead used 'std::vector' internally. Nothing lists or
   associative containers: https://dzone.com/articles/c-benchmark-–-stdvector-vs

   This verson optimised for POD types of keys and values.
   Always use 'find()' before 'push()' ! No additional key uniqueness checks.

   Usage:
1. Create queue object for some elements count. Each element is 'std::pair' of
   key and value objects.
2. On access call 'find()' with key. If successfully, returned 'std::optional'
   with associated value.
3. Call 'push()' to put new element in queue. If no free space, returned 
   'std::optional' contains oldest element of queue.
*/

#pragma once

#include <vector>
#include <optional>
#include <utility>
#include <cassert>


namespace cache
{

    template <typename Tindex, typename Tvalue>
    class fifo
    {
    public:
        using item_type = std::pair<Tindex, Tvalue>;

    protected:
        using cont_type = std::vector<item_type>;
        using iterator = typename cont_type::iterator;
        using size_type = typename cont_type::size_type;

    private:
        const size_type max_size;                           // Maximum items count in the 'items'.
        cont_type items;                                    // Array of pairs 'key/value'.
        iterator next_item;                                 // Position of next item to overwrite.

    protected:
        iterator item_find(Tindex index_) noexcept
        {
            iterator
                i_pos = items.begin(),
                i_end = items.end();

            while (i_pos != i_end)
            {
                if (i_pos->first == index_)
                    break;

                ++i_pos;
            }

            return i_pos;
        }

        std::optional<item_type> item_push(item_type value_)
        {
            std::optional<item_type> result;

            if (items.size() < max_size)
            {
                items.emplace_back(value_);
                next_item = items.end();
            }
            else
            {
                if (next_item == items.end())
                    next_item = items.begin();

                result = *next_item;
                *next_item = value_;

                ++next_item;
            }

            return result;
        }

#ifdef _DEBUG
    public:
        auto begin() noexcept { return items.begin(); }
        auto end() noexcept { return items.end(); }
#endif

    public:
        std::optional<Tvalue> find(Tindex index_)
        {
            std::optional<Tvalue> result;

            iterator i_res = item_find(index_);

            if (i_res != items.end())
                result = i_res->second;

            return result;
        }

        std::optional<item_type> push(item_type value_)
        {
            return item_push(value_);
        }

        void clear() noexcept
        {
            items.clear();
            next_item = items.end();
        }

        fifo(size_type size_) :
            max_size(size_)
        {
            assert(size_ >= 1);                             // Needed at least one item.

            items.reserve(max_size);
            next_item = items.end();
        }

        fifo(const fifo&) = delete;
        fifo(fifo&&) noexcept = default;
        fifo& operator=(const fifo&) = delete;
        fifo& operator=(fifo&&) noexcept = default;
    };


    template <typename Tindex, typename Tvalue>
    class lru
    {
    public:
        using item_type = std::pair<Tindex, Tvalue>;

    protected:
        using cont_type = std::vector<item_type>;
        using iterator = typename cont_type::iterator;
        using size_type = typename cont_type::size_type;

    private:
        const size_type max_size;                           // Maximum items count in the 'items'.
        cont_type items;                                    // Array of pairs 'key/value'.

    protected:
        iterator item_find(Tindex index_) noexcept
        {
            iterator
                i_pos = items.begin(),
                i_end = items.end();

            while (i_pos != i_end)
            {
                if (i_pos->first == index_)
                    break;

                ++i_pos;
            }

            return i_pos;
        }

        bool item_is_top(iterator i_item_) const noexcept
        {
            return i_item_ == items.end() - 1;
        }

        iterator item_move_top(iterator i_item_)
        {
            item_type tmp(*i_item_);

            items.erase(i_item_);
            items.emplace_back(tmp);

            return items.end() - 1;
        }

        std::optional<item_type> item_push(item_type value_)
        {
            std::optional<item_type> result;

            if (items.size() < max_size)
            {
                items.emplace_back(value_);
            }
            else
            {
                result = *items.begin();
                items.erase(items.begin());
                items.emplace_back(value_);
            }

            return result;
        }

#ifdef _DEBUG
    public:
        auto begin() noexcept { return items.begin(); }
        auto end() noexcept { return items.end(); }
#endif

    public:
        std::optional<Tvalue> find(Tindex index_)
        {
            std::optional<Tvalue> result;

            iterator i_res = item_find(index_);

            if (i_res != items.end())
                result = item_move_top(i_res)->second;

            return result;
        }

        std::optional<item_type> push(item_type value_)
        {
            return item_push(value_);
        }

        void clear() noexcept
        {
            items.clear();
        }

        lru(size_type size_) :
            max_size(size_)
        {
            assert(size_ >= 1);                             // Needed at least one item.

            items.reserve(max_size);
        }

        lru(const lru&) = delete;
        lru(lru&&) noexcept = default;
        lru& operator=(const lru&) = delete;
        lru& operator=(lru&&) noexcept = default;
    };


    template <typename Tindex, typename Tvalue>
    class twoq
    {
    public:
        using item_type = std::pair<Tindex, Tvalue>;

    private:
        fifo <Tindex, Tvalue> in;
        fifo <Tindex, char> out;                            /// Replace 'char' by 'int' ?
        lru <Tindex, Tvalue> main;

#ifdef _DEBUG
    public:
        auto in_begin() noexcept { return in.begin(); }
        auto in_end() noexcept { return in.end(); }
        auto out_begin() noexcept { return out.begin(); }
        auto out_end() noexcept { return out.end(); }
        auto main_begin() noexcept { return main.begin(); }
        auto main_end() noexcept { return main.end(); }
#endif

    public:
        std::optional<Tvalue> find(Tindex index_)
        {
            std::optional<Tvalue> result;

            result = main.find(index_);

            if (!result.has_value())
                result = in.find(index_);

            return result;
        }

        std::optional<item_type> push(item_type value_)
        {
            std::optional<item_type> result;

            if (!out.find(value_.first).has_value())
            {
                result = in.push(value_);

                if (result.has_value())
                {
                    out.push(
                        std::make_pair(result->first, 0));
                }
            }
            else
            {
                result = main.push(value_);
            }

            return result;
        }

        void clear() noexcept
        {
            in.clear();
            out.clear();
            main.clear();
        }

        twoq(std::size_t size_) :
            in(size_ / 4),
            out(size_ / 2),
            main(size_ - size_ / 4)
        {
        }

        twoq(const twoq&) = delete;
        twoq(twoq&&) noexcept = default;
        twoq& operator=(const twoq&) = delete;
        twoq& operator=(twoq&&) noexcept = default;
    };

}
