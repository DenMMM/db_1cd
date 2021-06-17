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
/*
   Example of extraction a list of the users from database.
*/

#include <iostream>
#include <string>
#include <optional>

#include "db_1cd_83.h"


std::optional<db_1cd_83::table::params> find_table(db_1cd_83::pages& pages_, const std::wstring& name_)
{
    db_1cd_83::root root(pages_);

    for (db_1cd_83::root::index_type i = 0; i < root.size(); ++i)
    {
        db_1cd_83::table::params params = root.get(i);

        if (params.name == name_)
            return params;
    }

    return {};
}


int wmain(int argc, wchar_t* argv[])
{
    if (argc != 2)
    {
        std::cout << "Pass DB file name as parameter." << std::endl;
        return 0;
    }

    try
    {
        std::setlocale(LC_ALL, "");

        db_1cd_83::pages pages(8);
        const db_1cd_83::pages::error err = pages.open(argv[1]);

        if (!err)
        {
            std::cout << err.to_string() << std::endl;
            return -1;
        }

        const auto params = find_table(pages, L"V8USERS");

        if (!params.has_value())
        {
            std::cout << "Table with users list not found." << std::endl;
            return -1;
        }

        db_1cd_83::records records(
            pages,
            params->i_records,
            params->columns);

        for (db_1cd_83::records::index_type i = 0; i < records.size(); ++i)
        {
            records.seek(i);

            if (!records.is_deleted())
            {
                auto field_name = records.get_field<db_1cd_83::field::str_var>(
                    records.field_index(L"NAME"));

                auto field_show = records.get_field<db_1cd_83::field::boolean>(
                    records.field_index(L"SHOW"));

                std::wcout
                    << (field_show.exists.value() ? L"+ " : L"- ")
                    << field_name.exists.value() << std::endl;
            }
        }
    }
    catch (db_1cd_83::exception& e)
    {
        std::cout << "Internal error: " << e.what() << std::endl;
        return -1;
    }
    catch (std::exception& e)
    {
        std::cout << "Unhandled error: " << e.what() << std::endl;
        return -1;
    }

    return 0;
}
