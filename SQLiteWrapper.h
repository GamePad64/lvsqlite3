/* Copyright (C) 2014-2015 Alexander Shishenko <GamePad64@gmail.com>
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
#ifndef SRC_SYNC_SQLITEWRAPPER_H_
#define SRC_SYNC_SQLITEWRAPPER_H_
#include <sqlite3.h>

namespace librevault {

class SQLValue {
public:
	enum ValueType {
		INT = SQLITE_INTEGER,
		DOUBLE = SQLITE_FLOAT,
		TEXT = SQLITE_TEXT,
		BLOB = SQLITE_BLOB,
		NULL = SQLITE_NULL
	};
protected:
	ValueType value_type;

	std::vector<uint8_t> blob_val;
	std::string text_val;
	union {
		int64_t int_val;
		double double_val;
	};
public:
	SQLValue();	// Binds NULL value;
	SQLValue(int64_t int_val);	// Binds INT value;
	SQLValue(double double_val);	// Binds DOUBLE value;
	SQLValue(const std::string& text_val);	// Binds TEXT value;
	SQLValue(const std::vector<uint8_t>& blob_val);	// Binds BLOB value;

	ValueType get_type(){return value_type;};

	bool is_null() const {return value_type == ValueType::NULL;};

	operator bool() const {return is_null();}
	operator int64_t() const {return int_val;};
	operator double() const {return double_val;}
	operator std::string&() const {return text_val;}
	operator std::vector<uint8_t>&() const {return blob_val;}
};

class SQLiteResultIterator : public std::iterator<std::input_iterator_tag, std::vector<SQLValue>> {
	std::shared_ptr<sqlite3_stmt> sqlite_statement;
	std::shared_ptr<int64_t> shared_idx;
	std::shared_ptr<std::vector<std::string>> cols;
	int64_t current_idx = 0;
	int rescode = SQLITE_OK;
public:
	SQLiteResultIterator(std::shared_ptr<sqlite3_stmt> prepared_stmt, std::shared_ptr<int64_t> shared_idx, std::shared_ptr<std::vector<std::string>> cols, int rescode);
	SQLiteResultIterator(int rescode);

	SQLiteResultIterator& operator++();
	SQLiteResultIterator operator++(int);
	bool operator==(const SQLiteResultIterator& lvalue);
	bool operator!=(const SQLiteResultIterator& lvalue);
	value_type& operator*();

	int result_code() const {return rescode;};
};

class SQLiteResult {
	int rescode = SQLITE_OK;
	std::mutex& db_mutex;

	std::shared_ptr<sqlite3_stmt> sqlite_statement;
	std::shared_ptr<int64_t> shared_idx;
	std::shared_ptr<std::vector<std::string>> cols;
public:
	SQLiteResult(std::shared_ptr<sqlite3_stmt> prepared_stmt, std::mutex& db_mutex);

	SQLiteResultIterator begin();
	SQLiteResultIterator end();

	int result_code() const {return rescode;};
	bool have_rows(){return result_code() == SQLITE_ROW;}
	std::vector<std::string> column_names(){return *cols;};
};

class SQLiteDB{
	std::mutex db_mutex;
	sqlite3* db = 0;
	struct BindValue {
		const char* param_name;
		SQLValue param_value;
	};
public:
	SQLiteDB(){};
	SQLiteDB(const boost::filesystem::path& db_path);
	SQLiteDB(const char* db_path);

	void open(const boost::filesystem::path& db_path);
	void open(const char* db_path);

	template<BindValue ... Values>
	SQLiteResult exec(const std::string& sql, Values...){
		std::vector<BindValue> values = {Values...};
		exec(sql, values);
	}
	SQLiteResult exec(const std::string& sql, std::vector<BindValue> values);

	void close();

	virtual ~SQLiteDB();
};

} /* namespace librevault */

#endif /* SRC_SYNC_SQLITEWRAPPER_H_ */
