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
#include <boost/filesystem.hpp>
#include <memory>
#include <map>

namespace librevault {

class SQLValue {
public:
	enum class ValueType {
		INT = SQLITE_INTEGER,
		DOUBLE = SQLITE_FLOAT,
		TEXT = SQLITE_TEXT,
		BLOB = SQLITE_BLOB,
		NULL_VALUE = SQLITE_NULL
	};
protected:
	ValueType value_type;

	union {
		int64_t int_val;
		double double_val;
		struct {
			union {
				const uint8_t* blob_val;
				const char* text_val;
			};
			uint64_t size;
		};
	};
public:
	SQLValue();	// Binds NULL value;
	SQLValue(int64_t int_val);	// Binds INT value;
	SQLValue(double double_val);	// Binds DOUBLE value;

	SQLValue(const std::string& text_val);	// Binds TEXT value;
	SQLValue(const char* text_val, uint64_t blob_size);	// Binds TEXT value;

	SQLValue(const std::vector<uint8_t>& blob_val);	// Binds BLOB value;
	SQLValue(const uint8_t* blob_ptr, uint64_t blob_size);	// Binds BLOB value;
	template<uint64_t array_size> SQLValue(std::array<uint8_t, array_size> blob_array) : SQLValue(blob_array.data(), blob_array.size()){}

	ValueType get_type(){return value_type;};

	bool is_null() const {return value_type == ValueType::NULL_VALUE;};
	int64_t as_int() const {return int_val;}
	double as_double() const {return double_val;}
	std::string as_text() const {return std::string(text_val, text_val+size);}
	std::vector<uint8_t> as_blob() const {return std::vector<uint8_t>(blob_val, blob_val+size);}
	template<uint64_t array_size> std::array<uint8_t, array_size> as_blob() const {
		std::array<uint8_t, array_size> new_array; std::copy(blob_val, blob_val+std::min(size, array_size), new_array.data());
		return new_array;
	}

	operator bool() const {return is_null();}
	operator int64_t() const {return int_val;};
	operator double() const {return double_val;}
	operator std::string() const {return std::string(text_val, text_val+size);}
	operator std::vector<uint8_t>() const {return std::vector<uint8_t>(blob_val, blob_val+size);}
};

class SQLiteResultIterator : public std::iterator<std::input_iterator_tag, std::vector<SQLValue>> {
	sqlite3_stmt* prepared_stmt = 0;
	std::shared_ptr<int64_t> shared_idx;
	std::shared_ptr<std::vector<std::string>> cols;
	std::vector<SQLValue> result;
	int64_t current_idx = 0;
	int rescode = SQLITE_OK;
public:
	SQLiteResultIterator(sqlite3_stmt* prepared_stmt, std::shared_ptr<int64_t> shared_idx, std::shared_ptr<std::vector<std::string>> cols, int rescode);
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

	sqlite3_stmt* prepared_stmt = 0;
	std::shared_ptr<int64_t> shared_idx;
	std::shared_ptr<std::vector<std::string>> cols;
public:
	SQLiteResult(sqlite3_stmt* prepared_stmt);
	virtual ~SQLiteResult();

	void finalize();

	SQLiteResultIterator begin();
	SQLiteResultIterator end();

	int result_code() const {return rescode;};
	bool have_rows(){return result_code() == SQLITE_ROW;}
	std::vector<std::string> column_names(){return *cols;};
};

class SQLiteDB {
	sqlite3* db = 0;
public:
	SQLiteDB(){};
	SQLiteDB(const boost::filesystem::path& db_path);
	SQLiteDB(const char* db_path);
	virtual ~SQLiteDB();

	void open(const boost::filesystem::path& db_path);
	void open(const char* db_path);
	void close();

	sqlite3* sqlite3_handle(){return db;};

	SQLiteResult exec(const std::string& sql, std::map<std::string, SQLValue> values = std::map<std::string, SQLValue>());

	int64_t last_insert_rowid();
};

} /* namespace librevault */

#endif /* SRC_SYNC_SQLITEWRAPPER_H_ */
