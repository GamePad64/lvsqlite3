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
#include "SQLiteWrapper.h"

namespace librevault {

// SQLValue
SQLValue::SQLValue() : value_type(ValueType::NULL) {}
SQLValue::SQLValue(int64_t int_val) : value_type(ValueType::INT), int_val(int_val) {}
SQLValue::SQLValue(double double_val) : value_type(ValueType::DOUBLE), double_val(double_val) {}
SQLValue::SQLValue(const std::string& text_val) : value_type(ValueType::TEXT), text_val(text_val) {}
SQLValue::SQLValue(const std::vector<uint8_t>& blob_val) : value_type(ValueType::BLOB), blob_val(blob_val){}

// SQLiteResultIterator
SQLiteResultIterator::SQLiteResultIterator(std::shared_ptr<sqlite3_stmt> prepared_stmt,
		std::shared_ptr<int64_t> shared_idx,
		std::shared_ptr<std::vector<std::string>> cols,
		int rescode) : sqlite_statement(prepared_stmt), shared_idx(shared_idx), cols(cols), rescode(rescode) {
	current_idx = shared_idx;
}

SQLiteResultIterator::SQLiteResultIterator(int rescode) : rescode(rescode){}

SQLiteResultIterator& SQLiteResultIterator::operator++() {
	rescode = sqlite3_step(sqlite_statement.get());
	(*shared_idx)++;
	current_idx = shared_idx;
	return *this;
}

SQLiteResultIterator SQLiteResultIterator::operator++(int) {
	SQLiteResultIterator orig = *this;
	++(*this);
	return orig;
}

bool SQLiteResultIterator::operator==(const SQLiteResultIterator& lvalue) {
	return sqlite_statement == lvalue.sqlite_statement && current_idx == lvalue.current_idx;
}

bool SQLiteResultIterator::operator!=(const SQLiteResultIterator& lvalue) {
	if(lvalue.result_code() == SQLITE_DONE && (result_code() == SQLITE_DONE || result_code() == SQLITE_OK)){
		return true;
	}else if(sqlite_statement == lvalue.sqlite_statement && current_idx == lvalue.current_idx){
		return true;
	}
	return false;
}

SQLiteResultIterator::value_type& SQLiteResultIterator::operator*() {
	std::vector<SQLValue> result;
	result.reserve(cols->size());
	for(auto iCol = 0; iCol < cols->size(); iCol++){
		switch(sqlite3_column_type(sqlite_statement.get(), iCol)){
		case SQLValue::ValueType::INT:
			result.push_back(SQLValue((int64_t)sqlite3_column_int64(sqlite_statement.get(), iCol)));
			break;
		case SQLValue::ValueType::DOUBLE:
			result.push_back(SQLValue((double)sqlite3_column_double(sqlite_statement.get(), iCol)));
			break;
		case SQLValue::ValueType::TEXT:
			result.push_back(SQLValue(std::string(sqlite3_column_text(sqlite_statement.get(), iCol))));
			break;
		case SQLValue::ValueType::BLOB:
			auto blob_ptr = sqlite3_column_blob(sqlite_statement.get(), iCol);
			auto blob_size = sqlite3_column_bytes(sqlite_statement.get(), iCol);
			result.push_back(SQLValue(std::vector<uint8_t>(blob_ptr, blob_ptr+blob_size)));
			break;
		case SQLValue::ValueType::NULL:
			result.push_back(SQLValue());
			break;
		}
	}
	return result;
}

// SQLiteResult
SQLiteResult::SQLiteResult(std::shared_ptr<sqlite3_stmt> prepared_stmt, std::mutex& db_mutex) : sqlite_statement(prepared_stmt), db_mutex(db_mutex) {
	*shared_idx = 0;
	db_mutex.lock();
	rescode = sqlite3_step(prepared_stmt.get());
	if(have_rows()){
		auto total_cols = sqlite3_column_count(sqlite_statement.get());
		cols = std::make_shared<std::vector<std::string>>(total_cols);
		for(int col_idx = 0; col_idx < total_cols; col_idx++){
			cols[col_idx] = sqlite3_column_name(sqlite_statement.get(), col_idx);
		}
	}
}

SQLiteResultIterator SQLiteResult::begin() {
	shared_idx = std::make_shared<int64_t>();
	return SQLiteResultIterator(sqlite_statement, shared_idx, cols, rescode);
}

SQLiteResultIterator SQLiteResult::end() {
	return SQLiteResultIterator(SQLITE_DONE);
}

// SQLiteDB
SQLiteDB::SQLiteDB(const boost::filesystem::path& db_path) {
	open(db_path);
}

SQLiteDB::SQLiteDB(const char* db_path) {
	open(db_path);
}

void SQLiteDB::open(const boost::filesystem::path& db_path) {
	open(db_path.string());
}

void SQLiteDB::open(const char* db_path) {
	sqlite3_open(db_path, &db);
}

SQLiteResult SQLiteDB::exec(const std::string& sql, std::vector<BindValue> values){
	std::shared_ptr<sqlite3_stmt> sqlite_stmt;
	char* sql_err_text = 0;
	int sql_err_code = 0;

	sqlite3_stmt* sqlite_stmt_tmp;
	sql_err_code = sqlite3_prepare_v2(db, sql.c_str(), (int)sql.size(), &sqlite_stmt_tmp, 0);
	sqlite_stmt = sqlite_stmt_tmp;

	for(auto value : values){
		switch(value.param_value){
		case SQLValue::ValueType::INT:
			sqlite3_bind_int64(sqlite_stmt.get(), sqlite3_bind_parameter_index(sqlite_stmt.get(), value.param_name), (int64_t)value.param_value);
			break;
		case SQLValue::ValueType::DOUBLE:
			sqlite3_bind_double(sqlite_stmt.get(), sqlite3_bind_parameter_index(sqlite_stmt.get(), value.param_name), (double)value.param_value);
			break;
		case SQLValue::ValueType::TEXT:
			sqlite3_bind_text(sqlite_stmt.get(), sqlite3_bind_parameter_index(sqlite_stmt.get(), value.param_name),
					((std::string&)value.param_value).data(),
					(int)((std::string&)value.param_value).size(),	// TODO use sqlite3_bind_text64
					SQLITE_TRANSIENT);
			break;
		case SQLValue::ValueType::BLOB:
			sqlite3_bind_blob(sqlite_stmt.get(), sqlite3_bind_parameter_index(sqlite_stmt.get(), value.param_name),
					((std::vector<uint8_t>&)value.param_value).data(),
					(int)((std::vector<uint8_t>&)value.param_value).size(),	// TODO use sqlite3_bind_blob64
					SQLITE_TRANSIENT);
			break;
		case SQLValue::ValueType::NULL:
			sqlite3_bind_null(sqlite_stmt.get(), sqlite3_bind_parameter_index(sqlite_stmt.get(), value.param_name));
			break;
		}
	}

	return SQLiteResult(sqlite_stmt, db_mutex);
}

void SQLiteDB::close() {
	sqlite3_close(db);
}

SQLiteDB::~SQLiteDB() {
	close();
}

} /* namespace librevault */
