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
SQLValue::SQLValue() : value_type(ValueType::NULL_VALUE) {}
SQLValue::SQLValue(int64_t int_val) : value_type(ValueType::INT), int_val(int_val) {}
SQLValue::SQLValue(uint64_t int_val) : value_type(ValueType::INT), int_val((int64_t)int_val) {}
SQLValue::SQLValue(double double_val) : value_type(ValueType::DOUBLE), double_val(double_val) {}

SQLValue::SQLValue(const std::string& text_val) : value_type(ValueType::TEXT), text_val(text_val.data()), size(text_val.size()) {}
SQLValue::SQLValue(const char* text_ptr, uint64_t text_size) : value_type(ValueType::TEXT), text_val(text_ptr), size(text_size) {}

SQLValue::SQLValue(const std::vector<uint8_t>& blob_val) : value_type(ValueType::BLOB), blob_val(blob_val.data()), size(blob_val.size()){}
SQLValue::SQLValue(const uint8_t* blob_ptr, uint64_t blob_size) : value_type(ValueType::BLOB), blob_val(blob_ptr), size(blob_size) {}

// SQLiteResultIterator
SQLiteResultIterator::SQLiteResultIterator(sqlite3_stmt* prepared_stmt,
		std::shared_ptr<int64_t> shared_idx,
		std::shared_ptr<std::vector<std::string>> cols,
		int rescode) : prepared_stmt(prepared_stmt), shared_idx(shared_idx), cols(cols), rescode(rescode) {
	current_idx = *shared_idx;

	if(rescode == SQLITE_ROW){
		result.reserve(cols->size());
		for(auto iCol = 0; iCol < cols->size(); iCol++){
			switch((SQLValue::ValueType)sqlite3_column_type(prepared_stmt, iCol)){
			case SQLValue::ValueType::INT:
				result.push_back(SQLValue((int64_t)sqlite3_column_int64(prepared_stmt, iCol)));
				break;
			case SQLValue::ValueType::DOUBLE:
				result.push_back(SQLValue((double)sqlite3_column_double(prepared_stmt, iCol)));
				break;
			case SQLValue::ValueType::TEXT:
				result.push_back(SQLValue(std::string((const char*)sqlite3_column_text(prepared_stmt, iCol))));
				break;
			case SQLValue::ValueType::BLOB:
			{
				const uint8_t* blob_ptr = (const uint8_t*)sqlite3_column_blob(prepared_stmt, iCol);
				auto blob_size = sqlite3_column_bytes(prepared_stmt, iCol);
				result.push_back(SQLValue(std::vector<uint8_t>(blob_ptr, blob_ptr+blob_size)));
			}
				break;
			case SQLValue::ValueType::NULL_VALUE:
				result.push_back(SQLValue());
				break;
			}
		}
	}
}

SQLiteResultIterator::SQLiteResultIterator(int rescode) : rescode(rescode){}

SQLiteResultIterator& SQLiteResultIterator::operator++() {
	rescode = sqlite3_step(prepared_stmt);
	(*shared_idx)++;
	current_idx = *shared_idx;
	return *this;
}

SQLiteResultIterator SQLiteResultIterator::operator++(int) {
	SQLiteResultIterator orig = *this;
	++(*this);
	return orig;
}

bool SQLiteResultIterator::operator==(const SQLiteResultIterator& lvalue) {
	return prepared_stmt == lvalue.prepared_stmt && current_idx == lvalue.current_idx;
}

bool SQLiteResultIterator::operator!=(const SQLiteResultIterator& lvalue) {
	if((lvalue.result_code() == SQLITE_DONE || lvalue.result_code() == SQLITE_OK) && (result_code() == SQLITE_DONE || result_code() == SQLITE_OK)){
		return false;
	}else if(prepared_stmt == lvalue.prepared_stmt && current_idx == lvalue.current_idx){
		return false;
	}
	return true;
}

const SQLiteResultIterator::value_type& SQLiteResultIterator::operator*() const {
	return result;
}

const SQLiteResultIterator::value_type* SQLiteResultIterator::operator->() const {
	return &result;
}

SQLValue SQLiteResultIterator::operator[](size_t pos) const {
	return result[pos];
}

// SQLiteResult
SQLiteResult::SQLiteResult(sqlite3_stmt* prepared_stmt) : prepared_stmt(prepared_stmt) {
	rescode = sqlite3_step(prepared_stmt);
	shared_idx = std::make_shared<int64_t>();
	*shared_idx = 0;
	cols = std::make_shared<std::vector<std::string>>(0);
	if(have_rows()){
		auto total_cols = sqlite3_column_count(prepared_stmt);
		cols->resize(total_cols);
		for(int col_idx = 0; col_idx < total_cols; col_idx++){
			cols->at(col_idx) = sqlite3_column_name(prepared_stmt, col_idx);
		}
	}else{
		finalize();
	}
}

SQLiteResult::~SQLiteResult(){
	finalize();
}

void SQLiteResult::finalize(){
	sqlite3_finalize(prepared_stmt);
	prepared_stmt = 0;
}

SQLiteResultIterator SQLiteResult::begin() {
	return SQLiteResultIterator(prepared_stmt, shared_idx, cols, rescode);
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

SQLiteDB::~SQLiteDB() {
	close();
}

void SQLiteDB::open(const boost::filesystem::path& db_path) {
	open(db_path.string().c_str());
}

void SQLiteDB::open(const char* db_path) {
	sqlite3_open(db_path, &db);
}

void SQLiteDB::close() {
	sqlite3_close(db);
}

SQLiteResult SQLiteDB::exec(const std::string& sql, std::map<std::string, SQLValue> values){
	int sql_err_code = 0;

	sqlite3_stmt* sqlite_stmt;
	sql_err_code = sqlite3_prepare_v2(db, sql.c_str(), (int)sql.size()+1, &sqlite_stmt, 0);

	for(auto value : values){
		switch(value.second.get_type()){
		case SQLValue::ValueType::INT:
			sqlite3_bind_int64(sqlite_stmt, sqlite3_bind_parameter_index(sqlite_stmt, value.first.c_str()), (int64_t)value.second);
			break;
		case SQLValue::ValueType::DOUBLE:
			sqlite3_bind_double(sqlite_stmt, sqlite3_bind_parameter_index(sqlite_stmt, value.first.c_str()), (double)value.second);
			break;
		case SQLValue::ValueType::TEXT:
		{
			auto text_data = value.second.as_text();
			sqlite3_bind_text(sqlite_stmt, sqlite3_bind_parameter_index(sqlite_stmt, value.first.c_str()),
					text_data.data(),
					text_data.size(),	// TODO use sqlite3_bind_text64
					SQLITE_TRANSIENT);
		}
			break;
		case SQLValue::ValueType::BLOB:
		{
			auto blob_data = value.second.as_blob();
			sqlite3_bind_blob(sqlite_stmt, sqlite3_bind_parameter_index(sqlite_stmt, value.first.c_str()),
					blob_data.data(),
					blob_data.size(),	// TODO use sqlite3_bind_blob64
					SQLITE_TRANSIENT);
		}
			break;
		case SQLValue::ValueType::NULL_VALUE:
			sqlite3_bind_null(sqlite_stmt, sqlite3_bind_parameter_index(sqlite_stmt, value.first.c_str()));
			break;
		}
	}

	return SQLiteResult(sqlite_stmt);
}

int64_t SQLiteDB::last_insert_rowid(){
	return sqlite3_last_insert_rowid(db);
}

SQLiteSavepoint::SQLiteSavepoint(SQLiteDB* db, const std::string savepoint_name) : db(db), name(savepoint_name) {
	db->exec(std::string("SAVEPOINT ")+name);
}
SQLiteSavepoint::~SQLiteSavepoint(){
	db->exec(std::string("RELEASE ")+name);
}

SQLiteLock::SQLiteLock(SQLiteDB* db) : db(db) {
	sqlite3_mutex_enter(sqlite3_db_mutex(db->sqlite3_handle()));
}
SQLiteLock::~SQLiteLock(){
	sqlite3_mutex_leave(sqlite3_db_mutex(db->sqlite3_handle()));
}

} /* namespace librevault */
