/*
  Copyright (c) 2013, Dan VerWeire <dverweire@gmail.com>
  Copyright (c) 2010, Lee Smith<notwink@gmail.com>

  Permission to use, copy, modify, and/or distribute this software for any
  purpose with or without fee is hereby granted, provided that the above
  copyright notice and this permission notice appear in all copies.

  THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
  WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
  MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
  ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
  WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
  ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
  OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
*/

#include <string.h>
#include <v8.h>
#include <node.h>
#include <node_version.h>
#include <node_object_wrap.h>
#include <time.h>
#include <uv.h>

#include "odbc.h"
#include "odbc_connection.h"
#include "odbc_result.h"
#include "odbc_statement.h"

#ifdef dynodbc
#include "dynodbc.h"
#endif

#ifdef _WIN32
	#include "strptime.h"
#endif

using namespace v8;
using namespace node;

uv_mutex_t ODBC::g_odbcMutex;
uv_async_t ODBC::g_async;

Persistent<FunctionTemplate> ODBC::constructor_template;

void ODBC::Init(v8::Handle<Object> target) {
	DEBUG_PRINTF("ODBC::Init\n");
	v8::Isolate* isolate = v8::Isolate::GetCurrent();
	v8::EscapableHandleScope scope(isolate);

	Local<FunctionTemplate> t = FunctionTemplate::New(isolate, ODBC::New);
	//Local<FunctionTemplate> t = FunctionTemplate::New(isolate, New);

	// Constructor Template
	constructor_template.Reset(isolate, t);
	//constructor_template = Persistent<FunctionTemplate>::New(t);
	t->SetClassName(String::NewFromUtf8(isolate, "ODBC", String::kInternalizedString));
	//constructor_template->SetClassName(String::NewFromUtf8(isolate, "ODBC", String::kInternalizedString));

	// Reserve space for one Handle<Value>
	//Local<ObjectTemplate> instance_template = constructor_template->InstanceTemplate();
	Local<ObjectTemplate> instance_template = t->InstanceTemplate();
	instance_template->SetInternalFieldCount(1);
  
	// Constants
	NODE_DEFINE_CONSTANT(t, SQL_CLOSE);
	NODE_DEFINE_CONSTANT(t, SQL_DROP);
	NODE_DEFINE_CONSTANT(t, SQL_UNBIND);
	NODE_DEFINE_CONSTANT(t, SQL_RESET_PARAMS);
	NODE_DEFINE_CONSTANT(t, SQL_DESTROY); //SQL_DESTROY is non-standard
	NODE_DEFINE_CONSTANT(t, FETCH_ARRAY);
	NODE_DEFINE_CONSTANT(t, FETCH_OBJECT);
  
	// Prototype Methods
	NODE_SET_PROTOTYPE_METHOD(t, "createConnection", CreateConnection);
	NODE_SET_PROTOTYPE_METHOD(t, "createConnectionSync", CreateConnectionSync);

	// Attach the Database Constructor to the target object
	target->Set(v8::String::NewFromUtf8(isolate, "ODBC", String::kInternalizedString), t->GetFunction());
  
#if NODE_VERSION_AT_LEAST(0, 7, 9)
	// Initialize uv_async so that we can prevent node from exiting
	uv_async_init( uv_default_loop(), &ODBC::g_async,	 (uv_async_cb)ODBC::WatcherCallback);
  
	// Not sure if the init automatically calls uv_ref() because there is weird
	// behavior going on. When ODBC::Init is called which initializes the 
	// uv_async_t g_async above, there seems to be a ref which will keep it alive
	// but we only want this available so that we can uv_ref() later on when
	// we have a connection.
	// so to work around this, I am possibly mistakenly calling uv_unref() once
	// so that there are no references on the loop.
	uv_unref((uv_handle_t *)&ODBC::g_async);
#endif
  
	// Initialize the cross platform mutex provided by libuv
	uv_mutex_init(&ODBC::g_odbcMutex);
}

ODBC::~ODBC() {
	DEBUG_PRINTF("ODBC::~ODBC\n");
	this->Free();
}

void ODBC::Free() {
	DEBUG_PRINTF("ODBC::Free\n");
	if (m_hEnv) {
		uv_mutex_lock(&ODBC::g_odbcMutex);
    
		if (m_hEnv) {
			SQLFreeHandle(SQL_HANDLE_ENV, m_hEnv);
			m_hEnv = NULL;      
		}

		uv_mutex_unlock(&ODBC::g_odbcMutex);
	}
}

void ODBC::New(const v8::FunctionCallbackInfo<v8::Value>& args) {
	DEBUG_PRINTF("ODBC::New\n");
	v8::Isolate* isolate = args.GetIsolate();
	v8::EscapableHandleScope scope(isolate);

	ODBC* dbo = new ODBC();
  
	dbo->Wrap(args.Holder());
	dbo->m_hEnv = NULL;
  
	uv_mutex_lock(&ODBC::g_odbcMutex);
  
	int ret = SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &dbo->m_hEnv);
  
	uv_mutex_unlock(&ODBC::g_odbcMutex);
  
	if (!SQL_SUCCEEDED(ret)) {
		DEBUG_PRINTF("ODBC::New - ERROR ALLOCATING ENV HANDLE!!\n");
    
		Local<Object> objError = ODBC::GetSQLError(SQL_HANDLE_ENV, dbo->m_hEnv);
    
		isolate->ThrowException(objError);
		throw objError;
	}
  
	SQLSetEnvAttr(dbo->m_hEnv, SQL_ATTR_ODBC_VERSION, (SQLPOINTER) SQL_OV_ODBC3, SQL_IS_UINTEGER);
  
	scope.Escape(args.Holder());
}

void ODBC::WatcherCallback(uv_async_t *w, int revents) {
	DEBUG_PRINTF("ODBC::WatcherCallback\n");
	//i don't know if we need to do anything here
}

/*
 * CreateConnection
 */
void ODBC::CreateConnection(const v8::FunctionCallbackInfo<v8::Value>& args) {
	DEBUG_PRINTF("ODBC::CreateConnection\n");
	v8::Isolate* isolate = args.GetIsolate();
	v8::EscapableHandleScope scope(isolate);

	if (args.Length() <= (0) || !args[0]->IsFunction()) {
		isolate->ThrowException(Exception::TypeError(String::NewFromUtf8(isolate, "Argument 0 must be a function")));
		throw Exception::TypeError(String::NewFromUtf8(isolate, "Argument 0 must be a function"));
	}

	Local<Function> cb = Local<Function>::Cast(args[0]);

	ODBC* dbo = ObjectWrap::Unwrap<ODBC>(args.Holder());
	//v8::Handle<v8::External> external = v8::Handle<v8::External>::Cast(args.Data());
	//ODBC* dbo = static_cast<ODBC*>(external->Value());

	//initialize work request
	uv_work_t* work_req = (uv_work_t *)(calloc(1, sizeof(uv_work_t)));

	//initialize our data
	create_connection_work_data* data = (create_connection_work_data *)(calloc(1, sizeof(create_connection_work_data)));

	v8::Persistent<v8::Function> persistent(isolate, cb);
	data->cb = persistent;
	data->dbo = dbo;

	work_req->data = data;

	uv_queue_work(uv_default_loop(), work_req, UV_CreateConnection, (uv_after_work_cb)UV_AfterCreateConnection);

	dbo->Ref();

	args.GetReturnValue().SetUndefined();
}

void ODBC::UV_CreateConnection(uv_work_t* req) {
	DEBUG_PRINTF("ODBC::UV_CreateConnection\n");
	//DEBUG_PRINTF("ODBC::UV_CreateConnection requ=%i\n", &req);

	//std::cout <<"ODBC::UV_CreateConnection req= " <<  req  << "\n";

	//get our work data
	create_connection_work_data* data = (create_connection_work_data *)(req->data);

	//create_connection_work_data* data = static_cast<create_connection_work_data*>(req->data);

	uv_mutex_lock(&ODBC::g_odbcMutex);

	data->result = SQLAllocHandle(SQL_HANDLE_DBC, data->dbo->m_hEnv, &data->hDBC);

	uv_mutex_unlock(&ODBC::g_odbcMutex);
}

void ODBC::UV_AfterCreateConnection(uv_work_t* req, int status) {
	DEBUG_PRINTF("ODBC::UV_AfterCreateConnection\n");

	v8::Isolate* isolate = v8::Isolate::GetCurrent();
	v8::EscapableHandleScope scope(isolate);

	create_connection_work_data* data = (create_connection_work_data *)(req->data);
  
	TryCatch try_catch;

	if (!SQL_SUCCEEDED(data->result)) {
		Local<Value> args[1];
    
		args[0] = ODBC::GetSQLError(SQL_HANDLE_ENV, data->dbo->m_hEnv);

		v8::Local<v8::Function> f = v8::Local<v8::Function>::New(isolate, data->cb);
		f->Call(isolate->GetCurrentContext()->Global(), 1, args);
	}
	else {
		Local<Value> args[2];
		args[0] = External::New(isolate, data->dbo->m_hEnv);
		args[1] = External::New(isolate, data->hDBC);
    
		v8::Local<v8::FunctionTemplate> ft = v8::Local<v8::FunctionTemplate>::New(isolate, ODBCConnection::constructor_template);
		Local<Object> js_result = ft->GetFunction()->NewInstance(2, args);

		args[0] = Local<Value>::New(isolate, Null(isolate));
		args[1] = Local<Object>::New(isolate, js_result);

		v8::Local<v8::Function> f = v8::Local<v8::Function>::New(isolate, data->cb);
		f->Call(isolate->GetCurrentContext()->Global(), 2, args);
	}
  
	if (try_catch.HasCaught()) {
		FatalException(try_catch);
	}
  
	data->dbo->Unref();
	data->cb.Reset();

	free(data);
	free(req);
}

/*
 * CreateConnectionSync
 */
void ODBC::CreateConnectionSync(const v8::FunctionCallbackInfo<v8::Value>& args) {
	DEBUG_PRINTF("ODBC::CreateConnectionSync\n");
	v8::Isolate* isolate = args.GetIsolate();
	v8::EscapableHandleScope scope(isolate);

	ODBC* dbo = ObjectWrap::Unwrap<ODBC>(args.Holder());
   
	HDBC hDBC;
  
	uv_mutex_lock(&ODBC::g_odbcMutex);
  
	//allocate a new connection handle
	SQLRETURN ret = SQLAllocHandle(SQL_HANDLE_DBC, dbo->m_hEnv, &hDBC);
  
	if (!SQL_SUCCEEDED(ret)) {
		DEBUG_PRINTF("ODBC::CreateConnectionSync error\n");
		//TODO: do something!
	}
  
	uv_mutex_unlock(&ODBC::g_odbcMutex);

	Local<Value> params[2];
	params[0] = External::New(isolate, dbo->m_hEnv);
	params[1] = External::New(isolate, hDBC);

	v8::Local<v8::FunctionTemplate> ft = v8::Local<v8::FunctionTemplate>::New(isolate, ODBCConnection::constructor_template);
	Local<Object> js_result = ft->GetFunction()->NewInstance(2, params);

	DEBUG_PRINTF("ODBC::CreateConnectionSync End\n");
	args.GetReturnValue().Set(js_result);
}

/*
 * GetColumns
 */
Column* ODBC::GetColumns(SQLHSTMT hStmt, short* colCount) {
	SQLRETURN ret;
	SQLSMALLINT buflen;

	//always reset colCount for the current result set to 0;
	*colCount = 0; 

	//get the number of columns in the result set
	ret = SQLNumResultCols(hStmt, colCount);
  
	if (!SQL_SUCCEEDED(ret)) {
		return new Column[0];
	}
  
	Column *columns = new Column[*colCount];

	for (int i = 0; i < *colCount; i++) {
		//save the index number of this column
		columns[i].index = i + 1;
		//TODO:that's a lot of memory for each field name....
		columns[i].name = new unsigned char[MAX_FIELD_SIZE];
    
		//set the first byte of name to \0 instead of memsetting the entire buffer
		columns[i].name[0] = '\0';
    
		//get the column name
		ret = SQLColAttribute(hStmt,
			columns[i].index,
#ifdef STRICT_COLUMN_NAMES
			SQL_DESC_NAME,
#else
			SQL_DESC_LABEL,
#endif
			columns[i].name,
			(SQLSMALLINT) MAX_FIELD_SIZE,
			(SQLSMALLINT *) &buflen,
			NULL);
    
		//store the len attribute
		columns[i].len = buflen;
    
		//get the column type and store it directly in column[i].type
		ret = SQLColAttribute(hStmt,
			columns[i].index,
			SQL_DESC_TYPE,
			NULL,
			0,
			NULL,
			&columns[i].type);
	}

	return columns;
}

/*
 * FreeColumns
 */
void ODBC::FreeColumns(Column* columns, short* colCount) {
	for(int i = 0; i < *colCount; i++) {
		delete [] columns[i].name;
	}

	delete [] columns;
  
	*colCount = 0;
}

/*
 * GetColumnValue
 */
Handle<Value> ODBC::GetColumnValue(SQLHSTMT hStmt, Column column, uint16_t* buffer, int bufferLength) {
	v8::Isolate* isolate = v8::Isolate::GetCurrent();
	v8::EscapableHandleScope scope(isolate);
	SQLLEN len = 0;

	//reset the buffer
	buffer[0] = '\0';

	//TODO: SQLGetData can supposedly return multiple chunks, need to do this to 
	//retrieve large fields
	int ret; 
  
	switch ((int) column.type) {
		case SQL_INTEGER : 
		case SQL_SMALLINT :
		case SQL_TINYINT : {
			long value;
        
			ret = SQLGetData(hStmt, column.index, SQL_C_SLONG, &value, sizeof(value), &len);
        
			DEBUG_PRINTF("ODBC::GetColumnValue - Integer: index=%i name=%s type=%i len=%i ret=%i\n", column.index, column.name, column.type, len, ret);
        
			if (len == SQL_NULL_DATA) {
				return Null(isolate);
				//return scope.Escape(Null(isolate));
				//return Null();
			}
			else {
				return scope.Escape(Integer::New(isolate, value));
				//return Integer::New(value);
			}
		}
		break;
		case SQL_NUMERIC :
		case SQL_DECIMAL :
		case SQL_BIGINT :
		case SQL_FLOAT :
		case SQL_REAL :
		case SQL_DOUBLE : {
			double value;
        
			ret = SQLGetData(hStmt, column.index, SQL_C_DOUBLE, &value, sizeof(value), &len);
        
			DEBUG_PRINTF("ODBC::GetColumnValue - Number: index=%i name=%s type=%i len=%i ret=%i val=%f\n", column.index, column.name, column.type, len, ret, value);
        
			if (len == SQL_NULL_DATA) {
				return Null(isolate);
				//return scope.Escape(Null(isolate));
				//return Null();
			}
			else {
				return scope.Escape(Number::New(isolate, value));
				//return Number::New(value);
			}
		}
		break;
		case SQL_DATETIME :
		case SQL_TIMESTAMP : {
			//I am not sure if this is locale-safe or cross database safe, but it 
			//works for me on MSSQL
#ifdef _WIN32
			struct tm timeInfo = {};

			ret = SQLGetData(hStmt, column.index, SQL_C_CHAR, (char *) buffer, bufferLength, &len);

			DEBUG_PRINTF("ODBC::GetColumnValue - W32 Timestamp: index=%i name=%s type=%i len=%i\n", column.index, column.name, column.type, len);

			if (len == SQL_NULL_DATA) {
				return Null(isolate);
				//return scope.Escape(Null(isolate));
				//return Null();
			}
			else {
				if (strptime((char *) buffer, "%Y-%m-%d %H:%M:%S", &timeInfo)) {
					//a negative value means that mktime() should use timezone information
					//and system databases to attempt to determine whether DST is in effect
					//at the specified time.
					timeInfo.tm_isdst = -1;

					//return Date::New((double(mktime(&timeInfo)) * 1000));
					return scope.Escape(Date::New(isolate, (double(mktime(&timeInfo)) * 1000)));
				}
				else {
					return scope.Escape(String::NewFromUtf8(isolate, (char *) buffer));
				}
			}
#else
			struct tm timeInfo = { 
				tm_sec : 0
				, tm_min : 0
				, tm_hour : 0
				, tm_mday : 0
				, tm_mon : 0
				, tm_year : 0
				, tm_wday : 0
				, tm_yday : 0
				, tm_isdst : 0
				, tm_gmtoff : 0
				, tm_zone : 0
			};

			SQL_TIMESTAMP_STRUCT odbcTime;
      
			ret = SQLGetData(hStmt, column.index, SQL_C_TYPE_TIMESTAMP, &odbcTime, bufferLength, &len);

			DEBUG_PRINTF("ODBC::GetColumnValue - Unix Timestamp: index=%i name=%s type=%i len=%i\n", column.index, column.name, column.type, len);

			if (len == SQL_NULL_DATA) {
				return Null(isolate);
				//return scope.Escape(Null(isolate));
				//return Null();
			}
			else {
				timeInfo.tm_year = odbcTime.year - 1900;
				timeInfo.tm_mon = odbcTime.month - 1;
				timeInfo.tm_mday = odbcTime.day;
				timeInfo.tm_hour = odbcTime.hour;
				timeInfo.tm_min = odbcTime.minute;
				timeInfo.tm_sec = odbcTime.second;

				//a negative value means that mktime() should use timezone information 
				//and system databases to attempt to determine whether DST is in effect 
				//at the specified time.
				timeInfo.tm_isdst = -1;
#ifdef TIMEGM
				return scope.Escape(Date::New(isolate, (double(timegm(&timeInfo)) * 1000) + (odbcTime.fraction / 1000000)));
#else
				return scope.Escape(Date::New(isolate, (double(timelocal(&timeInfo)) * 1000) + (odbcTime.fraction / 1000000)));
#endif
				//return Date::New((double(timegm(&timeInfo)) * 1000) + (odbcTime.fraction / 1000000));
			}
#endif
		}
		break;
		case SQL_BIT :
			//again, i'm not sure if this is cross database safe, but it works for 
			//MSSQL
			ret = SQLGetData(hStmt, column.index, SQL_C_CHAR, (char *) buffer, bufferLength, &len);

			DEBUG_PRINTF("ODBC::GetColumnValue - Bit: index=%i name=%s type=%i len=%i\n", column.index, column.name, column.type, len);

			if (len == SQL_NULL_DATA) {
				return Null(isolate);
				//return scope.Escape(Null(isolate));
				//return Null();
			}
			else {
				return scope.Escape(BooleanObject::New((*buffer == '0') ? false : true));
				//return scope.Escape(Boolean::New(isolate, (*buffer == '0') ? false : true));
				//return Boolean::New(( *buffer == '0') ? false : true );
			}
		default :
			Local<String> str;
			int count = 0;
      
			do {
				ret = SQLGetData(hStmt, column.index, SQL_C_TCHAR, (char *) buffer, bufferLength, &len);

				DEBUG_PRINTF("ODBC::GetColumnValue - String: index=%i name=%s type=%i len=%i value=%s ret=%i bufferLength=%i\n", column.index, column.name, column.type, len, (char *)buffer, ret, bufferLength);

				if (len == SQL_NULL_DATA && str.IsEmpty()) {
					return Null(isolate);
					//return scope.Escape(Null(isolate));
					//return Null();
				}
        
				if (SQL_NO_DATA == ret) {
					//we have captured all of the data
					//double check that we have some data else return null
					if (str.IsEmpty()) {
						return Null(isolate);
						//return scope.Escape(Null(isolate));
					}
					break;
				}
				else if (SQL_SUCCEEDED(ret)) {
					//we have not captured all of the data yet
					if (count == 0) {
						//no concatenation required, this is our first pass
#ifdef UNICODE
						str = String::NewFromTwoByte(isolate, (uint16_t *)buffer);
#else
						str = String::NewFromUtf8(isolate, (char *)buffer);
#endif
					}
					else {
						//we need to concatenate
#ifdef UNICODE
						str = String::Concat(str, String::NewFromTwoByte(isolate, (uint16_t *)buffer));
#else
						str = String::Concat(str, String::NewFromUtf8(isolate, (char *)buffer));
#endif
					}
          
					//if len is zero let's break out of the loop now and not attempt to
					//call SQLGetData again. The specific reason for this is because
					//some ODBC drivers may not correctly report SQL_NO_DATA the next
					//time around causing an infinite loop here
					if (len == 0) {
						break;
					}
          
					count += 1;
				}
				else {
					//an error has occured
					//possible values for ret are SQL_ERROR (-1) and SQL_INVALID_HANDLE (-2)

					//If we have an invalid handle, then stuff is way bad and we should abort
					//immediately. Memory errors are bound to follow as we must be in an
					//inconsisant state.
					assert(ret != SQL_INVALID_HANDLE);

					//Not sure if throwing here will work out well for us but we can try
					//since we should have a valid handle and the error is something we 
					//can look into
					return isolate->ThrowException(ODBC::GetSQLError(SQL_HANDLE_STMT, hStmt, (char *) "[node-odbc] Error in ODBC::GetColumnValue"));
          			break;
				}
			} while (true);
     
			return scope.Escape(str);
			//return str;
	}
}

/*
 * GetRecordTuple
 */
Local<Object> ODBC::GetRecordTuple(SQLHSTMT hStmt, Column* columns, short* colCount, uint16_t* buffer, int bufferLength) {
	v8::Isolate* isolate = v8::Isolate::GetCurrent();
	v8::EscapableHandleScope scope(isolate);

	Local<Object> tuple = Object::New(isolate);

	//Local<Array> array = Array::New(isolate);
	for(int i = 0; i < *colCount; i++) {
#ifdef UNICODE
		tuple->Set(String::NewFromTwoByte(isolate, (uint16_t *)columns[i].name), GetColumnValue(hStmt, columns[i], buffer, bufferLength));
		//array->Set(0, GetColumnValue(hStmt, columns[i], buffer, bufferLength));
		//array->Set(1, Integer::New(isolate, columns[i].type));
		//tuple->Set(String::NewFromTwoByte(isolate, (uint16_t *)columns[i].name), array);
#else
		tuple->Set( String::NewFromUtf8((const char *) columns[i].name), GetColumnValue( hStmt, columns[i], buffer, bufferLength));
#endif
	}
  
	//return tuple;
	return scope.Escape(tuple);
}

/*
 * GetRecordArray
 */
Handle<Value> ODBC::GetRecordArray(SQLHSTMT hStmt, Column* columns, short* colCount, uint16_t* buffer, int bufferLength) {
	v8::Isolate* isolate = v8::Isolate::GetCurrent();
	v8::EscapableHandleScope scope(isolate);

	Local<Array> array = Array::New(isolate);
        
	for(int i = 0; i < *colCount; i++) {
		array->Set( Integer::New(isolate, i), GetColumnValue( hStmt, columns[i], buffer, bufferLength));
	}
  
	//return array;
	return scope.Escape(array);
}

/*
 * GetParametersFromArray
 */
Parameter* ODBC::GetParametersFromArray (Local<Array> values, int *paramCount) {
	DEBUG_PRINTF("ODBC::GetParametersFromArray\n");
	*paramCount = values->Length();
  
	Parameter* params = NULL;
  
	if (*paramCount > 0) {
		params = (Parameter *) malloc(*paramCount * sizeof(Parameter));
	}
  
	for (int i = 0; i < *paramCount; i++) {
		Local<Value> value = values->Get(i);
    
		params[i].ColumnSize       = 0;
		params[i].StrLen_or_IndPtr = SQL_NULL_DATA;
		params[i].BufferLength     = 0;
		params[i].DecimalDigits    = 0;

		DEBUG_PRINTF("ODBC::GetParametersFromArray - &param[%i].length = %X\n", i, &params[i].StrLen_or_IndPtr);

		if (value->IsString()) {
			Local<String> string = value->ToString();
			int length = string->Length();
      
			params[i].ValueType         = SQL_C_TCHAR;
			params[i].ColumnSize        = 0; //SQL_SS_LENGTH_UNLIMITED 
#ifdef UNICODE
			params[i].ParameterType     = SQL_WVARCHAR;
			params[i].BufferLength      = (length * sizeof(uint16_t)) + sizeof(uint16_t);
#else
			params[i].ParameterType     = SQL_VARCHAR;
			params[i].BufferLength      = string->Utf8Length() + 1;
#endif
			params[i].ParameterValuePtr = malloc(params[i].BufferLength);
			params[i].StrLen_or_IndPtr  = SQL_NTS;//params[i].BufferLength;

#ifdef UNICODE
			string->Write((uint16_t *) params[i].ParameterValuePtr);
#else
			string->WriteUtf8((char *) params[i].ParameterValuePtr);
#endif

			DEBUG_PRINTF("ODBC::GetParametersFromArray - IsString(): params[%i] c_type=%i type=%i buffer_length=%i size=%i length=%i value=%s\n", i, params[i].ValueType, params[i].ParameterType,
				params[i].BufferLength, params[i].ColumnSize, params[i].StrLen_or_IndPtr, 
				(char*) params[i].ParameterValuePtr);
		}
		else if (value->IsNull()) {
			params[i].ValueType = SQL_C_DEFAULT;
			params[i].ParameterType   = SQL_VARCHAR;
			params[i].StrLen_or_IndPtr = SQL_NULL_DATA;

			DEBUG_PRINTF("ODBC::GetParametersFromArray - IsNull(): params[%i] c_type=%i type=%i buffer_length=%i size=%i length=%i\n",
				i, params[i].ValueType, params[i].ParameterType,
				params[i].BufferLength, params[i].ColumnSize, params[i].StrLen_or_IndPtr);
		}
		else if (value->IsInt32()) {
			int64_t  *number = new int64_t(value->IntegerValue());
			params[i].ValueType = SQL_C_SBIGINT;
			params[i].ParameterType   = SQL_BIGINT;
			params[i].ParameterValuePtr = number;
			params[i].StrLen_or_IndPtr = 0;
      
			DEBUG_PRINTF("ODBC::GetParametersFromArray - IsInt32(): params[%i] c_type=%i type=%i buffer_length=%i size=%i length=%i value=%lld\n", i, params[i].ValueType, params[i].ParameterType,
				params[i].BufferLength, params[i].ColumnSize, params[i].StrLen_or_IndPtr,
				*number);
		}
		else if (value->IsNumber()) {
			double *number   = new double(value->NumberValue());
      
			params[i].ValueType         = SQL_C_DOUBLE;
			params[i].ParameterType     = SQL_DECIMAL;
			params[i].ParameterValuePtr = number;
			params[i].BufferLength      = sizeof(double);
			params[i].StrLen_or_IndPtr  = params[i].BufferLength;
			params[i].DecimalDigits     = 7;
			params[i].ColumnSize        = sizeof(double);

			DEBUG_PRINTF("ODBC::GetParametersFromArray - IsNumber(): params[%i] c_type=%i type=%i buffer_length=%i size=%i length=%i value=%f\n",
				i, params[i].ValueType, params[i].ParameterType,
				params[i].BufferLength, params[i].ColumnSize, params[i].StrLen_or_IndPtr,
				*number);
		}
		else if (value->IsBoolean()) {
			bool *boolean = new bool(value->BooleanValue());
			params[i].ValueType         = SQL_C_BIT;
			params[i].ParameterType     = SQL_BIT;
			params[i].ParameterValuePtr = boolean;
			params[i].StrLen_or_IndPtr  = 0;
      
			DEBUG_PRINTF("ODBC::GetParametersFromArray - IsBoolean(): params[%i] c_type=%i type=%i buffer_length=%i size=%i length=%i\n",
				i, params[i].ValueType, params[i].ParameterType,
				params[i].BufferLength, params[i].ColumnSize, params[i].StrLen_or_IndPtr);
		}
	} 
  
	return params;
}

/*
 * CallbackSQLError
 */

Handle<Value> ODBC::CallbackSQLError(SQLSMALLINT handleType, SQLHANDLE handle, Persistent<Function, CopyablePersistentTraits<v8::Function>> cb) {
	v8::Isolate* isolate = v8::Isolate::GetCurrent();
	v8::EscapableHandleScope scope(isolate);

	return CallbackSQLError(handleType, handle, (char *) "[node-odbc] SQL_ERROR", cb);
	//return scope.Close(CallbackSQLError(handleType, handle, (char *) "[node-odbc] SQL_ERROR", cb));
}

Handle<Value> ODBC::CallbackSQLError(SQLSMALLINT handleType, SQLHANDLE handle, char* message, Persistent<Function, CopyablePersistentTraits<v8::Function>> cb) {
	v8::Isolate* isolate = v8::Isolate::GetCurrent();
	v8::EscapableHandleScope scope(isolate);

	Local<Object> objError = ODBC::GetSQLError(handleType, handle, message);
  
	Local<Value> args[1];
	args[0] = objError;

	v8::Local<v8::Function> f = v8::Local<v8::Function>::New(isolate, cb);
	f->Call(isolate->GetCurrentContext()->Global(), 1, args);

	return Undefined(isolate);
	//return scope.Close(Undefined());
}

/*
 * GetSQLError
 */
Local<Object> ODBC::GetSQLError (SQLSMALLINT handleType, SQLHANDLE handle) {
	v8::Isolate* isolate = v8::Isolate::GetCurrent();
	v8::EscapableHandleScope scope(isolate);

	return scope.Escape(GetSQLError(handleType, handle, (char *) "[node-odbc] SQL_ERROR"));
}

Local<Object> ODBC::GetSQLError (SQLSMALLINT handleType, SQLHANDLE handle, char* message) {
	v8::Isolate* isolate = v8::Isolate::GetCurrent();
	v8::EscapableHandleScope scope(isolate);

	DEBUG_PRINTF("ODBC::GetSQLError : handleType=%i, handle=%p\n", handleType, handle);
  
	Local<Object> objError = Object::New(isolate);
	Local<String> str = String::NewFromUtf8(isolate, "");

	SQLINTEGER i = 0;
	SQLINTEGER native;
  
	SQLSMALLINT len;
	SQLINTEGER statusRecCount;
	SQLRETURN ret;
	char errorSQLState[14];
	char errorMessage[ERROR_MESSAGE_BUFFER_BYTES];

	ret = SQLGetDiagField(handleType, handle, 0, SQL_DIAG_NUMBER, &statusRecCount, SQL_IS_INTEGER, &len);

	// Windows seems to define SQLINTEGER as long int, unixodbc as just int... %i should cover both
	DEBUG_PRINTF("ODBC::GetSQLError : called SQLGetDiagField; ret=%i, statusRecCount=%i\n", ret, statusRecCount);

	for (i = 0; i < statusRecCount; i++) {
		DEBUG_PRINTF("ODBC::GetSQLError : calling SQLGetDiagRec; i=%i, statusRecCount=%i\n", i, statusRecCount);
    
		ret = SQLGetDiagRec(handleType, handle, i + 1, (SQLTCHAR *) errorSQLState, &native, (SQLTCHAR *) errorMessage, ERROR_MESSAGE_BUFFER_CHARS, &len);
    
		DEBUG_PRINTF("ODBC::GetSQLError : after SQLGetDiagRec; i=%i\n", i);

		if (SQL_SUCCEEDED(ret)) {
			DEBUG_PRINTF("ODBC::GetSQLError : errorMessage=%s, errorSQLState=%s\n", errorMessage, errorSQLState);

			objError->Set(String::NewFromUtf8(isolate, "error"), String::NewFromUtf8(isolate, message));
#ifdef UNICODE
			str = String::Concat(str, String::NewFromTwoByte(isolate, (uint16_t *) errorMessage));

			objError->SetPrototype(Exception::Error(String::NewFromTwoByte(isolate, (uint16_t *)errorMessage)));
			objError->Set(String::NewFromUtf8(isolate, "message"), str);
			objError->Set(String::NewFromUtf8(isolate, "state"), String::NewFromTwoByte(isolate, (uint16_t *)errorSQLState));
#else
			str = String::Concat(str, String::New(errorMessage));

			objError->SetPrototype(Exception::Error(String::New(errorMessage)));
			objError->Set(String::NewFromUtf8(isolate, "message"), str);
			objError->Set(String::NewFromUtf8(isolate, "state"), String::NewFromUtf8(isolate, errorSQLState));
#endif
		}
		else if (ret == SQL_NO_DATA) {
			break;
		}
	}

	if (statusRecCount == 0) {
		//Create a default error object if there were no diag records
		objError->Set(String::NewFromUtf8(isolate, "error"), String::NewFromUtf8(isolate, message));
		objError->SetPrototype(Exception::Error(String::NewFromUtf8(isolate, message)));
		objError->Set(String::NewFromUtf8(isolate, "message"), String::NewFromUtf8(isolate, (const char *) "[node-odbc] An error occurred but no diagnostic information was available."));
	}

	return scope.Escape(objError);
}

/*
 * GetAllRecordsSync
 */
Local<Array> ODBC::GetAllRecordsSync (HENV hENV, HDBC hDBC, HSTMT hSTMT, uint16_t* buffer, int bufferLength) {
	DEBUG_PRINTF("ODBC::GetAllRecordsSync\n");
  
	v8::Isolate* isolate = v8::Isolate::GetCurrent();
	v8::EscapableHandleScope scope(isolate);

	Local<Object> objError = Object::New(isolate);
  
	int count = 0;
	int errorCount = 0;
	short colCount = 0;
  
	Column* columns = GetColumns(hSTMT, &colCount);
  
	Local<Array> rows = Array::New(isolate);
  
	//loop through all records
	while (true) {
		SQLRETURN ret = SQLFetch(hSTMT);
    
		//check to see if there was an error
		if (ret == SQL_ERROR)  {
			//TODO: what do we do when we actually get an error here...
			//should we throw??
      
			errorCount++;
      
			objError = ODBC::GetSQLError(SQL_HANDLE_STMT, hSTMT, (char *) "[node-odbc] Error in ODBC::GetAllRecordsSync");
      
			break;
		}
    
		//check to see if we are at the end of the recordset
		if (ret == SQL_NO_DATA) {
			ODBC::FreeColumns(columns, &colCount);
    		break;
		}

		rows->Set(Integer::New(isolate, count), ODBC::GetRecordTuple(hSTMT, columns, &colCount, buffer, bufferLength));

		count++;
	}
	//TODO: what do we do about errors!?!
	//we throw them
	return scope.Escape(rows);
}

#ifdef dynodbc
void ODBC::LoadODBCLibrary(const v8::FunctionCallbackInfo<v8::Value>& args) {
	v8::Isolate* isolate = args.GetIsolate();
	v8::EscapableHandleScope scope(isolate);

	if (args.Length() <= (0) || !args[0]->IsString()) {
		isolate->ThrowException(Exception::TypeError(String::NewFromUtf8(isolate, "Argument 0 must be a string")));
		throw Exception::TypeError(String::NewFromUtf8(isolate, "Argument 0 must be a string"));
	}
	String::Utf8Value js_library(args[0]->ToString());

	bool result = DynLoadODBC(*js_library);
  
	args.GetReturnValue().Set((result) ? True(isolate) : False(isolate));
	//return scope.Escape((result) ? True(isolate) : False(isolate));
}
#endif

extern "C" void init (v8::Handle<Object> target) {
#ifdef dynodbc
	v8::Isolate* isolate = v8::Isolate::GetCurrent();
	target->Set(String::NewFromUtf8(isolate, "loadODBCLibrary"), FunctionTemplate::New(isolate, ODBC::LoadODBCLibrary)->GetFunction());
#endif
  
	ODBC::Init(target);
	ODBCResult::Init(target);
	ODBCConnection::Init(target);
	ODBCStatement::Init(target);
}

NODE_MODULE(odbc_bindings, init)
