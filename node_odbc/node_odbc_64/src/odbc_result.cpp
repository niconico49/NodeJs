/*
  Copyright (c) 2013, Dan VerWeire<dverweire@gmail.com>

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
#include <time.h>
#include <uv.h>

#include "odbc.h"
#include "odbc_connection.h"
#include "odbc_result.h"
#include "odbc_statement.h"

using namespace v8;
using namespace node;

Persistent<FunctionTemplate> ODBCResult::constructor_template;
Persistent<String> ODBCResult::OPTION_FETCH_MODE(v8::Isolate::GetCurrent(), String::NewFromUtf8(v8::Isolate::GetCurrent(), "fetchMode"));

void ODBCResult::Init(v8::Handle<Object> target) {
	DEBUG_PRINTF("ODBCResult::Init\n");
	v8::Isolate* isolate = v8::Isolate::GetCurrent();
	v8::EscapableHandleScope scope(isolate);

	Local<FunctionTemplate> t = FunctionTemplate::New(isolate, ODBCResult::New);

	constructor_template.Reset(isolate, t);
	t->SetClassName(String::NewFromUtf8(isolate, "ODBCResult", String::kInternalizedString));

	// Reserve space for one Handle<Value>
	Local<ObjectTemplate> instance_template = t->InstanceTemplate();
	instance_template->SetInternalFieldCount(1);

	// Prototype Methods
	NODE_SET_PROTOTYPE_METHOD(t, "fetchAll", FetchAll);
	NODE_SET_PROTOTYPE_METHOD(t, "fetch", Fetch);

	NODE_SET_PROTOTYPE_METHOD(t, "moreResultsSync", MoreResultsSync);
	NODE_SET_PROTOTYPE_METHOD(t, "closeSync", CloseSync);
	NODE_SET_PROTOTYPE_METHOD(t, "fetchSync", FetchSync);
	NODE_SET_PROTOTYPE_METHOD(t, "fetchAllSync", FetchAllSync);
	NODE_SET_PROTOTYPE_METHOD(t, "getColumnNamesSync", GetColumnNamesSync);

	// Properties
	instance_template->SetAccessor(String::NewFromUtf8(isolate, "fetchMode"), FetchModeGetter, (AccessorSetterCallback)FetchModeSetter);
  
	// Attach the Database Constructor to the target object
	target->Set(v8::String::NewFromUtf8(isolate, "ODBCResult", String::kInternalizedString), t->GetFunction());
}

ODBCResult::~ODBCResult() {
	DEBUG_PRINTF("ODBCResult::~ODBCResult m_hSTMT=%x\n", m_hSTMT);
	this->Free();
}

void ODBCResult::Free() {
	DEBUG_PRINTF("ODBCResult::Free m_hSTMT=%X m_canFreeHandle=%X\n", m_hSTMT, m_canFreeHandle);
  
	if (m_hSTMT && m_canFreeHandle) {
		uv_mutex_lock(&ODBC::g_odbcMutex);
    
		SQLFreeHandle( SQL_HANDLE_STMT, m_hSTMT);
    
		m_hSTMT = NULL;
  
		uv_mutex_unlock(&ODBC::g_odbcMutex);
	}
  
	if (bufferLength > 0) {
		bufferLength = 0;
		free(buffer);
	}
}

void ODBCResult::New(const v8::FunctionCallbackInfo<v8::Value>& args) {
	DEBUG_PRINTF("ODBCResult::New\n");
  
	v8::Isolate* isolate = args.GetIsolate();
	v8::EscapableHandleScope scope(isolate);

	if (args.Length() <= (0) || !args[0]->IsExternal()) {
		isolate->ThrowException(Exception::TypeError(String::NewFromUtf8(isolate, "Argument 0 invalid")));
		throw Exception::TypeError(String::NewFromUtf8(isolate, "Argument 0 invalid"));
	}
	Local<External> js_henv = Local<External>::Cast(args[0]);

	if (args.Length() <= (1) || !args[1]->IsExternal()) {
		isolate->ThrowException(Exception::TypeError(String::NewFromUtf8(isolate, "Argument 1 invalid")));
		throw Exception::TypeError(String::NewFromUtf8(isolate, "Argument 1 invalid"));
	}
	Local<External> js_hdbc = Local<External>::Cast(args[1]);

	if (args.Length() <= (2) || !args[2]->IsExternal()) {
		isolate->ThrowException(Exception::TypeError(String::NewFromUtf8(isolate, "Argument 2 invalid")));
		throw Exception::TypeError(String::NewFromUtf8(isolate, "Argument 2 invalid"));
	}
	Local<External> js_hstmt = Local<External>::Cast(args[2]);

	if (args.Length() <= (3) || !args[3]->IsExternal()) {
		isolate->ThrowException(Exception::TypeError(String::NewFromUtf8(isolate, "Argument 3 invalid")));
		throw Exception::TypeError(String::NewFromUtf8(isolate, "Argument 3 invalid"));
	}
	Local<External> js_canFreeHandle = Local<External>::Cast(args[3]);

	HENV hENV = static_cast<HENV>(js_henv->Value());
	HDBC hDBC = static_cast<HDBC>(js_hdbc->Value());
	HSTMT hSTMT = static_cast<HSTMT>(js_hstmt->Value());
	bool* canFreeHandle = static_cast<bool *>(js_canFreeHandle->Value());
  
	//create a new OBCResult object
	ODBCResult* objODBCResult = new ODBCResult(hENV, hDBC, hSTMT, *canFreeHandle);
  
	DEBUG_PRINTF("ODBCResult::New m_hDBC=%X m_hDBC=%X m_hSTMT=%X canFreeHandle=%X\n", objODBCResult->m_hENV, objODBCResult->m_hDBC, objODBCResult->m_hSTMT, objODBCResult->m_canFreeHandle);
  
	//free the pointer to canFreeHandle
	delete canFreeHandle;

	//specify the buffer length
	objODBCResult->bufferLength = MAX_VALUE_SIZE - 1;
  
	//initialze a buffer for this object
	objODBCResult->buffer = (uint16_t *) malloc(objODBCResult->bufferLength + 1);
	//TODO: make sure the malloc succeeded

	//set the initial colCount to 0
	objODBCResult->colCount = 0;

	//default fetchMode to FETCH_OBJECT
	objODBCResult->m_fetchMode = FETCH_OBJECT;
  
	objODBCResult->Wrap(args.Holder());
  
	args.GetReturnValue().Set(args.Holder());
}

void ODBCResult::FetchModeGetter(Local<String> property, const PropertyCallbackInfo<Value>& args) {
	v8::Isolate* isolate = args.GetIsolate();
	v8::EscapableHandleScope scope(isolate);

	ODBCResult *obj = ObjectWrap::Unwrap<ODBCResult>(args.Holder());

	args.GetReturnValue().Set(Integer::New(isolate, obj->m_fetchMode));
}

void ODBCResult::FetchModeSetter(Local<String> property, Local<Value> value, const PropertyCallbackInfo<Value>& args) {
	v8::Isolate* isolate = args.GetIsolate();
	v8::EscapableHandleScope scope(isolate);

	ODBCResult *obj = ObjectWrap::Unwrap<ODBCResult>(args.Holder());
  
	if (value->IsNumber()) {
		obj->m_fetchMode = value->Int32Value();
	}
}

/*
 * Fetch
 */
void ODBCResult::Fetch(const v8::FunctionCallbackInfo<v8::Value>& args) {
	DEBUG_PRINTF("ODBCResult::Fetch\n");
  
	v8::Isolate* isolate = args.GetIsolate();
	v8::EscapableHandleScope scope(isolate);

	ODBCResult* objODBCResult = ObjectWrap::Unwrap<ODBCResult>(args.Holder());
  
	uv_work_t* work_req = (uv_work_t *) (calloc(1, sizeof(uv_work_t)));
  
	fetch_work_data* data = (fetch_work_data *) calloc(1, sizeof(fetch_work_data));
  
	Local<Function> cb;
   
	//set the fetch mode to the default of this instance
	data->fetchMode = objODBCResult->m_fetchMode;
  
	if (args.Length() == 1 && args[0]->IsFunction()) {
		cb = Local<Function>::Cast(args[0]);
	}
	else if (args.Length() == 2 && args[0]->IsObject() && args[1]->IsFunction()) {
		cb = Local<Function>::Cast(args[1]);  
    
		Local<Object> obj = args[0]->ToObject();
    
		v8::Local<v8::String> optionFetchMode = v8::Local<v8::String>::New(isolate, OPTION_FETCH_MODE);

		if (obj->Has(optionFetchMode) && obj->Get(optionFetchMode)->IsInt32()) {
			data->fetchMode = obj->Get(optionFetchMode)->ToInt32()->Value();
		}
	}
	else {
		isolate->ThrowException(Exception::TypeError(String::NewFromUtf8(isolate, "ODBCResult::Fetch(): 1 or 2 arguments are required. The last argument must be a callback function.")));
		throw Exception::TypeError(String::NewFromUtf8(isolate, "ODBCResult::Fetch(): 1 or 2 arguments are required. The last argument must be a callback function."));
	}
  
	v8::Persistent<v8::Function> persistent(isolate, cb);
	data->cb = persistent;

	data->objResult = objODBCResult;
	work_req->data = data;
  
	uv_queue_work(uv_default_loop(), work_req, UV_Fetch, (uv_after_work_cb)UV_AfterFetch);

	objODBCResult->Ref();

	args.GetReturnValue().SetUndefined();
}

void ODBCResult::UV_Fetch(uv_work_t* work_req) {
	DEBUG_PRINTF("ODBCResult::UV_Fetch\n");
	fetch_work_data* data = (fetch_work_data *)(work_req->data);
	data->result = SQLFetch(data->objResult->m_hSTMT);
}

void ODBCResult::UV_AfterFetch(uv_work_t* work_req, int status) {
	DEBUG_PRINTF("ODBCResult::UV_AfterFetch\n");
  
	v8::Isolate* isolate = v8::Isolate::GetCurrent();
	v8::EscapableHandleScope scope(isolate);

	fetch_work_data* data = (fetch_work_data *)(work_req->data);
  
	SQLRETURN ret = data->result;
	//TODO: we should probably define this on the work data so we
	//don't have to keep creating it?
	Local<Object> objError;
	bool moreWork = true;
	bool error = false;
  
	if (data->objResult->colCount == 0) {
		data->objResult->columns = ODBC::GetColumns(data->objResult->m_hSTMT, &data->objResult->colCount);
	}
  
	//check to see if the result has no columns
	if (data->objResult->colCount == 0) {
		//this means
		moreWork = false;
	}
	//check to see if there was an error
	else if (ret == SQL_ERROR)  {
		moreWork = false;
		error = true;
		objError = ODBC::GetSQLError(SQL_HANDLE_STMT, data->objResult->m_hSTMT,	(char *) "Error in ODBCResult::UV_AfterFetch");
	}
	//check to see if we are at the end of the recordset
	else if (ret == SQL_NO_DATA) {
		moreWork = false;
	}

	if (moreWork) {
		Handle<Value> args1[2];

		args1[0] = Null(isolate);
		if (data->fetchMode == FETCH_ARRAY) {
			args1[1] = ODBC::GetRecordArray(data->objResult->m_hSTMT, data->objResult->columns, &data->objResult->colCount, data->objResult->buffer, data->objResult->bufferLength);
		}
		else {
			args1[1] = ODBC::GetRecordTuple(data->objResult->m_hSTMT, data->objResult->columns, &data->objResult->colCount, data->objResult->buffer, data->objResult->bufferLength);
		}

		TryCatch try_catch;

		v8::Local<v8::Function> f = v8::Local<v8::Function>::New(isolate, data->cb);
		f->Call(isolate->GetCurrentContext()->Global(), 2, args1);
		data->cb.Reset();

		if (try_catch.HasCaught()) {
			FatalException(try_catch);
		}
	}
	else {
		ODBC::FreeColumns(data->objResult->columns, &data->objResult->colCount);
    
		Handle<Value> args[2];
    
		//if there was an error, pass that as arg[0] otherwise Null
		if (error) {
			args[0] = objError;
		}
		else {
			args[0] = Null(isolate);
		}
    
		args[1] = Null(isolate);

		TryCatch try_catch;

		v8::Local<v8::Function> f = v8::Local<v8::Function>::New(isolate, data->cb);
		f->Call(isolate->GetCurrentContext()->Global(), 2, args);
		data->cb.Reset();

		if (try_catch.HasCaught()) {
			FatalException(try_catch);
		}
	}
  
	data->objResult->Unref();
  
	free(data);
	free(work_req);
  
	return;
}

/*
 * FetchSync
 */

void ODBCResult::FetchSync(const v8::FunctionCallbackInfo<v8::Value>& args) {
	DEBUG_PRINTF("ODBCResult::FetchSync\n");
  
	v8::Isolate* isolate = args.GetIsolate();
	v8::EscapableHandleScope scope(isolate);

	ODBCResult* objResult = ObjectWrap::Unwrap<ODBCResult>(args.Holder());

	Local<Object> objError;
	bool moreWork = true;
	bool error = false;
	int fetchMode = objResult->m_fetchMode;
  
	if (args.Length() == 1 && args[0]->IsObject()) {
		Local<Object> obj = args[0]->ToObject();
    
		v8::Local<v8::String> optionFetchMode = v8::Local<v8::String>::New(isolate, OPTION_FETCH_MODE);
	
		if (obj->Has(optionFetchMode) && obj->Get(optionFetchMode)->IsInt32()) {
			fetchMode = obj->Get(optionFetchMode)->ToInt32()->Value();
		}
	}
  
	SQLRETURN ret = SQLFetch(objResult->m_hSTMT);

	if (objResult->colCount == 0) {
		objResult->columns = ODBC::GetColumns(objResult->m_hSTMT, &objResult->colCount);
	}
  
	//check to see if the result has no columns
	if (objResult->colCount == 0) {
		moreWork = false;
	}
	//check to see if there was an error
	else if (ret == SQL_ERROR)  {
		moreWork = false;
		error = true;
		objError = ODBC::GetSQLError(SQL_HANDLE_STMT, objResult->m_hSTMT, (char *) "Error in ODBCResult::UV_AfterFetch");
	}
	//check to see if we are at the end of the recordset
	else if (ret == SQL_NO_DATA) {
		moreWork = false;
	}

	if (moreWork) {
		Handle<Value> data;
    
		if (fetchMode == FETCH_ARRAY) {
			data = ODBC::GetRecordArray(objResult->m_hSTMT, objResult->columns,	&objResult->colCount, objResult->buffer, objResult->bufferLength);
		}
		else {
			data = ODBC::GetRecordTuple(objResult->m_hSTMT, objResult->columns, &objResult->colCount, objResult->buffer, objResult->bufferLength);
		}
    
		args.GetReturnValue().Set(data);
	}
	else {
		ODBC::FreeColumns(objResult->columns, &objResult->colCount);

		//if there was an error, pass that as arg[0] otherwise Null
		if (error) {
			isolate->ThrowException(objError);
			args.GetReturnValue().SetNull();
			throw objError;
		}
		else {
			args.GetReturnValue().SetNull();
		}
	}
}

/*
 * FetchAll
 */
void ODBCResult::FetchAll(const v8::FunctionCallbackInfo<v8::Value>& args) {
	DEBUG_PRINTF("ODBCResult::FetchAll\n");
  
	v8::Isolate* isolate = args.GetIsolate();
	v8::EscapableHandleScope scope(isolate);

	ODBCResult* objODBCResult = ObjectWrap::Unwrap<ODBCResult>(args.Holder());
  
	uv_work_t* work_req = (uv_work_t *) (calloc(1, sizeof(uv_work_t)));
  
	fetch_work_data* data = (fetch_work_data *) calloc(1, sizeof(fetch_work_data));
  
	Local<Function> cb;
  
	data->fetchMode = objODBCResult->m_fetchMode;
  
	if (args.Length() == 1 && args[0]->IsFunction()) {
		cb = Local<Function>::Cast(args[0]);
	}
	else if (args.Length() == 2 && args[0]->IsObject() && args[1]->IsFunction()) {
		cb = Local<Function>::Cast(args[1]);  
    
		Local<Object> obj = args[0]->ToObject();
    
		v8::Local<v8::String> optionFetchMode = v8::Local<v8::String>::New(isolate, OPTION_FETCH_MODE);
	
		if (obj->Has(optionFetchMode) && obj->Get(optionFetchMode)->IsInt32()) {
			data->fetchMode = obj->Get(optionFetchMode)->ToInt32()->Value();
		}
	}
	else {
		isolate->ThrowException(Exception::TypeError(String::NewFromUtf8(isolate, "ODBCResult::FetchAll(): 1 or 2 arguments are required. The last argument must be a callback function.")));
		throw Exception::TypeError(String::NewFromUtf8(isolate, "ODBCResult::FetchAll(): 1 or 2 arguments are required. The last argument must be a callback function."));
	}
  
	v8::Persistent<v8::Array> persistentRows(isolate, Array::New(isolate));
	data->rows = persistentRows;
	data->errorCount = 0;
	data->count = 0;

	v8::Persistent<v8::Object> persistentError(isolate, Object::New(isolate));
	data->objError = persistentError;

	v8::Persistent<v8::Function> persistent(isolate, cb);
	data->cb = persistent;
	data->objResult = objODBCResult;
  
	work_req->data = data;
  
	uv_queue_work(uv_default_loop(), work_req, UV_FetchAll, (uv_after_work_cb)UV_AfterFetchAll);

	data->objResult->Ref();

	args.GetReturnValue().SetUndefined();
}

void ODBCResult::UV_FetchAll(uv_work_t* work_req) {
	DEBUG_PRINTF("ODBCResult::UV_FetchAll\n");
	fetch_work_data* data = (fetch_work_data *)(work_req->data);
	data->result = SQLFetch(data->objResult->m_hSTMT);
}

void ODBCResult::UV_AfterFetchAll(uv_work_t* work_req, int status) {
	DEBUG_PRINTF("ODBCResult::UV_AfterFetchAll\n");
  
	v8::Isolate* isolate = v8::Isolate::GetCurrent();
	v8::EscapableHandleScope scope(isolate);

	fetch_work_data* data = (fetch_work_data *)(work_req->data);
  
	ODBCResult* self = data->objResult->self();
  
	bool doMoreWork = true;
  
	if (self->colCount == 0) {
		self->columns = ODBC::GetColumns(self->m_hSTMT, &self->colCount);
	}
  
	//check to see if the result set has columns
	if (self->colCount == 0) {
		//this most likely means that the query was something like
		//'insert into ....'
		doMoreWork = false;
	}
	//check to see if there was an error
	else if (data->result == SQL_ERROR)  {
		data->errorCount++;
    
		v8::Persistent<v8::Object> objError(isolate, ODBC::GetSQLError(SQL_HANDLE_STMT, self->m_hSTMT, (char *) "[node-odbc] Error in ODBCResult::UV_AfterFetchAll"));
		data->objError = objError;
    
		doMoreWork = false;
	}
	//check to see if we are at the end of the recordset
	else if (data->result == SQL_NO_DATA) {
		doMoreWork = false;
	}
	else {
		v8::Local<v8::Array> a = v8::Local<v8::Array>::New(isolate, data->rows);
	
		if (data->fetchMode == FETCH_ARRAY) {
			a->Set(Integer::New(isolate, data->count), ODBC::GetRecordArray(self->m_hSTMT, self->columns, &self->colCount, self->buffer, self->bufferLength));
		}
		else {
			a->Set(Integer::New(isolate, data->count), ODBC::GetRecordTuple(self->m_hSTMT, self->columns, &self->colCount, self->buffer, self->bufferLength));
		}
		data->count++;
	}
  
	if (doMoreWork) {
		//Go back to the thread pool and fetch more data!
		uv_queue_work(uv_default_loop(), work_req, UV_FetchAll, (uv_after_work_cb)UV_AfterFetchAll);
	}
	else {
		ODBC::FreeColumns(self->columns, &self->colCount);
    
		Handle<Value> args[2];
    
		if (data->errorCount > 0) {
			args[0] = Local<Object>::New(isolate, data->objError);
		}
		else {
			args[0] = Null(isolate);
		}
    
		args[1] = Local<Array>::New(isolate, data->rows);

		TryCatch try_catch;

		v8::Local<v8::Function> f = v8::Local<v8::Function>::New(isolate, data->cb);
		f->Call(isolate->GetCurrentContext()->Global(), 2, args);
		data->cb.Reset();
		data->rows.Reset();
		data->objError.Reset();

		if (try_catch.HasCaught()) {
			FatalException(try_catch);
		}

		//TODO: Do we need to free self->rows somehow?
		free(data);
		free(work_req);

		self->Unref(); 
	}
}

/*
 * FetchAllSync
 */
void ODBCResult::FetchAllSync(const v8::FunctionCallbackInfo<v8::Value>& args) {
	DEBUG_PRINTF("ODBCResult::FetchAllSync\n");

	v8::Isolate* isolate = args.GetIsolate();
	v8::EscapableHandleScope scope(isolate);

	ODBCResult* self = ObjectWrap::Unwrap<ODBCResult>(args.Holder());
  
	Local<Object> objError = Object::New(isolate);
  
	SQLRETURN ret;
	int count = 0;
	int errorCount = 0;
	int fetchMode = self->m_fetchMode;

	if (args.Length() == 1 && args[0]->IsObject()) {
		Local<Object> obj = args[0]->ToObject();
    
		v8::Local<v8::String> optionFetchMode = v8::Local<v8::String>::New(isolate, OPTION_FETCH_MODE);
	
		if (obj->Has(optionFetchMode) && obj->Get(optionFetchMode)->IsInt32()) {
			fetchMode = obj->Get(optionFetchMode)->ToInt32()->Value();
		}
	}
  
	if (self->colCount == 0) {
		self->columns = ODBC::GetColumns(self->m_hSTMT, &self->colCount);
	}
  
	Local<Array> rows = Array::New(isolate);
  
	//Only loop through the recordset if there are columns
	if (self->colCount > 0) {
		//loop through all records
		while (true) {
			ret = SQLFetch(self->m_hSTMT);
      
			//check to see if there was an error
			if (ret == SQL_ERROR)  {
				errorCount++;
				objError = ODBC::GetSQLError(SQL_HANDLE_STMT, self->m_hSTMT, (char *) "[node-odbc] Error in ODBCResult::UV_AfterFetchAll; probably your query did not have a result set.");
				break;
			}
      
			//check to see if we are at the end of the recordset
			if (ret == SQL_NO_DATA) {
				ODBC::FreeColumns(self->columns, &self->colCount);
				break;
			}

			if (fetchMode == FETCH_ARRAY) {
				rows->Set(Integer::New(isolate, count), ODBC::GetRecordArray(self->m_hSTMT,	self->columns, &self->colCount, self->buffer, self->bufferLength));
			}
			else {
				rows->Set(Integer::New(isolate, count), ODBC::GetRecordTuple(self->m_hSTMT, self->columns, &self->colCount, self->buffer, self->bufferLength));
			}
			count++;
		}
	}
	else {
		ODBC::FreeColumns(self->columns, &self->colCount);
	}
  
	//throw the error object if there were errors
	if (errorCount > 0) {
		isolate->ThrowException(objError);
		throw objError;
	}
  
	args.GetReturnValue().Set(rows);
}

/*
 * CloseSync
 * 
 */
void ODBCResult::CloseSync(const v8::FunctionCallbackInfo<v8::Value>& args) {
	DEBUG_PRINTF("ODBCResult::CloseSync\n");
  
	v8::Isolate* isolate = args.GetIsolate();
	v8::EscapableHandleScope scope(isolate);

	int closeOption;
	if (args.Length() <= (0)) {
		closeOption = (SQL_DESTROY);
	}
	else if (args[0]->IsInt32()) {
		closeOption = args[0]->Int32Value();
	}
	else {
		isolate->ThrowException(Exception::TypeError(String::NewFromUtf8(isolate, "Argument 0 must be an integer")));
		throw Exception::TypeError(String::NewFromUtf8(isolate, "Argument 0 must be an integer"));
	}

	ODBCResult* result = ObjectWrap::Unwrap<ODBCResult>(args.Holder());
 
	DEBUG_PRINTF("ODBCResult::CloseSync closeOption=%i m_canFreeHandle=%i\n", closeOption, result->m_canFreeHandle);
  
	if (closeOption == SQL_DESTROY && result->m_canFreeHandle) {
		result->Free();
	}
	else if (closeOption == SQL_DESTROY && !result->m_canFreeHandle) {
		//We technically can't free the handle so, we'll SQL_CLOSE
		uv_mutex_lock(&ODBC::g_odbcMutex);
    
		SQLFreeStmt(result->m_hSTMT, SQL_CLOSE);
  
		uv_mutex_unlock(&ODBC::g_odbcMutex);
	}
	else {
		uv_mutex_lock(&ODBC::g_odbcMutex);
    
		SQLFreeStmt(result->m_hSTMT, closeOption);
  
		uv_mutex_unlock(&ODBC::g_odbcMutex);
	}
  
	args.GetReturnValue().Set(True(isolate));
}

void ODBCResult::MoreResultsSync(const v8::FunctionCallbackInfo<v8::Value>& args) {
	DEBUG_PRINTF("ODBCResult::MoreResultsSync\n");
  
	v8::Isolate* isolate = args.GetIsolate();
	v8::EscapableHandleScope scope(isolate);

	ODBCResult* result = ObjectWrap::Unwrap<ODBCResult>(args.Holder());
  
	SQLRETURN ret = SQLMoreResults(result->m_hSTMT);

	if (ret == SQL_ERROR) {
		isolate->ThrowException(ODBC::GetSQLError(SQL_HANDLE_STMT, result->m_hSTMT, (char *)"[node-odbc] Error in ODBCResult::MoreResultsSync"));
		throw ODBC::GetSQLError(SQL_HANDLE_STMT, result->m_hSTMT, (char *)"[node-odbc] Error in ODBCResult::MoreResultsSync");
	}

	args.GetReturnValue().Set(SQL_SUCCEEDED(ret) || ret == SQL_ERROR ? True(isolate) : False(isolate));
}

/*
 * GetColumnNamesSync
 */
void ODBCResult::GetColumnNamesSync(const v8::FunctionCallbackInfo<v8::Value>& args) {
	DEBUG_PRINTF("ODBCResult::GetColumnNamesSync\n");

	v8::Isolate* isolate = args.GetIsolate();
	v8::EscapableHandleScope scope(isolate);

	ODBCResult* self = ObjectWrap::Unwrap<ODBCResult>(args.Holder());
  
	Local<Array> cols = Array::New(isolate);
  
	if (self->colCount == 0) {
		self->columns = ODBC::GetColumns(self->m_hSTMT, &self->colCount);
	}
  
	//Local<Object> cols = Object::New(isolate);
	
	for (int i = 0; i < self->colCount; i++) {
		//cols->Set(Integer::New(isolate, i), nameType);
		//cols->Set(Integer::New(isolate, i), String::NewFromTwoByte(isolate, (const uint16_t *)self->columns[i].type));
		//cols->Set(Integer::New(isolate, i), String::NewFromTwoByte(isolate, (const uint16_t *)self->columns[i].name));
		//cols->Set(Integer::New(isolate, i), Integer::New(isolate, self->columns[i].type));
		Local<Object> nameType = Object::New(isolate);
		nameType->Set(String::NewFromUtf8(isolate, "NAME"), String::NewFromTwoByte(isolate, (uint16_t *)self->columns[i].name));
		nameType->Set(String::NewFromUtf8(isolate, "TYPE"), Integer::New(isolate, self->columns[i].type));
		cols->Set(Integer::New(isolate, i), nameType);
		//cols->Set(String::NewFromTwoByte(isolate, (uint16_t *)self->columns[i].name), Integer::New(isolate, self->columns[i].type));
	}
   
	args.GetReturnValue().Set(cols);
}
