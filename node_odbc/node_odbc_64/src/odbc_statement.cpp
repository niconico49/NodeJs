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

Persistent<FunctionTemplate> ODBCStatement::constructor_template;

void ODBCStatement::Init(v8::Handle<Object> target) {
	DEBUG_PRINTF("ODBCStatement::Init\n");
	v8::Isolate* isolate = v8::Isolate::GetCurrent();
	v8::EscapableHandleScope scope(isolate);

	Local<FunctionTemplate> t = FunctionTemplate::New(isolate, ODBCStatement::New);

	constructor_template.Reset(isolate, t);
	t->SetClassName(String::NewFromUtf8(isolate, "ODBCStatement", String::kInternalizedString));

	// Reserve space for one Handle<Value>
	Local<ObjectTemplate> instance_template = t->InstanceTemplate();
	instance_template->SetInternalFieldCount(1);
  
	// Prototype Methods
	NODE_SET_PROTOTYPE_METHOD(t, "execute", Execute);
	NODE_SET_PROTOTYPE_METHOD(t, "executeSync", ExecuteSync);
  
	NODE_SET_PROTOTYPE_METHOD(t, "executeDirect", ExecuteDirect);
	NODE_SET_PROTOTYPE_METHOD(t, "executeDirectSync", ExecuteDirectSync);
  
	NODE_SET_PROTOTYPE_METHOD(t, "executeNonQuery", ExecuteNonQuery);
	NODE_SET_PROTOTYPE_METHOD(t, "executeNonQuerySync", ExecuteNonQuerySync);
  
	NODE_SET_PROTOTYPE_METHOD(t, "prepare", Prepare);
	NODE_SET_PROTOTYPE_METHOD(t, "prepareSync", PrepareSync);
  
	NODE_SET_PROTOTYPE_METHOD(t, "bind", Bind);
	NODE_SET_PROTOTYPE_METHOD(t, "bindSync", BindSync);
  
	NODE_SET_PROTOTYPE_METHOD(t, "closeSync", CloseSync);

	// Attach the Database Constructor to the target object
	target->Set(v8::String::NewFromUtf8(isolate, "ODBCStatement", String::kInternalizedString), t->GetFunction());
}

ODBCStatement::~ODBCStatement() {
	this->Free();
}

void ODBCStatement::Free() {
	DEBUG_PRINTF("ODBCStatement::Free\n");
	//if we previously had parameters, then be sure to free them
	if (paramCount) {
		int count = paramCount;
		paramCount = 0;
    
		Parameter prm;
    
		//free parameter memory
		for (int i = 0; i < count; i++) {
			if (prm = params[i], prm.ParameterValuePtr != NULL) {
				switch (prm.ValueType) {
					case SQL_C_WCHAR:
						free(prm.ParameterValuePtr);
						break;
					case SQL_C_CHAR:
						free(prm.ParameterValuePtr);
						break; 
					case SQL_C_SBIGINT:
						delete (int64_t *)prm.ParameterValuePtr;
						break;
					case SQL_C_DOUBLE:
						delete (double  *)prm.ParameterValuePtr;
						break;
					case SQL_C_BIT: 
						delete (bool    *)prm.ParameterValuePtr;
						break;
				}
			}
		}

		free(params);
	}
  
	if (m_hSTMT) {
		uv_mutex_lock(&ODBC::g_odbcMutex);
    
		SQLFreeHandle(SQL_HANDLE_STMT, m_hSTMT);
		m_hSTMT = NULL;
    
		uv_mutex_unlock(&ODBC::g_odbcMutex);
    
		if (bufferLength > 0) {
			free(buffer);
		}
	}
}

void ODBCStatement::New(const v8::FunctionCallbackInfo<v8::Value>& args) {
	DEBUG_PRINTF("ODBCStatement::New\n");
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

	HENV hENV = static_cast<HENV>(js_henv->Value());
	HDBC hDBC = static_cast<HDBC>(js_hdbc->Value());
	HSTMT hSTMT = static_cast<HSTMT>(js_hstmt->Value());
  
	//create a new OBCResult object
	ODBCStatement* stmt = new ODBCStatement(hENV, hDBC, hSTMT);
  
	//specify the buffer length
	stmt->bufferLength = MAX_VALUE_SIZE - 1;
  
	//initialze a buffer for this object
	stmt->buffer = (uint16_t *) malloc(stmt->bufferLength + 1);
	//TODO: make sure the malloc succeeded

	//set the initial colCount to 0
	stmt->colCount = 0;
  
	//initialize the paramCount
	stmt->paramCount = 0;
  
	stmt->Wrap(args.Holder());
  
	args.GetReturnValue().Set(args.Holder());
}

/*
 * Execute
 */
void ODBCStatement::Execute(const v8::FunctionCallbackInfo<v8::Value>& args) {
	DEBUG_PRINTF("ODBCStatement::Execute\n");
  
	v8::Isolate* isolate = args.GetIsolate();
	v8::EscapableHandleScope scope(isolate);

	if (args.Length() <= (0) || !args[0]->IsFunction()) {
		isolate->ThrowException(Exception::TypeError(String::NewFromUtf8(isolate, "Argument 0 must be a function")));
		throw Exception::TypeError(String::NewFromUtf8(isolate, "Argument 0 must be a function"));
	}
	Local<Function> cb = Local<Function>::Cast(args[0]);

	ODBCStatement* stmt = ObjectWrap::Unwrap<ODBCStatement>(args.Holder());
  
	uv_work_t* work_req = (uv_work_t *) (calloc(1, sizeof(uv_work_t)));
  
	execute_work_data* data = (execute_work_data *) calloc(1, sizeof(execute_work_data));

	v8::Persistent<v8::Function, CopyablePersistentTraits<v8::Function>> persistent(isolate, cb);
	data->cb = persistent;

	data->stmt = stmt;
	work_req->data = data;
  
	uv_queue_work(uv_default_loop(), work_req, UV_Execute, (uv_after_work_cb)UV_AfterExecute);

	stmt->Ref();

	args.GetReturnValue().SetUndefined();
}

void ODBCStatement::UV_Execute(uv_work_t* req) {
	DEBUG_PRINTF("ODBCStatement::UV_Execute\n");
  
	execute_work_data* data = (execute_work_data *)(req->data);

	SQLRETURN ret;
  
	ret = SQLExecute(data->stmt->m_hSTMT); 

	data->result = ret;
}

void ODBCStatement::UV_AfterExecute(uv_work_t* req, int status) {
	DEBUG_PRINTF("ODBCStatement::UV_AfterExecute\n");
  
	execute_work_data* data = (execute_work_data *)(req->data);
  
	v8::Isolate* isolate = v8::Isolate::GetCurrent();
	v8::EscapableHandleScope scope(isolate);

	//an easy reference to the statment object
	ODBCStatement* self = data->stmt->self();

	//First thing, let's check if the execution of the query returned any errors 
	if(data->result == SQL_ERROR) {
		ODBC::CallbackSQLError(SQL_HANDLE_STMT, self->m_hSTMT, data->cb);
	}
	else {
		Local<Value> args[4];
		bool* canFreeHandle = new bool(false);
    
		args[0] = External::New(isolate, self->m_hENV);
		args[1] = External::New(isolate, self->m_hDBC);
		args[2] = External::New(isolate, self->m_hSTMT);
		args[3] = External::New(isolate, canFreeHandle);
    
		v8::Local<v8::FunctionTemplate> ft = v8::Local<v8::FunctionTemplate>::New(isolate, ODBCResult::constructor_template);
		Local<Object> js_result = ft->GetFunction()->NewInstance(4, args);

		args[0] = Local<Value>::New(isolate, Null(isolate));
		args[1] = Local<Object>::New(isolate, js_result);

		TryCatch try_catch;

		v8::Local<v8::Function> f = v8::Local<v8::Function>::New(isolate, data->cb);
		f->Call(isolate->GetCurrentContext()->Global(), 2, args);

		if (try_catch.HasCaught()) {
			FatalException(try_catch);
		}
	}

	self->Unref();
	data->cb.Reset();
  
	free(data);
	free(req);
}

/*
 * ExecuteSync
 * 
 */
void ODBCStatement::ExecuteSync(const v8::FunctionCallbackInfo<v8::Value>& args) {
	DEBUG_PRINTF("ODBCStatement::ExecuteSync\n");
  
	v8::Isolate* isolate = args.GetIsolate();
	v8::EscapableHandleScope scope(isolate);

	ODBCStatement* stmt = ObjectWrap::Unwrap<ODBCStatement>(args.Holder());

	SQLRETURN ret = SQLExecute(stmt->m_hSTMT); 
  
	if(ret == SQL_ERROR) {
		isolate->ThrowException(ODBC::GetSQLError(SQL_HANDLE_STMT, stmt->m_hSTMT, (char *) "[node-odbc] Error in ODBCStatement::ExecuteSync"));
		args.GetReturnValue().SetNull();
		throw ODBC::GetSQLError(SQL_HANDLE_STMT, stmt->m_hSTMT, (char *) "[node-odbc] Error in ODBCStatement::ExecuteSync");
	}
	else {
		Local<Value> args1[4];
		bool* canFreeHandle = new bool(false);
    
		args1[0] = External::New(isolate, stmt->m_hENV);
		args1[1] = External::New(isolate, stmt->m_hDBC);
		args1[2] = External::New(isolate, stmt->m_hSTMT);
		args1[3] = External::New(isolate, canFreeHandle);
    
		v8::Local<v8::FunctionTemplate> ft = v8::Local<v8::FunctionTemplate>::New(isolate, ODBCResult::constructor_template);
		Local<Object> js_result = ft->GetFunction()->NewInstance(4, args1);
    
		args.GetReturnValue().Set(js_result);
	}
}

/*
 * ExecuteNonQuery
 */
void ODBCStatement::ExecuteNonQuery(const v8::FunctionCallbackInfo<v8::Value>& args) {
	DEBUG_PRINTF("ODBCStatement::ExecuteNonQuery\n");
  
	v8::Isolate* isolate = args.GetIsolate();
	v8::EscapableHandleScope scope(isolate);

	if (args.Length() <= (0) || !args[0]->IsFunction()) {
		isolate->ThrowException(Exception::TypeError(String::NewFromUtf8(isolate, "Argument 0 must be a function")));
		throw Exception::TypeError(String::NewFromUtf8(isolate, "Argument 0 must be a function"));
	}
	Local<Function> cb = Local<Function>::Cast(args[0]);

	ODBCStatement* stmt = ObjectWrap::Unwrap<ODBCStatement>(args.Holder());
  
	uv_work_t* work_req = (uv_work_t *) (calloc(1, sizeof(uv_work_t)));
  
	execute_work_data* data = (execute_work_data *) calloc(1, sizeof(execute_work_data));

	v8::Persistent<v8::Function, CopyablePersistentTraits<v8::Function>> persistent(isolate, cb);
	data->cb = persistent;

	data->stmt = stmt;
	work_req->data = data;
  
	uv_queue_work(uv_default_loop(), work_req, UV_ExecuteNonQuery, (uv_after_work_cb)UV_AfterExecuteNonQuery);

	stmt->Ref();

	args.GetReturnValue().SetUndefined();
}

void ODBCStatement::UV_ExecuteNonQuery(uv_work_t* req) {
	DEBUG_PRINTF("ODBCStatement::ExecuteNonQuery\n");
  
	execute_work_data* data = (execute_work_data *)(req->data);

	SQLRETURN ret;
  
	ret = SQLExecute(data->stmt->m_hSTMT); 

	data->result = ret;
}

void ODBCStatement::UV_AfterExecuteNonQuery(uv_work_t* req, int status) {
	DEBUG_PRINTF("ODBCStatement::ExecuteNonQuery\n");
  
	execute_work_data* data = (execute_work_data *)(req->data);
  
	v8::Isolate* isolate = v8::Isolate::GetCurrent();
	v8::EscapableHandleScope scope(isolate);

	//an easy reference to the statment object
	ODBCStatement* self = data->stmt->self();

	//First thing, let's check if the execution of the query returned any errors 
	if(data->result == SQL_ERROR) {
		ODBC::CallbackSQLError(SQL_HANDLE_STMT, self->m_hSTMT, data->cb);
	}
	else {
		SQLLEN rowCount = 0;
    
		SQLRETURN ret = SQLRowCount(self->m_hSTMT, &rowCount);
    
		if (!SQL_SUCCEEDED(ret)) {
			rowCount = 0;
		}
    
		uv_mutex_lock(&ODBC::g_odbcMutex);
		SQLFreeStmt(self->m_hSTMT, SQL_CLOSE);
		uv_mutex_unlock(&ODBC::g_odbcMutex);
    
		Local<Value> args[2];

		args[0] = Local<Value>::New(isolate, Null(isolate));
		args[1] = Local<Value>::New(isolate, Number::New(isolate, rowCount));

		TryCatch try_catch;

		v8::Local<v8::Function> f = v8::Local<v8::Function>::New(isolate, data->cb);
		f->Call(isolate->GetCurrentContext()->Global(), 2, args);

		if (try_catch.HasCaught()) {
			FatalException(try_catch);
		}
	}

	self->Unref();
	data->cb.Reset();
  
	free(data);
	free(req);
}

/*
 * ExecuteNonQuerySync
 * 
 */
void ODBCStatement::ExecuteNonQuerySync(const v8::FunctionCallbackInfo<v8::Value>& args) {
	DEBUG_PRINTF("ODBCStatement::ExecuteNonQuerySync\n");
  
	v8::Isolate* isolate = args.GetIsolate();
	v8::EscapableHandleScope scope(isolate);

	ODBCStatement* stmt = ObjectWrap::Unwrap<ODBCStatement>(args.Holder());

	SQLRETURN ret = SQLExecute(stmt->m_hSTMT); 
  
	if(ret == SQL_ERROR) {
		isolate->ThrowException(ODBC::GetSQLError(SQL_HANDLE_STMT, stmt->m_hSTMT, (char *) "[node-odbc] Error in ODBCStatement::ExecuteSync"));
		args.GetReturnValue().SetNull();
		throw ODBC::GetSQLError(SQL_HANDLE_STMT, stmt->m_hSTMT, (char *) "[node-odbc] Error in ODBCStatement::ExecuteSync");
	}
	else {
		SQLLEN rowCount = 0;
    
		SQLRETURN ret = SQLRowCount(stmt->m_hSTMT, &rowCount);
    
		if (!SQL_SUCCEEDED(ret)) {
			rowCount = 0;
		}
    
		uv_mutex_lock(&ODBC::g_odbcMutex);
		SQLFreeStmt(stmt->m_hSTMT, SQL_CLOSE);
		uv_mutex_unlock(&ODBC::g_odbcMutex);
    
		args.GetReturnValue().Set(Number::New(isolate, rowCount));
	}
}

/*
 * ExecuteDirect
 * 
 */
void ODBCStatement::ExecuteDirect(const v8::FunctionCallbackInfo<v8::Value>& args) {
	DEBUG_PRINTF("ODBCStatement::ExecuteDirect\n");
  
	v8::Isolate* isolate = args.GetIsolate();
	v8::EscapableHandleScope scope(isolate);

	if (args.Length() <= (0) || !args[0]->IsString()) {
		isolate->ThrowException(Exception::TypeError(String::NewFromUtf8(isolate, "Argument 0 must be a string")));
		throw Exception::TypeError(String::NewFromUtf8(isolate, "Argument 0 must be a string"));
	}
	Local<String> sql(args[0]->ToString());

	if (args.Length() <= (1) || !args[1]->IsFunction()) {
		isolate->ThrowException(Exception::TypeError(String::NewFromUtf8(isolate, "Argument 1 must be a function")));
		throw Exception::TypeError(String::NewFromUtf8(isolate, "Argument 1 must be a function"));
	}
	Local<Function> cb = Local<Function>::Cast(args[1]);

	ODBCStatement* stmt = ObjectWrap::Unwrap<ODBCStatement>(args.Holder());
  
	uv_work_t* work_req = (uv_work_t *) (calloc(1, sizeof(uv_work_t)));
  
	execute_direct_work_data* data = (execute_direct_work_data *) calloc(1, sizeof(execute_direct_work_data));

	v8::Persistent<v8::Function, CopyablePersistentTraits<v8::Function>> persistent(isolate, cb);
	data->cb = persistent;

	data->sqlLen = sql->Length();

#ifdef UNICODE
	data->sql = (uint16_t *) malloc((data->sqlLen * sizeof(uint16_t)) + sizeof(uint16_t));
	sql->Write((uint16_t *) data->sql);
#else
	data->sql = (char *) malloc(data->sqlLen +1);
	sql->WriteUtf8((char *) data->sql);
#endif

	data->stmt = stmt;
	work_req->data = data;
  
	uv_queue_work(uv_default_loop(), work_req, UV_ExecuteDirect, (uv_after_work_cb)UV_AfterExecuteDirect);

	stmt->Ref();

	args.GetReturnValue().SetUndefined();
}

void ODBCStatement::UV_ExecuteDirect(uv_work_t* req) {
	DEBUG_PRINTF("ODBCStatement::UV_ExecuteDirect\n");
  
	execute_direct_work_data* data = (execute_direct_work_data *)(req->data);

	SQLRETURN ret;
  
	ret = SQLExecDirect(data->stmt->m_hSTMT, (SQLTCHAR *) data->sql, data->sqlLen);  

	data->result = ret;
}

void ODBCStatement::UV_AfterExecuteDirect(uv_work_t* req, int status) {
	DEBUG_PRINTF("ODBCStatement::UV_AfterExecuteDirect\n");
  
	execute_direct_work_data* data = (execute_direct_work_data *)(req->data);
  
	v8::Isolate* isolate = v8::Isolate::GetCurrent();
	v8::EscapableHandleScope scope(isolate);

	//an easy reference to the statment object
	ODBCStatement* self = data->stmt->self();

	//First thing, let's check if the execution of the query returned any errors 
	if(data->result == SQL_ERROR) {
		ODBC::CallbackSQLError(SQL_HANDLE_STMT, self->m_hSTMT, data->cb);
	}
	else {
		Local<Value> args[4];
		bool* canFreeHandle = new bool(false);
    
		args[0] = External::New(isolate, self->m_hENV);
		args[1] = External::New(isolate, self->m_hDBC);
		args[2] = External::New(isolate, self->m_hSTMT);
		args[3] = External::New(isolate, canFreeHandle);
    
		v8::Local<v8::FunctionTemplate> ft = v8::Local<v8::FunctionTemplate>::New(isolate, ODBCResult::constructor_template);
		Local<Object> js_result = ft->GetFunction()->NewInstance(4, args);

		args[0] = Local<Value>::New(isolate, Null(isolate));
		args[1] = Local<Object>::New(isolate, js_result);

		TryCatch try_catch;

		v8::Local<v8::Function> f = v8::Local<v8::Function>::New(isolate, data->cb);
		f->Call(isolate->GetCurrentContext()->Global(), 2, args);

		if (try_catch.HasCaught()) {
			FatalException(try_catch);
		}
	}

	self->Unref();
	data->cb.Reset();
  
	free(data->sql);
	free(data);
	free(req);
}

/*
 * ExecuteDirectSync
 * 
 */
void ODBCStatement::ExecuteDirectSync(const v8::FunctionCallbackInfo<v8::Value>& args) {
	DEBUG_PRINTF("ODBCStatement::ExecuteDirectSync\n");
  
	v8::Isolate* isolate = args.GetIsolate();
	v8::EscapableHandleScope scope(isolate);

#ifdef UNICODE
	if (args.Length() <= (0) || !args[0]->IsString()) {
		isolate->ThrowException(Exception::TypeError(String::NewFromUtf8(isolate, "Argument 0 must be a string")));
		throw Exception::TypeError(String::NewFromUtf8(isolate, "Argument 0 must be a string"));
	}
	String::Value sql(args[0]->ToString());

#else
	REQ_STR_ARG(0, sql);
#endif

	ODBCStatement* stmt = ObjectWrap::Unwrap<ODBCStatement>(args.Holder());
  
	SQLRETURN ret = SQLExecDirect(stmt->m_hSTMT, (SQLTCHAR *) *sql, sql.length());  

	if(ret == SQL_ERROR) {
		isolate->ThrowException(ODBC::GetSQLError(SQL_HANDLE_STMT, stmt->m_hSTMT, (char *) "[node-odbc] Error in ODBCStatement::ExecuteDirectSync"));
		args.GetReturnValue().SetNull();
		throw ODBC::GetSQLError(SQL_HANDLE_STMT, stmt->m_hSTMT, (char *) "[node-odbc] Error in ODBCStatement::ExecuteDirectSync");
	}
	else {
		Local<Value> args1[4];
		bool* canFreeHandle = new bool(false);
    
		args1[0] = External::New(isolate, stmt->m_hENV);
		args1[1] = External::New(isolate, stmt->m_hDBC);
		args1[2] = External::New(isolate, stmt->m_hSTMT);
		args1[3] = External::New(isolate, canFreeHandle);
    
		v8::Local<v8::FunctionTemplate> ft = v8::Local<v8::FunctionTemplate>::New(isolate, ODBCResult::constructor_template);
		Local<Object> js_result = ft->GetFunction()->NewInstance(4, args1);
	
		args.GetReturnValue().Set(js_result);
	}
}

/*
 * PrepareSync
 * 
 */
void ODBCStatement::PrepareSync(const v8::FunctionCallbackInfo<v8::Value>& args) {
	DEBUG_PRINTF("ODBCStatement::PrepareSync\n");
  
	v8::Isolate* isolate = args.GetIsolate();
	v8::EscapableHandleScope scope(isolate);

	if (args.Length() <= (0) || !args[0]->IsString()) {
		isolate->ThrowException(Exception::TypeError(String::NewFromUtf8(isolate, "Argument 0 must be a string")));
		throw Exception::TypeError(String::NewFromUtf8(isolate, "Argument 0 must be a string"));
	}
	Local<String> sql(args[0]->ToString());

	ODBCStatement* stmt = ObjectWrap::Unwrap<ODBCStatement>(args.Holder());

	SQLRETURN ret;

	int sqlLen = sql->Length() + 1;

#ifdef UNICODE
	uint16_t *sql2;
	sql2 = (uint16_t *) malloc(sqlLen * sizeof(uint16_t));
	sql->Write(sql2);
#else
	char *sql2;
	sql2 = (char *) malloc(sqlLen);
	sql->WriteUtf8(sql2);
#endif
  
	ret = SQLPrepare(stmt->m_hSTMT, (SQLTCHAR *) sql2, sqlLen);
  
	if (SQL_SUCCEEDED(ret)) {
		args.GetReturnValue().Set(True(isolate));
	}
	else {
		isolate->ThrowException(ODBC::GetSQLError(SQL_HANDLE_STMT, stmt->m_hSTMT, (char *) "[node-odbc] Error in ODBCStatement::PrepareSync"));
		args.GetReturnValue().Set(False(isolate));
		throw ODBC::GetSQLError(SQL_HANDLE_STMT, stmt->m_hSTMT, (char *) "[node-odbc] Error in ODBCStatement::PrepareSync");
	}
}

/*
 * Prepare
 * 
 */
void ODBCStatement::Prepare(const v8::FunctionCallbackInfo<v8::Value>& args) {
	DEBUG_PRINTF("ODBCStatement::Prepare\n");
  
	v8::Isolate* isolate = args.GetIsolate();
	v8::EscapableHandleScope scope(isolate);

	if (args.Length() <= (0) || !args[0]->IsString()) {
		isolate->ThrowException(Exception::TypeError(String::NewFromUtf8(isolate, "Argument 0 must be a string")));
		throw Exception::TypeError(String::NewFromUtf8(isolate, "Argument 0 must be a string"));
	}
	Local<String> sql(args[0]->ToString());

	if (args.Length() <= (1) || !args[1]->IsFunction()) {
		isolate->ThrowException(Exception::TypeError(String::NewFromUtf8(isolate, "Argument 1 must be a function")));
		throw Exception::TypeError(String::NewFromUtf8(isolate, "Argument 1 must be a function"));
	}
	Local<Function> cb = Local<Function>::Cast(args[1]);

	ODBCStatement* stmt = ObjectWrap::Unwrap<ODBCStatement>(args.Holder());
  
	uv_work_t* work_req = (uv_work_t *) (calloc(1, sizeof(uv_work_t)));
  
	prepare_work_data* data = (prepare_work_data *) calloc(1, sizeof(prepare_work_data));

	v8::Persistent<v8::Function, CopyablePersistentTraits<v8::Function>> persistent(isolate, cb);
	data->cb = persistent;

	data->sqlLen = sql->Length();

#ifdef UNICODE
	data->sql = (uint16_t *) malloc((data->sqlLen * sizeof(uint16_t)) + sizeof(uint16_t));
	sql->Write((uint16_t *) data->sql);
#else
	data->sql = (char *) malloc(data->sqlLen +1);
	sql->WriteUtf8((char *) data->sql);
#endif
  
	data->stmt = stmt;
  
	work_req->data = data;
  
	uv_queue_work(uv_default_loop(), work_req, UV_Prepare, (uv_after_work_cb)UV_AfterPrepare);

	stmt->Ref();

	args.GetReturnValue().SetUndefined();
}

void ODBCStatement::UV_Prepare(uv_work_t* req) {
	DEBUG_PRINTF("ODBCStatement::UV_Prepare\n");
  
	prepare_work_data* data = (prepare_work_data *)(req->data);

	DEBUG_PRINTF("ODBCStatement::UV_Prepare m_hDBC=%X m_hDBC=%X m_hSTMT=%X\n", data->stmt->m_hENV, data->stmt->m_hDBC, data->stmt->m_hSTMT);
  
	SQLRETURN ret;
  
	ret = SQLPrepare(data->stmt->m_hSTMT, (SQLTCHAR *) data->sql, data->sqlLen);

	data->result = ret;
}

void ODBCStatement::UV_AfterPrepare(uv_work_t* req, int status) {
	DEBUG_PRINTF("ODBCStatement::UV_AfterPrepare\n");
  
	prepare_work_data* data = (prepare_work_data *)(req->data);
  
	DEBUG_PRINTF("ODBCStatement::UV_AfterPrepare m_hDBC=%X m_hDBC=%X m_hSTMT=%X\n",	data->stmt->m_hENV,	data->stmt->m_hDBC,	data->stmt->m_hSTMT);
  
	v8::Isolate* isolate = v8::Isolate::GetCurrent();
	v8::EscapableHandleScope scope(isolate);

	//First thing, let's check if the execution of the query returned any errors 
	if(data->result == SQL_ERROR) {
		ODBC::CallbackSQLError(SQL_HANDLE_STMT, data->stmt->m_hSTMT, data->cb);
	}
	else {
		Local<Value> args[2];

		args[0] = Local<Value>::New(isolate, Null(isolate));
		args[1] = Local<Value>::New(isolate, True(isolate));

		TryCatch try_catch;

		v8::Local<v8::Function> f = v8::Local<v8::Function>::New(isolate, data->cb);
		f->Call(isolate->GetCurrentContext()->Global(), 2, args);

		if (try_catch.HasCaught()) {
			FatalException(try_catch);
		}
	}
  
	data->stmt->Unref();
	data->cb.Reset();
  
	free(data->sql);
	free(data);
	free(req);
}

/*
 * BindSync
 * 
 */
void ODBCStatement::BindSync(const v8::FunctionCallbackInfo<v8::Value>& args) {
	DEBUG_PRINTF("ODBCStatement::BindSync\n");
  
	v8::Isolate* isolate = args.GetIsolate();
	v8::EscapableHandleScope scope(isolate);

	if ( !args[0]->IsArray() ) {
		isolate->ThrowException(Exception::TypeError(String::NewFromUtf8(isolate, "Argument 1 must be an Array")));
		throw Exception::TypeError(String::NewFromUtf8(isolate, "Argument 0 must be an Array"));
	}

	ODBCStatement* stmt = ObjectWrap::Unwrap<ODBCStatement>(args.Holder());
  
	DEBUG_PRINTF("ODBCStatement::BindSync m_hDBC=%X m_hDBC=%X m_hSTMT=%X\n", stmt->m_hENV, stmt->m_hDBC, stmt->m_hSTMT);
  
	//if we previously had parameters, then be sure to free them
	//before allocating more
	if (stmt->paramCount) {
		int count = stmt->paramCount;
		stmt->paramCount = 0;
    
		Parameter prm;
    
		//free parameter memory
		for (int i = 0; i < count; i++) {
			if (prm = stmt->params[i], prm.ParameterValuePtr != NULL) {
				switch (prm.ValueType) {
					case SQL_C_WCHAR:
						free(prm.ParameterValuePtr);
						break;
					case SQL_C_CHAR:
						free(prm.ParameterValuePtr);
						break; 
					case SQL_C_SBIGINT:
						delete (int64_t *)prm.ParameterValuePtr;
						break;
					case SQL_C_DOUBLE:
						delete (double *)prm.ParameterValuePtr;
						break;
					case SQL_C_BIT:
						delete (bool *)prm.ParameterValuePtr;
						break;
				}
			}
		}

		free(stmt->params);
	}
  
	stmt->params = ODBC::GetParametersFromArray(Local<Array>::Cast(args[0]), &stmt->paramCount);
  
	SQLRETURN ret = SQL_SUCCESS;
	Parameter prm;
  
	for (int i = 0; i < stmt->paramCount; i++) {
		prm = stmt->params[i];
    
		DEBUG_PRINTF("ODBCStatement::BindSync - param[%i]: c_type=%i type=%i buffer_length=%i size=%i length=%i &length=%X decimals=%i value=%s\n", i, prm.ValueType, prm.ParameterType, prm.BufferLength, prm.ColumnSize, prm.StrLen_or_IndPtr, &stmt->params[i].StrLen_or_IndPtr, prm.DecimalDigits, prm.ParameterValuePtr);
		
		//StatementHandle, ParameterNumber, InputOutputType
		ret = SQLBindParameter(stmt->m_hSTMT, i + 1, SQL_PARAM_INPUT, prm.ValueType, prm.ParameterType,	prm.ColumnSize, prm.DecimalDigits, prm.ParameterValuePtr, prm.BufferLength, &stmt->params[i].StrLen_or_IndPtr);

		if (ret == SQL_ERROR) {
			break;
		}
	}

	if (SQL_SUCCEEDED(ret)) {
		args.GetReturnValue().Set(True(isolate));
	}
	else {
		isolate->ThrowException(ODBC::GetSQLError(SQL_HANDLE_STMT, stmt->m_hSTMT, (char *) "[node-odbc] Error in ODBCStatement::BindSync"));
		args.GetReturnValue().Set(False(isolate));
		throw ODBC::GetSQLError(SQL_HANDLE_STMT, stmt->m_hSTMT, (char *) "[node-odbc] Error in ODBCStatement::BindSync");
	}

	args.GetReturnValue().SetUndefined();
}

/*
 * Bind
 * 
 */
void ODBCStatement::Bind(const v8::FunctionCallbackInfo<v8::Value>& args) {
	DEBUG_PRINTF("ODBCStatement::Bind\n");
  
	v8::Isolate* isolate = args.GetIsolate();
	v8::EscapableHandleScope scope(isolate);

	if ( !args[0]->IsArray() ) {
		isolate->ThrowException(Exception::TypeError(String::NewFromUtf8(isolate, "Argument 1 must be an Array")));
		throw Exception::TypeError(String::NewFromUtf8(isolate, "Argument 1 must be an Array"));
	}
  
	if (args.Length() <= (1) || !args[1]->IsFunction()) {
		isolate->ThrowException(Exception::TypeError(String::NewFromUtf8(isolate, "Argument 1 must be a function")));
		throw Exception::TypeError(String::NewFromUtf8(isolate, "Argument 1 must be a function"));
	}
	Local<Function> cb = Local<Function>::Cast(args[1]);

	ODBCStatement* stmt = ObjectWrap::Unwrap<ODBCStatement>(args.Holder());
  
	uv_work_t* work_req = (uv_work_t *) (calloc(1, sizeof(uv_work_t)));
  
	bind_work_data* data = (bind_work_data *) calloc(1, sizeof(bind_work_data));

	//if we previously had parameters, then be sure to free them
	//before allocating more
	if (stmt->paramCount) {
		int count = stmt->paramCount;
		stmt->paramCount = 0;
    
		Parameter prm;
    
		//free parameter memory
		for (int i = 0; i < count; i++) {
			if (prm = stmt->params[i], prm.ParameterValuePtr != NULL) {
				switch (prm.ValueType) {
					case SQL_C_WCHAR:
						free(prm.ParameterValuePtr);
						break;
					case SQL_C_CHAR:
						free(prm.ParameterValuePtr);
						break; 
					case SQL_C_SBIGINT:
						delete (int64_t *)prm.ParameterValuePtr;
						break;
					case SQL_C_DOUBLE:
						delete (double *)prm.ParameterValuePtr;
						break;
					case SQL_C_BIT:
						delete (bool *)prm.ParameterValuePtr;
						break;
				}
			}
		}

		free(stmt->params);
	}
  
	data->stmt = stmt;
  
	DEBUG_PRINTF("ODBCStatement::Bind m_hDBC=%X m_hDBC=%X m_hSTMT=%X\n", data->stmt->m_hENV, data->stmt->m_hDBC, data->stmt->m_hSTMT);
  
	v8::Persistent<v8::Function, CopyablePersistentTraits<v8::Function>> persistent(isolate, cb);
	data->cb = persistent;

	data->stmt->params = ODBC::GetParametersFromArray(
	Local<Array>::Cast(args[0]), 
	&data->stmt->paramCount);
  
	work_req->data = data;
  
	uv_queue_work(uv_default_loop(), work_req, UV_Bind, (uv_after_work_cb)UV_AfterBind);

	stmt->Ref();

	args.GetReturnValue().SetUndefined();
}

void ODBCStatement::UV_Bind(uv_work_t* req) {
	DEBUG_PRINTF("ODBCStatement::UV_Bind\n");
  
	bind_work_data* data = (bind_work_data *)(req->data);

	DEBUG_PRINTF("ODBCStatement::UV_Bind m_hDBC=%X m_hDBC=%X m_hSTMT=%X\n",	data->stmt->m_hENV,	data->stmt->m_hDBC,	data->stmt->m_hSTMT);
  
	SQLRETURN ret = SQL_SUCCESS;
	Parameter prm;
  
	for (int i = 0; i < data->stmt->paramCount; i++) {
		prm = data->stmt->params[i];
    
		DEBUG_PRINTF("ODBCStatement::UV_Bind - param[%i]: c_type=%i type=%i buffer_length=%i size=%i length=%i &length=%X decimals=%i value=%s\n", i, prm.ValueType, prm.ParameterType, prm.BufferLength, prm.ColumnSize, prm.StrLen_or_IndPtr,	&data->stmt->params[i].StrLen_or_IndPtr, prm.DecimalDigits, prm.ParameterValuePtr);

		//StatementHandle, ParameterNumber, InputOutputType
		ret = SQLBindParameter(data->stmt->m_hSTMT,	i + 1, SQL_PARAM_INPUT, prm.ValueType, prm.ParameterType, prm.ColumnSize, prm.DecimalDigits, prm.ParameterValuePtr, prm.BufferLength, &data->stmt->params[i].StrLen_or_IndPtr);

		if (ret == SQL_ERROR) {
			break;
		}
	}

	data->result = ret;
}

void ODBCStatement::UV_AfterBind(uv_work_t* req, int status) {
	DEBUG_PRINTF("ODBCStatement::UV_AfterBind\n");
  
	bind_work_data* data = (bind_work_data *)(req->data);
  
	v8::Isolate* isolate = v8::Isolate::GetCurrent();
	v8::EscapableHandleScope scope(isolate);

	//an easy reference to the statment object
	ODBCStatement* self = data->stmt->self();

	//Check if there were errors 
	if(data->result == SQL_ERROR) {
		ODBC::CallbackSQLError(SQL_HANDLE_STMT, self->m_hSTMT, data->cb);
	}
	else {
		Local<Value> args[2];

		args[0] = Local<Value>::New(isolate, Null(isolate));
		args[1] = Local<Value>::New(isolate, True(isolate));

		TryCatch try_catch;

		v8::Local<v8::Function> f = v8::Local<v8::Function>::New(isolate, data->cb);
		f->Call(isolate->GetCurrentContext()->Global(), 2, args);

		if (try_catch.HasCaught()) {
			FatalException(try_catch);
		}
	}

	self->Unref();
	data->cb.Reset();
  
	free(data);
	free(req);
}

/*
 * CloseSync
 */
void ODBCStatement::CloseSync(const v8::FunctionCallbackInfo<v8::Value>& args) {
	DEBUG_PRINTF("ODBCStatement::CloseSync\n");
  
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

	ODBCStatement* stmt = ObjectWrap::Unwrap<ODBCStatement>(args.Holder());
  
	DEBUG_PRINTF("ODBCStatement::CloseSync closeOption=%i\n", closeOption);
  
	if (closeOption == SQL_DESTROY) {
		stmt->Free();
	}
	else {
		uv_mutex_lock(&ODBC::g_odbcMutex);
    	SQLFreeStmt(stmt->m_hSTMT, closeOption);
		uv_mutex_unlock(&ODBC::g_odbcMutex);
	}

	args.GetReturnValue().Set(True(isolate));
}
