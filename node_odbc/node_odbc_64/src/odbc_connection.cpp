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
#include <time.h>
#include <uv.h>

#include "odbc.h"
#include "odbc_connection.h"
#include "odbc_result.h"
#include "odbc_statement.h"

using namespace v8;
using namespace node;

Persistent<FunctionTemplate> ODBCConnection::constructor_template;
Persistent<String> ODBCConnection::OPTION_SQL(v8::Isolate::GetCurrent(), String::NewFromUtf8(v8::Isolate::GetCurrent(), "sql"));
Persistent<String> ODBCConnection::OPTION_PARAMS(v8::Isolate::GetCurrent(), String::NewFromUtf8(v8::Isolate::GetCurrent(), "params"));
Persistent<String> ODBCConnection::OPTION_NORESULTS(v8::Isolate::GetCurrent(), String::NewFromUtf8(v8::Isolate::GetCurrent(), "noResults"));

void ODBCConnection::Init(v8::Handle<Object> target) {
	DEBUG_PRINTF("ODBCConnection::Init\n");
	v8::Isolate* isolate = v8::Isolate::GetCurrent();
	v8::EscapableHandleScope scope(isolate);

	Local<FunctionTemplate> t = FunctionTemplate::New(isolate, ODBCConnection::New);

	// Constructor Template
	constructor_template.Reset(isolate, t);
	t->SetClassName(String::NewFromUtf8(isolate, "ODBCConnection", String::kInternalizedString));

	// Reserve space for one Handle<Value>
	Local<ObjectTemplate> instance_template = t->InstanceTemplate();
	instance_template->SetInternalFieldCount(1);
  
	// Properties
	//instance_template->SetAccessor(String::New("mode"), ModeGetter, ModeSetter);
	instance_template->SetAccessor(String::NewFromUtf8(isolate, "connected"), ConnectedGetter);
	instance_template->SetAccessor(String::NewFromUtf8(isolate, "connectTimeout"), ConnectTimeoutGetter, (AccessorSetterCallback)ConnectTimeoutSetter);
	instance_template->SetAccessor(String::NewFromUtf8(isolate, "loginTimeout"), LoginTimeoutGetter, (AccessorSetterCallback)LoginTimeoutSetter);
  
	// Prototype Methods
	NODE_SET_PROTOTYPE_METHOD(t, "open", Open);
	NODE_SET_PROTOTYPE_METHOD(t, "openSync", OpenSync);
	NODE_SET_PROTOTYPE_METHOD(t, "close", Close);
	NODE_SET_PROTOTYPE_METHOD(t, "closeSync", CloseSync);
	NODE_SET_PROTOTYPE_METHOD(t, "createStatement", CreateStatement);
	NODE_SET_PROTOTYPE_METHOD(t, "createStatementSync", CreateStatementSync);
	NODE_SET_PROTOTYPE_METHOD(t, "query", Query);
	NODE_SET_PROTOTYPE_METHOD(t, "querySync", QuerySync);
  
	NODE_SET_PROTOTYPE_METHOD(t, "beginTransaction", BeginTransaction);
	NODE_SET_PROTOTYPE_METHOD(t, "beginTransactionSync", BeginTransactionSync);
	NODE_SET_PROTOTYPE_METHOD(t, "endTransaction", EndTransaction);
	NODE_SET_PROTOTYPE_METHOD(t, "endTransactionSync", EndTransactionSync);
  
	NODE_SET_PROTOTYPE_METHOD(t, "columns", Columns);
	NODE_SET_PROTOTYPE_METHOD(t, "tables", Tables);
  
	// Attach the Database Constructor to the target object
	target->Set(v8::String::NewFromUtf8(isolate, "ODBCConnection", String::kInternalizedString), t->GetFunction());
}

ODBCConnection::~ODBCConnection() {
	DEBUG_PRINTF("ODBCConnection::~ODBCConnection\n");
	this->Free();
}

void ODBCConnection::Free() {
	DEBUG_PRINTF("ODBCConnection::Free\n");
	if (m_hDBC) {
		uv_mutex_lock(&ODBC::g_odbcMutex);
    
		if (m_hDBC) {
			SQLDisconnect(m_hDBC);
			SQLFreeHandle(SQL_HANDLE_DBC, m_hDBC);
			m_hDBC = NULL;
		}
    
		uv_mutex_unlock(&ODBC::g_odbcMutex);
	}
}

/*
 * New
 */
void ODBCConnection::New(const v8::FunctionCallbackInfo<v8::Value>& args) {
	DEBUG_PRINTF("ODBCConnection::New\n");
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

	HENV hENV = static_cast<HENV>(js_henv->Value());
	HDBC hDBC = static_cast<HDBC>(js_hdbc->Value());
  
	ODBCConnection* conn = new ODBCConnection(hENV, hDBC);
  
	conn->Wrap(args.Holder());
  
	//set default connectTimeout to 0 seconds
	conn->connectTimeout = 0;
	//set default loginTimeout to 5 seconds
	conn->loginTimeout = 5;

	args.GetReturnValue().Set(args.Holder());
}

void ODBCConnection::ConnectedGetter(Local<String> property, const PropertyCallbackInfo<Value>& info) {
	v8::Isolate* isolate = info.GetIsolate();
	v8::EscapableHandleScope scope(isolate);

	ODBCConnection *obj = ObjectWrap::Unwrap<ODBCConnection>(info.Holder());

	info.GetReturnValue().Set(obj->connected ? True(isolate) : False(isolate));
}

void ODBCConnection::ConnectTimeoutGetter(Local<String> property, const PropertyCallbackInfo<Value>& info) {
	v8::Isolate* isolate = info.GetIsolate();
	v8::EscapableHandleScope scope(isolate);

	ODBCConnection *obj = ObjectWrap::Unwrap<ODBCConnection>(info.Holder());

	info.GetReturnValue().Set(Number::New(isolate, obj->connectTimeout));
}

void ODBCConnection::ConnectTimeoutSetter(Local<String> property, Local<Value> value, const PropertyCallbackInfo<Value>& info) {
	v8::Isolate* isolate = info.GetIsolate();
	v8::EscapableHandleScope scope(isolate);

	ODBCConnection *obj = ObjectWrap::Unwrap<ODBCConnection>(info.Holder());
  
	if (value->IsNumber()) {
		obj->connectTimeout = value->Uint32Value();
	}
}

void ODBCConnection::LoginTimeoutGetter(Local<String> property, const PropertyCallbackInfo<Value>& info) {
	v8::Isolate* isolate = info.GetIsolate();
	v8::EscapableHandleScope scope(isolate);

	ODBCConnection *obj = ObjectWrap::Unwrap<ODBCConnection>(info.Holder());

	info.GetReturnValue().Set(Number::New(isolate, obj->loginTimeout));
}

void ODBCConnection::LoginTimeoutSetter(Local<String> property, Local<Value> value, const PropertyCallbackInfo<Value>& info) {
	v8::Isolate* isolate = info.GetIsolate();
	v8::EscapableHandleScope scope(isolate);

	ODBCConnection *obj = ObjectWrap::Unwrap<ODBCConnection>(info.Holder());
  
	if (value->IsNumber()) {
		obj->loginTimeout = value->Uint32Value();
	}
}

/*
 * Open
 * 
 */
void ODBCConnection::Open(const v8::FunctionCallbackInfo<v8::Value>& args) {
	DEBUG_PRINTF("ODBCConnection::Open\n");
	v8::Isolate* isolate = args.GetIsolate();
	v8::EscapableHandleScope scope(isolate);

	if (args.Length() <= (0) || !args[0]->IsString()) {
		isolate->ThrowException(Exception::TypeError(String::NewFromUtf8(isolate, "Argument 0 must be a string")));
		throw Exception::TypeError(String::NewFromUtf8(isolate, "Argument 0 must be a string"));
	}

	Local<String> connection(args[0]->ToString());

	if (args.Length() <= (1) || !args[1]->IsFunction()) {
		isolate->ThrowException(Exception::TypeError(String::NewFromUtf8(isolate, "Argument 1 must be a function")));
		throw Exception::TypeError(String::NewFromUtf8(isolate, "Argument 1 must be a function"));
	}
	Local<Function> cb = Local<Function>::Cast(args[1]);

	//get reference to the connection object
	ODBCConnection* conn = ObjectWrap::Unwrap<ODBCConnection>(args.Holder());

	//create a uv work request
	uv_work_t* work_req = (uv_work_t *)(calloc(1, sizeof(uv_work_t)));

	//allocate our worker data
	open_connection_work_data* data = (open_connection_work_data *)calloc(1, sizeof(open_connection_work_data));

	data->connectionLength = connection->Length() + 1;

	//copy the connection string to the work data  
#ifdef UNICODE
	data->connection = (uint16_t *)malloc(sizeof(uint16_t) * data->connectionLength);
	connection->Write((uint16_t*)data->connection);
#else
	data->connection = (char *)malloc(sizeof(char) * data->connectionLength);
	connection->WriteUtf8((char*)data->connection);
#endif

	v8::Persistent<v8::Function, CopyablePersistentTraits<v8::Function>> persistent(isolate, cb);
	data->cb = persistent;
	data->conn = conn;

	work_req->data = data;

	//queue the work
	uv_queue_work(uv_default_loop(), work_req, UV_Open, (uv_after_work_cb)UV_AfterOpen);

	conn->Ref();

	args.GetReturnValue().Set(args.Holder());
}

void ODBCConnection::UV_Open(uv_work_t* req) {
	DEBUG_PRINTF("ODBCConnection::UV_Open\n");
	open_connection_work_data* data = (open_connection_work_data *)(req->data);
  
	ODBCConnection* self = data->conn->self();

	DEBUG_PRINTF("ODBCConnection::UV_Open : connectTimeout=%i, loginTimeout = %i\n", *&(self->connectTimeout), *&(self->loginTimeout));
  
	uv_mutex_lock(&ODBC::g_odbcMutex); 
  
	if (self->connectTimeout > 0) {
		//NOTE: SQLSetConnectAttr requires the thread to be locked
		//ConnectionHandle, Attribute, ValuePtr,StringLength
		SQLSetConnectAttr(self->m_hDBC, SQL_ATTR_CONNECTION_TIMEOUT, (SQLPOINTER) size_t(self->connectTimeout), SQL_IS_UINTEGER);
	}
  
	if (self->loginTimeout > 0) {
		//NOTE: SQLSetConnectAttr requires the thread to be locked
		//ConnectionHandle, Attribute, ValuePtr,StringLength
		SQLSetConnectAttr(self->m_hDBC, SQL_ATTR_LOGIN_TIMEOUT, (SQLPOINTER) size_t(self->loginTimeout), SQL_IS_UINTEGER);
	}
  
	//Attempt to connect
	//NOTE: SQLDriverConnect requires the thread to be locked
	//ConnectionHandle, WindowHandle, InConnectionString, StringLength1, OutConnectionString, BufferLength - in characters, StringLength2Ptr, DriverCompletion
	int ret = SQLDriverConnect(self->m_hDBC, NULL, (SQLTCHAR*) data->connection, data->connectionLength, NULL, 0, NULL, SQL_DRIVER_NOPROMPT);
  
	if (SQL_SUCCEEDED(ret)) {
		HSTMT hStmt;
    
		//allocate a temporary statment
		ret = SQLAllocHandle(SQL_HANDLE_STMT, self->m_hDBC, &hStmt);
    
		//try to determine if the driver can handle
		//multiple recordsets
		ret = SQLGetFunctions(self->m_hDBC, SQL_API_SQLMORERESULTS, &(self->canHaveMoreResults));

		if (!SQL_SUCCEEDED(ret)) {
			self->canHaveMoreResults = 0;
		}
    
		//free the handle
		ret = SQLFreeHandle( SQL_HANDLE_STMT, hStmt);
	}

	uv_mutex_unlock(&ODBC::g_odbcMutex);
  
	data->result = ret;
}

void ODBCConnection::UV_AfterOpen(uv_work_t* req, int status) {
	DEBUG_PRINTF("ODBCConnection::UV_AfterOpen\n");
	v8::Isolate* isolate = v8::Isolate::GetCurrent();
	v8::EscapableHandleScope scope(isolate);

	open_connection_work_data* data = (open_connection_work_data *)(req->data);
  
	Local<Value> argv[1];
  
	bool err = false;

	if (data->result) {
		err = true;
		Local<Object> objError = ODBC::GetSQLError(SQL_HANDLE_DBC, data->conn->self()->m_hDBC);
		argv[0] = objError;
	}

	if (!err) {
		data->conn->self()->connected = true;
		//only uv_ref if the connection was successful
#if NODE_VERSION_AT_LEAST(0, 7, 9)
		uv_ref((uv_handle_t *)&ODBC::g_async);
#else
		uv_ref(uv_default_loop());
#endif
	}

	TryCatch try_catch;

	data->conn->Unref();

	v8::Local<v8::Function> f = v8::Local<v8::Function>::New(isolate, data->cb);
	f->Call(isolate->GetCurrentContext()->Global(), err ? 1 : 0, argv);

	if (try_catch.HasCaught()) {
		FatalException(try_catch);
	}

	data->cb.Reset();
  
	free(data->connection);
	free(data);
	free(req);
}

/*
 * OpenSync
 */
void ODBCConnection::OpenSync(const v8::FunctionCallbackInfo<v8::Value>& args) {
	DEBUG_PRINTF("ODBCConnection::OpenSync\n");
	v8::Isolate* isolate = args.GetIsolate();
	v8::EscapableHandleScope scope(isolate);

	if (args.Length() <= (0) || !args[0]->IsString()) {
		isolate->ThrowException(Exception::TypeError(String::NewFromUtf8(isolate, "Argument 0 must be a string"))); \
		throw Exception::TypeError(String::NewFromUtf8(isolate, "Argument 0 must be a string"));
	}
	Local<String> connection(args[0]->ToString());

	//get reference to the connection object
	ODBCConnection* conn = ObjectWrap::Unwrap<ODBCConnection>(args.Holder());
 
	DEBUG_PRINTF("ODBCConnection::OpenSync : connectTimeout=%i, loginTimeout = %i\n", *&(conn->connectTimeout), *&(conn->loginTimeout));

	Local<Object> objError;
	SQLRETURN ret;
	bool err = false;
  
	int connectionLength = connection->Length() + 1;
  
#ifdef UNICODE
	uint16_t* connectionString = (uint16_t *) malloc(connectionLength * sizeof(uint16_t));
	connection->Write(connectionString);
#else
	char* connectionString = (char *) malloc(connectionLength);
	connection->WriteUtf8(connectionString);
#endif
  
	uv_mutex_lock(&ODBC::g_odbcMutex);
  
	if (conn->connectTimeout > 0) {
		//NOTE: SQLSetConnectAttr requires the thread to be locked
		//ConnectionHandle, Attribute, ValuePtr, StringLength
		SQLSetConnectAttr(conn->m_hDBC, SQL_ATTR_CONNECTION_TIMEOUT, (SQLPOINTER) size_t(conn->connectTimeout), SQL_IS_UINTEGER);
	}

	if (conn->loginTimeout > 0) {
		//NOTE: SQLSetConnectAttr requires the thread to be locked
		//ConnectionHandle, Attribute, ValuePtr, StringLength
		SQLSetConnectAttr(conn->m_hDBC, SQL_ATTR_LOGIN_TIMEOUT, (SQLPOINTER) size_t(conn->loginTimeout), SQL_IS_UINTEGER);
	}
  
	//Attempt to connect
	//NOTE: SQLDriverConnect requires the thread to be locked
	//ConnectionHandle, WindowHandle, InConnectionString, StringLength1, OutConnectionString, BufferLength - in characters, StringLength2Ptr, DriverCompletion
	ret = SQLDriverConnect(conn->m_hDBC, NULL, (SQLTCHAR*) connectionString, connectionLength, NULL, 0, NULL, SQL_DRIVER_NOPROMPT);

	if (!SQL_SUCCEEDED(ret)) {
		err = true;
		objError = ODBC::GetSQLError(SQL_HANDLE_DBC, conn->self()->m_hDBC);
	}
	else {
		HSTMT hStmt;
    
		//allocate a temporary statment
		ret = SQLAllocHandle(SQL_HANDLE_STMT, conn->m_hDBC, &hStmt);
    
		//try to determine if the driver can handle
		//multiple recordsets
		ret = SQLGetFunctions(conn->m_hDBC, SQL_API_SQLMORERESULTS, &(conn->canHaveMoreResults));

		if (!SQL_SUCCEEDED(ret)) {
			conn->canHaveMoreResults = 0;
		}
  
		//free the handle
		ret = SQLFreeHandle( SQL_HANDLE_STMT, hStmt);
    
		conn->self()->connected = true;
    
		//only uv_ref if the connection was successful
#if NODE_VERSION_AT_LEAST(0, 7, 9)
		uv_ref((uv_handle_t *)&ODBC::g_async);
#else
		uv_ref(uv_default_loop());
#endif
	}

	uv_mutex_unlock(&ODBC::g_odbcMutex);

	free(connectionString);
  
	if (err) {
		isolate->ThrowException(objError);
		args.GetReturnValue().Set(False(isolate));
		throw objError;
	}
	else {
		args.GetReturnValue().Set(True(isolate));
	}
}

/*
 * Close
 * 
 */
void ODBCConnection::Close(const v8::FunctionCallbackInfo<v8::Value>& args) {
	DEBUG_PRINTF("ODBCConnection::Close\n");
	v8::Isolate* isolate = args.GetIsolate();
	v8::EscapableHandleScope scope(isolate);

	if (args.Length() <= (0) || !args[0]->IsFunction()) {
		isolate->ThrowException(Exception::TypeError(String::NewFromUtf8(isolate, "Argument 0 must be a function")));
		throw Exception::TypeError(String::NewFromUtf8(isolate, "Argument 0 must be a function"));
	}
	Local<Function> cb = Local<Function>::Cast(args[0]);

	ODBCConnection* conn = ObjectWrap::Unwrap<ODBCConnection>(args.Holder());
  
	uv_work_t* work_req = (uv_work_t *) (calloc(1, sizeof(uv_work_t)));
  
	close_connection_work_data* data = (close_connection_work_data *)(calloc(1, sizeof(close_connection_work_data)));

	v8::Persistent<v8::Function, CopyablePersistentTraits<v8::Function>> persistent(isolate, cb);
	data->cb = persistent;

	data->conn = conn;

	work_req->data = data;
  
	uv_queue_work(uv_default_loop(), work_req, UV_Close, (uv_after_work_cb)UV_AfterClose);

	conn->Ref();

	args.GetReturnValue().SetUndefined();
}

void ODBCConnection::UV_Close(uv_work_t* req) {
	DEBUG_PRINTF("ODBCConnection::UV_Close\n");
	close_connection_work_data* data = (close_connection_work_data *)(req->data);
	ODBCConnection* conn = data->conn;
  
	//TODO: check to see if there are any open statements
	//on this connection
  
	conn->Free();
  
	data->result = 0;
}

void ODBCConnection::UV_AfterClose(uv_work_t* req, int status) {
	DEBUG_PRINTF("ODBCConnection::UV_AfterClose\n");
	v8::Isolate* isolate = v8::Isolate::GetCurrent();
	v8::EscapableHandleScope scope(isolate);

	close_connection_work_data* data = (close_connection_work_data *)(req->data);

	ODBCConnection* conn = data->conn;
  
	Local<Value> argv[1];
	bool err = false;
  
	if (data->result) {
		err = true;
		argv[0] = Exception::Error(String::NewFromUtf8(isolate, "Error closing database"));
	}
	else {
		conn->connected = false;
    
		//only unref if the connection was closed
#if NODE_VERSION_AT_LEAST(0, 7, 9)
		uv_unref((uv_handle_t *)&ODBC::g_async);
#else
		uv_unref(uv_default_loop());
#endif
	}

	TryCatch try_catch;

	data->conn->Unref();

	v8::Local<v8::Function> f = v8::Local<v8::Function>::New(isolate, data->cb);
	f->Call(isolate->GetCurrentContext()->Global(), err ? 1 : 0, argv);

	if (try_catch.HasCaught()) {
		FatalException(try_catch);
	}

	data->cb.Reset();

	free(data);
	free(req);
}

/*
 * CloseSync
 */
void ODBCConnection::CloseSync(const v8::FunctionCallbackInfo<v8::Value>& args) {
	DEBUG_PRINTF("ODBCConnection::CloseSync\n");
	v8::Isolate* isolate = args.GetIsolate();
	v8::EscapableHandleScope scope(isolate);

	ODBCConnection* conn = ObjectWrap::Unwrap<ODBCConnection>(args.Holder());
  
	//TODO: check to see if there are any open statements
	//on this connection
  
	conn->Free();
  
	conn->connected = false;

#if NODE_VERSION_AT_LEAST(0, 7, 9)
	uv_unref((uv_handle_t *)&ODBC::g_async);
#else
	uv_unref(uv_default_loop());
#endif
 	args.GetReturnValue().Set(True(isolate));
}

/*
 * CreateStatementSync
 * 
 */
void ODBCConnection::CreateStatementSync(const v8::FunctionCallbackInfo<v8::Value>& args) {
	DEBUG_PRINTF("ODBCConnection::CreateStatementSync\n");
	v8::Isolate* isolate = args.GetIsolate();
	v8::EscapableHandleScope scope(isolate);

	ODBCConnection* conn = ObjectWrap::Unwrap<ODBCConnection>(args.Holder());
   
	HSTMT hSTMT;

	uv_mutex_lock(&ODBC::g_odbcMutex);
  
	SQLAllocHandle(SQL_HANDLE_STMT, conn->m_hDBC, &hSTMT);
  
	uv_mutex_unlock(&ODBC::g_odbcMutex);
  
	Local<Value> params[3];
	params[0] = External::New(isolate, conn->m_hENV);
	params[1] = External::New(isolate, conn->m_hDBC);
	params[2] = External::New(isolate, hSTMT);
  
	v8::Local<v8::FunctionTemplate> ft = v8::Local<v8::FunctionTemplate>::New(isolate, ODBCConnection::constructor_template);
	Local<Object> js_result = ft->GetFunction()->NewInstance(3, params);

	args.GetReturnValue().Set(js_result);
}

/*
 * CreateStatement
 * 
 */
void ODBCConnection::CreateStatement(const v8::FunctionCallbackInfo<v8::Value>& args) {
	DEBUG_PRINTF("ODBCConnection::CreateStatement\n");
	v8::Isolate* isolate = args.GetIsolate();
	v8::EscapableHandleScope scope(isolate);

	if (args.Length() <= (0) || !args[0]->IsFunction()) {
		isolate->ThrowException(Exception::TypeError(String::NewFromUtf8(isolate, "Argument 0 must be a function")));
		throw Exception::TypeError(String::NewFromUtf8(isolate, "Argument 0 must be a function"));
	}
	Local<Function> cb = Local<Function>::Cast(args[0]);

	ODBCConnection* conn = ObjectWrap::Unwrap<ODBCConnection>(args.Holder());
    
	//initialize work request
	uv_work_t* work_req = (uv_work_t *) (calloc(1, sizeof(uv_work_t)));
  
	//initialize our data
	create_statement_work_data* data = (create_statement_work_data *) (calloc(1, sizeof(create_statement_work_data)));

	v8::Persistent<v8::Function, CopyablePersistentTraits<v8::Function>> persistent(isolate, cb);
	data->cb = persistent;
	data->conn = conn;

	work_req->data = data;
  
	uv_queue_work(uv_default_loop(), work_req, UV_CreateStatement, (uv_after_work_cb)UV_AfterCreateStatement);

	conn->Ref();

	args.GetReturnValue().SetUndefined();
}

void ODBCConnection::UV_CreateStatement(uv_work_t* req) {
	DEBUG_PRINTF("ODBCConnection::UV_CreateStatement\n");
  
	//get our work data
	create_statement_work_data* data = (create_statement_work_data *)(req->data);

	DEBUG_PRINTF("ODBCConnection::UV_CreateStatement m_hDBC=%X m_hDBC=%X m_hSTMT=%X\n", data->conn->m_hENV, data->conn->m_hDBC, data->hSTMT);
  
	uv_mutex_lock(&ODBC::g_odbcMutex);
  
	//allocate a new statment handle
	SQLAllocHandle( SQL_HANDLE_STMT, data->conn->m_hDBC, &data->hSTMT);

	uv_mutex_unlock(&ODBC::g_odbcMutex);
  
	DEBUG_PRINTF("ODBCConnection::UV_CreateStatement m_hDBC=%X m_hDBC=%X m_hSTMT=%X\n",	data->conn->m_hENV,	data->conn->m_hDBC,	data->hSTMT);
}

void ODBCConnection::UV_AfterCreateStatement(uv_work_t* req, int status) {
	DEBUG_PRINTF("ODBCConnection::UV_AfterCreateStatement\n");
	v8::Isolate* isolate = v8::Isolate::GetCurrent();
	v8::EscapableHandleScope scope(isolate);

	create_statement_work_data* data = (create_statement_work_data *)(req->data);

	DEBUG_PRINTF("ODBCConnection::UV_AfterCreateStatement m_hDBC=%X m_hDBC=%X hSTMT=%X\n", data->conn->m_hENV, data->conn->m_hDBC, data->hSTMT);
  
	Local<Value> args[3];
	args[0] = External::New(isolate, data->conn->m_hENV);
	args[1] = External::New(isolate, data->conn->m_hDBC);
	args[2] = External::New(isolate, data->hSTMT);
  
	v8::Local<v8::FunctionTemplate> ft = v8::Local<v8::FunctionTemplate>::New(isolate, ODBCConnection::constructor_template);
	Local<Object> js_result = ft->GetFunction()->NewInstance(3, args);

	args[0] = Local<Value>::New(isolate, Null(isolate));
	args[1] = Local<Object>::New(isolate, js_result);


	TryCatch try_catch;

	v8::Local<v8::Function> f = v8::Local<v8::Function>::New(isolate, data->cb);
	f->Call(isolate->GetCurrentContext()->Global(), 2, args);

	if (try_catch.HasCaught()) {
		FatalException(try_catch);
	}
  
	data->conn->Unref();
	data->cb.Reset();

	free(data);
	free(req);
}

/*
 * Query
 */

void ODBCConnection::Query(const v8::FunctionCallbackInfo<v8::Value>& args) {
	DEBUG_PRINTF("ODBCConnection::Query\n");
  
	v8::Isolate* isolate = args.GetIsolate();
	v8::EscapableHandleScope scope(isolate);

	Local<Function> cb;
  
	Local<String> sql;
  
	ODBCConnection* conn = ObjectWrap::Unwrap<ODBCConnection>(args.Holder());
  
	uv_work_t* work_req = (uv_work_t *) (calloc(1, sizeof(uv_work_t)));
  
	query_work_data* data = (query_work_data *) calloc(1, sizeof(query_work_data));

	//Check arguments for different variations of calling this function
	if (args.Length() == 3) {
		//handle Query("sql string", [params], function cb () {});
    
		if ( !args[0]->IsString() ) {
			isolate->ThrowException(Exception::TypeError(String::NewFromUtf8(isolate, "Argument 0 must be an String.")));
			throw Exception::TypeError(String::NewFromUtf8(isolate, "Argument 0 must be a String"));
		}
		else if ( !args[1]->IsArray() ) {
			isolate->ThrowException(Exception::TypeError(String::NewFromUtf8(isolate, "Argument 1 must be an Array.")));
			throw Exception::TypeError(String::NewFromUtf8(isolate, "Argument 1 must be a an Array"));
		}
		else if ( !args[2]->IsFunction() ) {
			isolate->ThrowException(Exception::TypeError(String::NewFromUtf8(isolate, "Argument 2 must be a Function.")));
			throw Exception::TypeError(String::NewFromUtf8(isolate, "Argument 2 must be a Function"));
		}

		sql = args[0]->ToString();
    
		data->params = ODBC::GetParametersFromArray(
		Local<Array>::Cast(args[1]),
		&data->paramCount);
    
		cb = Local<Function>::Cast(args[2]);
	}
	else if (args.Length() == 2 ) {
		//handle either Query("sql", cb) or Query({ settings }, cb)
    
		if (!args[1]->IsFunction()) {
			isolate->ThrowException(Exception::TypeError(String::NewFromUtf8(isolate, "ODBCConnection::Query(): Argument 1 must be a Function.")));
			throw Exception::TypeError(String::NewFromUtf8(isolate, "ODBCConnection::Query(): Argument 1 must be a Function."));
		}
    
		cb = Local<Function>::Cast(args[1]);
    
		if (args[0]->IsString()) {
			//handle Query("sql", function cb () {})
			sql = args[0]->ToString();
			data->paramCount = 0;
		}
		else if (args[0]->IsObject()) {
			//NOTE: going forward this is the way we should expand options
			//rather than adding more arguments to the function signature.
			//specify options on an options object.
			//handle Query({}, function cb () {});
      
			Local<Object> obj = args[0]->ToObject();

			v8::Local<v8::String> optionSql = v8::Local<v8::String>::New(isolate, OPTION_SQL);
			v8::Local<v8::String> optionParams = v8::Local<v8::String>::New(isolate, OPTION_PARAMS);
			v8::Local<v8::String> optionNoResults = v8::Local<v8::String>::New(isolate, OPTION_NORESULTS);

			if (obj->Has(optionSql) && obj->Get(optionSql)->IsString()) {
				sql = obj->Get(optionSql)->ToString();
			}
			else {
				sql = String::NewFromUtf8(isolate, "");
			}
			if (obj->Has(optionParams) && obj->Get(optionParams)->IsArray()) {
				data->params = ODBC::GetParametersFromArray(Local<Array>::Cast(obj->Get(optionParams)), &data->paramCount);
			}
			else {
				data->paramCount = 0;
			}
      
			if (obj->Has(optionNoResults) && obj->Get(optionNoResults)->IsBoolean()) {
				data->noResultObject = obj->Get(optionNoResults)->ToBoolean()->Value();
			}
			else {
				data->noResultObject = false;
			}
		}
		else {
			isolate->ThrowException(Exception::TypeError(String::NewFromUtf8(isolate, "ODBCConnection::Query(): Argument 0 must be a String or an Object.")));
			throw Exception::TypeError(String::NewFromUtf8(isolate, "ODBCConnection::Query(): Argument 0 must be a String or an Object."));
		}
	}
	else {
		isolate->ThrowException(Exception::TypeError(String::NewFromUtf8(isolate, "ODBCConnection::Query(): Requires either 2 or 3 Arguments. ")));
		throw Exception::TypeError(String::NewFromUtf8(isolate, "ODBCConnection::Query(): Require either 2 or 3 Arguments."));
	}
	//Done checking arguments

	v8::Persistent<v8::Function, CopyablePersistentTraits<v8::Function>> persistent(isolate, cb);
	data->cb = persistent;
	data->sqlLen = sql->Length();

#ifdef UNICODE
	data->sqlSize = (data->sqlLen * sizeof(uint16_t)) + sizeof(uint16_t);
	data->sql = (uint16_t *) malloc(data->sqlSize);
	sql->Write((uint16_t *) data->sql);
#else
	data->sqlSize = sql->Utf8Length() + 1;
	data->sql = (char *) malloc(data->sqlSize);
	sql->WriteUtf8((char *) data->sql);
#endif

	DEBUG_PRINTF("ODBCConnection::Query : sqlLen=%i, sqlSize=%i, sql=%s\n", data->sqlLen, data->sqlSize, (char*) data->sql);
  
	data->conn = conn;
	work_req->data = data;
  
	uv_queue_work(uv_default_loop(), work_req, UV_Query, (uv_after_work_cb)UV_AfterQuery);

	conn->Ref();

	args.GetReturnValue().SetUndefined();
}

void ODBCConnection::UV_Query(uv_work_t* req) {
	DEBUG_PRINTF("ODBCConnection::UV_Query\n");
  
	query_work_data* data = (query_work_data *)(req->data);
  
	Parameter prm;
	SQLRETURN ret;
  
	uv_mutex_lock(&ODBC::g_odbcMutex);

	//allocate a new statment handle
	SQLAllocHandle( SQL_HANDLE_STMT, data->conn->m_hDBC, &data->hSTMT);

	uv_mutex_unlock(&ODBC::g_odbcMutex);

	// SQLExecDirect will use bound parameters, but without the overhead of SQLPrepare
	// for a single execution.
	if (data->paramCount) {
		for (int i = 0; i < data->paramCount; i++) {
			prm = data->params[i];
			DEBUG_TPRINTF(SQL_T("ODBCConnection::UV_Query - param[%i]: ValueType=%i type=%i BufferLength=%i size=%i length=%i &length=%X\n"), i, prm.ValueType, prm.ParameterType, prm.BufferLength, prm.ColumnSize, prm.StrLen_or_IndPtr, &data->params[i].StrLen_or_IndPtr);
			//StatementHandle, ParameterNumber, InputOutputType, ...
			ret = SQLBindParameter(data->hSTMT, i + 1, SQL_PARAM_INPUT, prm.ValueType, prm.ParameterType, prm.ColumnSize, prm.DecimalDigits, prm.ParameterValuePtr, prm.BufferLength, &data->params[i].StrLen_or_IndPtr);
	
			if (ret == SQL_ERROR) {
				data->result = ret;
				return;
			}
		}
	}

	// execute the query directly
	ret = SQLExecDirect(data->hSTMT, (SQLTCHAR *)data->sql, data->sqlLen);

	// this will be checked later in UV_AfterQuery
	data->result = ret;
}

void ODBCConnection::UV_AfterQuery(uv_work_t* req, int status) {
	DEBUG_PRINTF("ODBCConnection::UV_AfterQuery\n");
  
	v8::Isolate* isolate = v8::Isolate::GetCurrent();
	v8::EscapableHandleScope scope(isolate);

	query_work_data* data = (query_work_data *)(req->data);

	TryCatch try_catch;

	DEBUG_PRINTF("ODBCConnection::UV_AfterQuery : data->result=%i, data->noResultObject=%i\n", data->result, data->noResultObject);

	if (data->result != SQL_ERROR && data->noResultObject) {
		//We have been requested to not create a result object
		//this means we should release the handle now and call back
		//with True()
    
		uv_mutex_lock(&ODBC::g_odbcMutex);
    
		SQLFreeHandle(SQL_HANDLE_STMT, data->hSTMT);
   
		uv_mutex_unlock(&ODBC::g_odbcMutex);
    
		Local<Value> args[2];
		args[0] = Local<Value>::New(isolate, Null(isolate));
		args[1] = Local<Value>::New(isolate, True(isolate));
    
		v8::Local<v8::Function> f = v8::Local<v8::Function>::New(isolate, data->cb);
		f->Call(isolate->GetCurrentContext()->Global(), 2, args);
	}
	else {
		Local<Value> args[4];
		bool* canFreeHandle = new bool(true);
    
		args[0] = External::New(isolate, data->conn->m_hENV);
		args[1] = External::New(isolate, data->conn->m_hDBC);
		args[2] = External::New(isolate, data->hSTMT);
		args[3] = External::New(isolate, canFreeHandle);
    
		v8::Local<v8::FunctionTemplate> ft = v8::Local<v8::FunctionTemplate>::New(isolate, ODBCResult::constructor_template);
		Local<Object> js_result = ft->GetFunction()->NewInstance(4, args);

		// Check now to see if there was an error (as there may be further result sets)
		if (data->result == SQL_ERROR) {
			args[0] = ODBC::GetSQLError(SQL_HANDLE_STMT, data->hSTMT, (char *) "[node-odbc] SQL_ERROR");
		}
		else {
			args[0] = Local<Value>::New(isolate, Null(isolate));
		}
		args[1] = Local<Object>::New(isolate, js_result);
    
		v8::Local<v8::Function> f = v8::Local<v8::Function>::New(isolate, data->cb);
		f->Call(isolate->GetCurrentContext()->Global(), 2, args);
	}
  
	data->conn->Unref();
  
	if (try_catch.HasCaught()) {
		FatalException(try_catch);
	}
  
	data->cb.Reset();

	if (data->paramCount) {
		Parameter prm;
		// free parameters
		for (int i = 0; i < data->paramCount; i++) {
			if (prm = data->params[i], prm.ParameterValuePtr != NULL) {
				switch (prm.ValueType) {
					case SQL_C_WCHAR:
						free(prm.ParameterValuePtr);
						break; 
					case SQL_C_CHAR:
						free(prm.ParameterValuePtr);
						break; 
					case SQL_C_LONG:
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
    
		free(data->params);
	}
  
	free(data->sql);
	free(data->catalog);
	free(data->schema);
	free(data->table);
	free(data->type);
	free(data->column);
	free(data);
	free(req);
}


/*
 * QuerySync
 */
void ODBCConnection::QuerySync(const v8::FunctionCallbackInfo<v8::Value>& args) {
	DEBUG_PRINTF("ODBCConnection::QuerySync\n");
  
	v8::Isolate* isolate = args.GetIsolate();
	v8::EscapableHandleScope scope(isolate);

#ifdef UNICODE
	String::Value* sql;
#else
	String::Utf8Value* sql;
#endif
	ODBCConnection* conn = ObjectWrap::Unwrap<ODBCConnection>(args.Holder());
  
	Parameter* params = new Parameter[0];
	Parameter prm;
	SQLRETURN ret;
	HSTMT hSTMT;
	int paramCount = 0;
	bool noResultObject = false;
  
	//Check arguments for different variations of calling this function
	if (args.Length() == 2) {
		//handle QuerySync("sql string", [params]);
		if ( !args[0]->IsString() ) {
			isolate->ThrowException(Exception::TypeError(String::NewFromUtf8(isolate, "ODBCConnection::QuerySync(): Argument 0 must be an String.")));
			throw Exception::TypeError(String::NewFromUtf8(isolate, "ODBCConnection::Query(): Argument 0 must be a String."));
		}
		else if (!args[1]->IsArray()) {
			isolate->ThrowException(Exception::TypeError(String::NewFromUtf8(isolate, "ODBCConnection::QuerySync(): Argument 1 must be an Array.")));
			throw Exception::TypeError(String::NewFromUtf8(isolate, "ODBCConnection::Query(): Argument 1 must be a an Array."));
		}

#ifdef UNICODE
		sql = new String::Value(args[0]->ToString());
#else
		sql = new String::Utf8Value(args[0]->ToString());
#endif
		params = ODBC::GetParametersFromArray(Local<Array>::Cast(args[1]), &paramCount);
	}
	else if (args.Length() == 1 ) {
		//handle either QuerySync("sql") or QuerySync({ settings })

		if (args[0]->IsString()) {
			//handle Query("sql")
#ifdef UNICODE
			sql = new String::Value(args[0]->ToString());
#else
			sql = new String::Utf8Value(args[0]->ToString());
#endif
			paramCount = 0;
		}
		else if (args[0]->IsObject()) {
			//NOTE: going forward this is the way we should expand options
			//rather than adding more arguments to the function signature.
			//specify options on an options object.
			//handle Query({}, function cb () {});
      
			Local<Object> obj = args[0]->ToObject();
      
			v8::Local<v8::String> optionSql = v8::Local<v8::String>::New(isolate, OPTION_SQL);
			v8::Local<v8::String> optionParams = v8::Local<v8::String>::New(isolate, OPTION_PARAMS);
			v8::Local<v8::String> optionNoResults = v8::Local<v8::String>::New(isolate, OPTION_NORESULTS);

			if (obj->Has(optionSql) && obj->Get(optionSql)->IsString()) {
#ifdef UNICODE
				sql = new String::Value(obj->Get(optionSql)->ToString());
#else
				sql = new String::Utf8Value(obj->Get(optionSql)->ToString());
#endif
			}
			else {
#ifdef UNICODE
				sql = new String::Value(String::NewFromUtf8(isolate, ""));
#else
				sql = new String::Utf8Value(String::NewFromUtf8(isolate, ""));
#endif
			}
      
			if (obj->Has(optionParams) && obj->Get(optionParams)->IsArray()) {
				params = ODBC::GetParametersFromArray(Local<Array>::Cast(obj->Get(optionParams)), &paramCount);
			}
			else {
				paramCount = 0;
			}
      
			if (obj->Has(optionNoResults) && obj->Get(optionNoResults)->IsBoolean()) {
				noResultObject = obj->Get(optionNoResults)->ToBoolean()->Value();
			}
		}
		else {
			isolate->ThrowException(Exception::TypeError(String::NewFromUtf8(isolate, "ODBCConnection::QuerySync(): Argument 0 must be a String or an Object.")));
			throw Exception::TypeError(String::NewFromUtf8(isolate, "ODBCConnection::QuerySync(): Argument 0 must be a String or an Object."));
		}
	}
	else {
		isolate->ThrowException(Exception::TypeError(String::NewFromUtf8(isolate, "ODBCConnection::QuerySync(): Requires either 1 or 2 Arguments. ")));
		throw Exception::TypeError(String::NewFromUtf8(isolate, "ODBCConnection::QuerySync(): Requires either 1 or 2 Arguments."));
	}
	//Done checking arguments

	uv_mutex_lock(&ODBC::g_odbcMutex);

	//allocate a new statment handle
	ret = SQLAllocHandle( SQL_HANDLE_STMT, conn->m_hDBC, &hSTMT);

	uv_mutex_unlock(&ODBC::g_odbcMutex);

	DEBUG_PRINTF("ODBCConnection::QuerySync - hSTMT=%p\n", hSTMT);
  
	if (SQL_SUCCEEDED(ret)) {
		if (paramCount) {
			for (int i = 0; i < paramCount; i++) {
				prm = params[i];
				DEBUG_PRINTF("ODBCConnection::UV_Query - param[%i]: ValueType=%i type=%i BufferLength=%i size=%i length=%i &length=%X\n", i, prm.ValueType, prm.ParameterType, prm.BufferLength, prm.ColumnSize, prm.StrLen_or_IndPtr, &params[i].StrLen_or_IndPtr);
				//StatementHandle, ParameterNumber, InputOutputType
				ret = SQLBindParameter(hSTMT, i + 1, SQL_PARAM_INPUT, prm.ValueType, prm.ParameterType, prm.ColumnSize, prm.DecimalDigits, prm.ParameterValuePtr, prm.BufferLength, &params[i].StrLen_or_IndPtr);
        
				if (ret == SQL_ERROR) {
					break;
				}
			}
		}

		if (SQL_SUCCEEDED(ret)) {
			ret = SQLExecDirect(hSTMT, (SQLTCHAR *) **sql, sql->length());
		}
    
		// free parameters
		for (int i = 0; i < paramCount; i++) {
			if (prm = params[i], prm.ParameterValuePtr != NULL) {
				switch (prm.ValueType) {
					case SQL_C_WCHAR:
						free(prm.ParameterValuePtr); 
						break;
					case SQL_C_CHAR:
						free(prm.ParameterValuePtr);
						break; 
					case SQL_C_LONG:
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
  
	delete sql;
  
	//check to see if there was an error during execution
	if (ret == SQL_ERROR) {
		isolate->ThrowException(ODBC::GetSQLError(SQL_HANDLE_STMT, hSTMT, (char *) "[node-odbc] Error in ODBCConnection::QuerySync"));
		throw ODBC::GetSQLError(SQL_HANDLE_STMT, hSTMT, (char *) "[node-odbc] Error in ODBCConnection::QuerySync");
		args.GetReturnValue().SetUndefined();
	}
	else if (noResultObject) {
		//if there is not result object requested then
		//we must destroy the STMT ourselves.
		uv_mutex_lock(&ODBC::g_odbcMutex);
    
		SQLFreeHandle(SQL_HANDLE_STMT, hSTMT);
   
		uv_mutex_unlock(&ODBC::g_odbcMutex);
    
		args.GetReturnValue().Set(True(isolate));
	}
	else {
		Local<Value> args1[4];
		bool* canFreeHandle = new bool(true);
    
		args1[0] = External::New(isolate, conn->m_hENV);
		args1[1] = External::New(isolate, conn->m_hDBC);
		args1[2] = External::New(isolate, hSTMT);
		args1[3] = External::New(isolate, canFreeHandle);
    
		v8::Local<v8::FunctionTemplate> ft = v8::Local<v8::FunctionTemplate>::New(isolate, ODBCResult::constructor_template);
		Local<Object> js_result = ft->GetFunction()->NewInstance(4, args1);
		args.GetReturnValue().Set(js_result);
	}
}

/*
 * Tables
 */
void ODBCConnection::Tables(const v8::FunctionCallbackInfo<v8::Value>& args) {
	v8::Isolate* isolate = args.GetIsolate();
	v8::EscapableHandleScope scope(isolate);

	if (args.Length() <= (0) || (!args[0]->IsString() && !args[0]->IsNull())) {
		isolate->ThrowException(Exception::TypeError(String::NewFromUtf8(isolate, "Argument 0 must be a string or null")));
		throw Exception::TypeError(String::NewFromUtf8(isolate, "Argument 0 must be a string or null"));
	}
	Local<String> catalog(args[0]->ToString());

	if (args.Length() <= (1) || (!args[1]->IsString() && !args[1]->IsNull())) {
		isolate->ThrowException(Exception::TypeError(String::NewFromUtf8(isolate, "Argument 1 must be a string or null")));
		throw Exception::TypeError(String::NewFromUtf8(isolate, "Argument 1 must be a string or null"));
	}
	Local<String> schema(args[1]->ToString());

	if (args.Length() <= (2) || (!args[2]->IsString() && !args[2]->IsNull())) {
		isolate->ThrowException(Exception::TypeError(String::NewFromUtf8(isolate, "Argument 2 must be a string or null")));
		throw Exception::TypeError(String::NewFromUtf8(isolate, "Argument 2 must be a string or null"));
	}
	Local<String> table(args[2]->ToString());

	if (args.Length() <= (3) || (!args[3]->IsString() && !args[3]->IsNull())) {
		isolate->ThrowException(Exception::TypeError(String::NewFromUtf8(isolate, "Argument 3 must be a string or null")));
		throw Exception::TypeError(String::NewFromUtf8(isolate, "Argument 3 must be a string or null"));
	}
	Local<String> type(args[3]->ToString());

	Local<Function> cb = Local<Function>::Cast(args[4]);

	ODBCConnection* conn = ObjectWrap::Unwrap<ODBCConnection>(args.Holder());
  
	uv_work_t* work_req = (uv_work_t *) (calloc(1, sizeof(uv_work_t)));
  
	query_work_data* data = (query_work_data *) calloc(1, sizeof(query_work_data));
  
	if (!data) {
		isolate->ThrowException(Exception::Error(String::NewFromUtf8(isolate, "Could not allocate enough memory")));
		throw Exception::TypeError(String::NewFromUtf8(isolate, "Could not allocate enough memory"));
	}

	data->sql = NULL;
	data->catalog = NULL;
	data->schema = NULL;
	data->table = NULL;
	data->type = NULL;
	data->column = NULL;
	v8::Persistent<v8::Function, CopyablePersistentTraits<v8::Function>> persistent(isolate, cb);
	data->cb = persistent;

	if (!catalog->Equals(String::NewFromUtf8(isolate, "null"))) {
#ifdef UNICODE
		data->catalog = (uint16_t *) malloc((catalog->Length() * sizeof(uint16_t)) + sizeof(uint16_t));
		catalog->Write((uint16_t *) data->catalog);
#else
		data->catalog = (char *) malloc(catalog->Length() + 1);
		catalog->WriteUtf8((char *) data->catalog);
#endif
	}

	if (!schema->Equals(String::NewFromUtf8(isolate, "null"))) {
#ifdef UNICODE
	    data->schema = (uint16_t *) malloc((schema->Length() * sizeof(uint16_t)) + sizeof(uint16_t));
		schema->Write((uint16_t *) data->schema);
#else
		data->schema = (char *) malloc(schema->Length() + 1);
		schema->WriteUtf8((char *) data->schema);
#endif
	}
  
	if (!table->Equals(String::NewFromUtf8(isolate, "null"))) {
#ifdef UNICODE
		data->table = (uint16_t *) malloc((table->Length() * sizeof(uint16_t)) + sizeof(uint16_t));
		table->Write((uint16_t *) data->table);
#else
		data->table = (char *) malloc(table->Length() + 1);
		table->WriteUtf8((char *) data->table);
#endif
	}
  
	if (!type->Equals(String::NewFromUtf8(isolate, "null"))) {
#ifdef UNICODE
		data->type = (uint16_t *) malloc((type->Length() * sizeof(uint16_t)) + sizeof(uint16_t));
		type->Write((uint16_t *) data->type);
#else
		data->type = (char *) malloc(type->Length() + 1);
		type->WriteUtf8((char *) data->type);
#endif
	}
  
	data->conn = conn;
	work_req->data = data;
  
	uv_queue_work(uv_default_loop(), work_req, UV_Tables, (uv_after_work_cb) UV_AfterQuery);

	conn->Ref();

	args.GetReturnValue().SetUndefined();
}

void ODBCConnection::UV_Tables(uv_work_t* req) {
	query_work_data* data = (query_work_data *)(req->data);
  
	uv_mutex_lock(&ODBC::g_odbcMutex);
  
	SQLAllocHandle(SQL_HANDLE_STMT, data->conn->m_hDBC, &data->hSTMT );
  
	uv_mutex_unlock(&ODBC::g_odbcMutex);
  
	SQLRETURN ret = SQLTables(data->hSTMT, (SQLTCHAR *) data->catalog, SQL_NTS, (SQLTCHAR *) data->schema, SQL_NTS, (SQLTCHAR *) data->table, SQL_NTS, (SQLTCHAR *) data->type, SQL_NTS);
  
	// this will be checked later in UV_AfterQuery
	data->result = ret; 
}

/*
 * Columns
 */
void ODBCConnection::Columns(const v8::FunctionCallbackInfo<v8::Value>& args) {
	v8::Isolate* isolate = args.GetIsolate();
	v8::EscapableHandleScope scope(isolate);

	if (args.Length() <= (0) || (!args[0]->IsString() && !args[0]->IsNull())) {
		isolate->ThrowException(Exception::TypeError(String::NewFromUtf8(isolate, "Argument 0 must be a string or null")));
		throw Exception::TypeError(String::NewFromUtf8(isolate, "Argument 0 must be a string or null"));
	}
	Local<String> catalog(args[0]->ToString());

	if (args.Length() <= (1) || (!args[1]->IsString() && !args[1]->IsNull())) {
		isolate->ThrowException(Exception::TypeError(String::NewFromUtf8(isolate, "Argument 1 must be a string or null")));
		throw Exception::TypeError(String::NewFromUtf8(isolate, "Argument 1 must be a string or null"));
	}
	Local<String> schema(args[1]->ToString());

	if (args.Length() <= (2) || (!args[2]->IsString() && !args[2]->IsNull())) {
		isolate->ThrowException(Exception::TypeError(String::NewFromUtf8(isolate, "Argument 2 must be a string or null")));
		throw Exception::TypeError(String::NewFromUtf8(isolate, "Argument 2 must be a string or null"));
	}
	Local<String> table(args[2]->ToString());

	if (args.Length() <= (3) || (!args[3]->IsString() && !args[3]->IsNull())) {
		isolate->ThrowException(Exception::TypeError(String::NewFromUtf8(isolate, "Argument 3 must be a string or null")));
		throw Exception::TypeError(String::NewFromUtf8(isolate, "Argument 3 must be a string or null"));
	}
	Local<String> column(args[3]->ToString());

	Local<Function> cb = Local<Function>::Cast(args[4]);
  
	ODBCConnection* conn = ObjectWrap::Unwrap<ODBCConnection>(args.Holder());
  
	uv_work_t* work_req = (uv_work_t *) (calloc(1, sizeof(uv_work_t)));
  
	query_work_data* data = (query_work_data *) calloc(1, sizeof(query_work_data));
  
	if (!data) {
		isolate->ThrowException(Exception::Error(String::NewFromUtf8(isolate, "Could not allocate enough memory")));
		throw Exception::TypeError(String::NewFromUtf8(isolate, "Could not allocate enough memory"));
	}

	data->sql = NULL;
	data->catalog = NULL;
	data->schema = NULL;
	data->table = NULL;
	data->type = NULL;
	data->column = NULL;
	v8::Persistent<v8::Function, CopyablePersistentTraits<v8::Function>> persistent(isolate, cb);
	data->cb = persistent;

	if (!catalog->Equals(String::NewFromUtf8(isolate, "null"))) {
#ifdef UNICODE
		data->catalog = (uint16_t *) malloc((catalog->Length() * sizeof(uint16_t)) + sizeof(uint16_t));
		catalog->Write((uint16_t *) data->catalog);
#else
		data->catalog = (char *) malloc(catalog->Length() + 1);
		catalog->WriteUtf8((char *) data->catalog);
#endif
	}

	if (!schema->Equals(String::NewFromUtf8(isolate, "null"))) {
#ifdef UNICODE
		data->schema = (uint16_t *) malloc((schema->Length() * sizeof(uint16_t)) + sizeof(uint16_t));
		schema->Write((uint16_t *) data->schema);
#else
		data->schema = (char *) malloc(schema->Length() + 1);
		schema->WriteUtf8((char *) data->schema);
#endif
	}
  
	if (!table->Equals(String::NewFromUtf8(isolate, "null"))) {
#ifdef UNICODE
		data->table = (uint16_t *) malloc((table->Length() * sizeof(uint16_t)) + sizeof(uint16_t));
		table->Write((uint16_t *) data->table);
#else
		data->table = (char *) malloc(table->Length() + 1);
		table->WriteUtf8((char *) data->table);
#endif
	}
  
	if (!column->Equals(String::NewFromUtf8(isolate, "null"))) {
#ifdef UNICODE
		data->column = (uint16_t *) malloc((column->Length() * sizeof(uint16_t)) + sizeof(uint16_t));
		column->Write((uint16_t *) data->column);
#else
		data->column = (char *) malloc(column->Length() + 1);
		column->WriteUtf8((char *) data->column);
#endif
	}
  
	data->conn = conn;
	work_req->data = data;
  
	uv_queue_work(uv_default_loop(), work_req, UV_Columns, (uv_after_work_cb)UV_AfterQuery);
  
	conn->Ref();

	args.GetReturnValue().SetUndefined();
}

void ODBCConnection::UV_Columns(uv_work_t* req) {
	query_work_data* data = (query_work_data *)(req->data);
  
	uv_mutex_lock(&ODBC::g_odbcMutex);
  
	SQLAllocHandle(SQL_HANDLE_STMT, data->conn->m_hDBC, &data->hSTMT );
  
	uv_mutex_unlock(&ODBC::g_odbcMutex);
  
	SQLRETURN ret = SQLColumns(data->hSTMT, (SQLTCHAR *) data->catalog, SQL_NTS, (SQLTCHAR *) data->schema, SQL_NTS, (SQLTCHAR *) data->table, SQL_NTS, (SQLTCHAR *) data->column, SQL_NTS);
  
	// this will be checked later in UV_AfterQuery
	data->result = ret;
}

/*
 * BeginTransactionSync
 * 
 */
void ODBCConnection::BeginTransactionSync(const v8::FunctionCallbackInfo<v8::Value>& args) {
	DEBUG_PRINTF("ODBCConnection::BeginTransactionSync\n");
	v8::Isolate* isolate = args.GetIsolate();
	v8::EscapableHandleScope scope(isolate);

	ODBCConnection* conn = ObjectWrap::Unwrap<ODBCConnection>(args.Holder());
  
	SQLRETURN ret;

	//set the connection manual commits
	ret = SQLSetConnectAttr(conn->m_hDBC, SQL_ATTR_AUTOCOMMIT, (SQLPOINTER) SQL_AUTOCOMMIT_OFF, SQL_NTS);
  
	if (!SQL_SUCCEEDED(ret)) {
		Local<Object> objError = ODBC::GetSQLError(SQL_HANDLE_DBC, conn->m_hDBC);
    
		isolate->ThrowException(objError);
		args.GetReturnValue().Set(False(isolate));
		throw objError;
	}

	args.GetReturnValue().Set(True(isolate));
}

/*
 * BeginTransaction
 * 
 */
void ODBCConnection::BeginTransaction(const v8::FunctionCallbackInfo<v8::Value>& args) {
	DEBUG_PRINTF("ODBCConnection::BeginTransaction\n");
	v8::Isolate* isolate = args.GetIsolate();
	v8::EscapableHandleScope scope(isolate);

	if (args.Length() <= (0) || !args[0]->IsFunction()) {
		isolate->ThrowException(Exception::TypeError(String::NewFromUtf8(isolate, "Argument 0 must be a function")));
		throw Exception::TypeError(String::NewFromUtf8(isolate, "Argument 2 must be a function"));
	}
	Local<Function> cb = Local<Function>::Cast(args[0]);

	ODBCConnection* conn = ObjectWrap::Unwrap<ODBCConnection>(args.Holder());
  
	uv_work_t* work_req = (uv_work_t *) (calloc(1, sizeof(uv_work_t)));
  
	query_work_data* data = (query_work_data *) calloc(1, sizeof(query_work_data));
  
	if (!data) {
		isolate->ThrowException(Exception::Error(String::NewFromUtf8(isolate, "Could not allocate enough memory")));
		throw Exception::TypeError(String::NewFromUtf8(isolate, "Could not allocate enough memory"));
	}

	v8::Persistent<v8::Function, CopyablePersistentTraits<v8::Function>> persistent(isolate, cb);
	data->cb = persistent;
	data->conn = conn;
	work_req->data = data;
  
	uv_queue_work(uv_default_loop(), work_req, UV_BeginTransaction, (uv_after_work_cb)UV_AfterBeginTransaction);

	args.GetReturnValue().SetUndefined();
}

/*
 * UV_BeginTransaction
 * 
 */
void ODBCConnection::UV_BeginTransaction(uv_work_t* req) {
	DEBUG_PRINTF("ODBCConnection::UV_BeginTransaction\n");
  
	query_work_data* data = (query_work_data *)(req->data);
  
	//set the connection manual commits
	data->result = SQLSetConnectAttr(data->conn->self()->m_hDBC, SQL_ATTR_AUTOCOMMIT, (SQLPOINTER) SQL_AUTOCOMMIT_OFF, SQL_NTS);
}

/*
 * UV_AfterBeginTransaction
 * 
 */
void ODBCConnection::UV_AfterBeginTransaction(uv_work_t* req, int status) {
	DEBUG_PRINTF("ODBCConnection::UV_AfterBeginTransaction\n");
	v8::Isolate* isolate = v8::Isolate::GetCurrent();
	v8::EscapableHandleScope scope(isolate);

	open_connection_work_data* data = (open_connection_work_data *)(req->data);
  
	Local<Value> argv[1];
  
	bool err = false;

	if (!SQL_SUCCEEDED(data->result)) {
		err = true;
		Local<Object> objError = ODBC::GetSQLError(SQL_HANDLE_DBC, data->conn->self()->m_hDBC);
		argv[0] = objError;
	}

	TryCatch try_catch;

	v8::Local<v8::Function> f = v8::Local<v8::Function>::New(isolate, data->cb);
	f->Call(isolate->GetCurrentContext()->Global(), err ? 1 : 0, argv);

	if (try_catch.HasCaught()) {
		FatalException(try_catch);
	}

	data->cb.Reset();
  
	free(data);
	free(req);
}

/*
 * EndTransactionSync
 * 
 */
void ODBCConnection::EndTransactionSync(const v8::FunctionCallbackInfo<v8::Value>& args) {
	DEBUG_PRINTF("ODBCConnection::EndTransactionSync\n");
	v8::Isolate* isolate = args.GetIsolate();
	v8::EscapableHandleScope scope(isolate);

	ODBCConnection* conn = ObjectWrap::Unwrap<ODBCConnection>(args.Holder());
  
	if (args.Length() <= (0) || !args[0]->IsBoolean()) {
		isolate->ThrowException(Exception::TypeError(String::NewFromUtf8(isolate, "Argument 0 must be a boolean")));
		throw Exception::TypeError(String::NewFromUtf8(isolate, "Argument 0 must be a boolean"));
	}
	Local<Boolean> rollback = (args[0]->ToBoolean());

	Local<Object> objError;
	SQLRETURN ret;
	bool error = false;
	SQLSMALLINT completionType = (rollback->Value()) ? SQL_ROLLBACK : SQL_COMMIT;
  
	//Call SQLEndTran
	ret = SQLEndTran(SQL_HANDLE_DBC, conn->m_hDBC, completionType);
  
	//check how the transaction went
	if (!SQL_SUCCEEDED(ret)) {
		error = true;
		objError = ODBC::GetSQLError(SQL_HANDLE_DBC, conn->m_hDBC);
	}
  
	//Reset the connection back to autocommit
	ret = SQLSetConnectAttr(conn->m_hDBC, SQL_ATTR_AUTOCOMMIT, (SQLPOINTER) SQL_AUTOCOMMIT_ON, SQL_NTS);
  
	//check how setting the connection attr went
	//but only process the code if an error has not already
	//occurred. If an error occurred during SQLEndTran,
	//that is the error that we want to throw.
	if (!SQL_SUCCEEDED(ret) && !error) {
		//TODO: if this also failed, we really should
		//be restarting the connection or something to deal with this state
		error = true;
    
		objError = ODBC::GetSQLError(SQL_HANDLE_DBC, conn->m_hDBC);
	}
  
	if (error) {
		isolate->ThrowException(objError);
		args.GetReturnValue().Set(False(isolate));
		throw objError;
	}
	else {
		args.GetReturnValue().Set(True(isolate));
	}
}

/*
 * EndTransaction
 * 
 */
void ODBCConnection::EndTransaction(const v8::FunctionCallbackInfo<v8::Value>& args) {
	DEBUG_PRINTF("ODBCConnection::EndTransaction\n");
	v8::Isolate* isolate = args.GetIsolate();
	v8::EscapableHandleScope scope(isolate);

	if (args.Length() <= (0) || !args[0]->IsBoolean()) {
		isolate->ThrowException(Exception::TypeError(String::NewFromUtf8(isolate, "Argument 0 must be a boolean")));
		throw Exception::TypeError(String::NewFromUtf8(isolate, "Argument 0 must be a boolean"));
	}
	Local<Boolean> rollback = (args[0]->ToBoolean());

	if (args.Length() <= (1) || !args[1]->IsFunction()) {
		isolate->ThrowException(Exception::TypeError(String::NewFromUtf8(isolate, "Argument 1 must be a function")));
		throw Exception::TypeError(String::NewFromUtf8(isolate, "Argument 1 must be a function"));
	}
	Local<Function> cb = Local<Function>::Cast(args[1]);

	ODBCConnection* conn = ObjectWrap::Unwrap<ODBCConnection>(args.Holder());
  
	uv_work_t* work_req = (uv_work_t *) (calloc(1, sizeof(uv_work_t)));
  
	query_work_data* data =  (query_work_data *) calloc(1, sizeof(query_work_data));
  
	if (!data) {
		isolate->ThrowException(Exception::Error(String::NewFromUtf8(isolate, "Could not allocate enough memory")));
		throw Exception::TypeError(String::NewFromUtf8(isolate, "Could not allocate enough memory"));
	}
  
	data->completionType = (rollback->Value()) ? SQL_ROLLBACK : SQL_COMMIT;

	v8::Persistent<v8::Function, CopyablePersistentTraits<v8::Function>> persistent(isolate, cb);
	data->cb = persistent;
	data->conn = conn;
	work_req->data = data;
  
	uv_queue_work(uv_default_loop(), work_req, UV_EndTransaction, (uv_after_work_cb)UV_AfterEndTransaction);

	args.GetReturnValue().SetUndefined();
}

/*
 * UV_EndTransaction
 * 
 */
void ODBCConnection::UV_EndTransaction(uv_work_t* req) {
	DEBUG_PRINTF("ODBCConnection::UV_EndTransaction\n");
  
	query_work_data* data = (query_work_data *)(req->data);
  
	bool err = false;
  
	//Call SQLEndTran
	SQLRETURN ret = SQLEndTran(SQL_HANDLE_DBC, data->conn->m_hDBC, data->completionType);
  
	data->result = ret;
  
	if (!SQL_SUCCEEDED(ret)) {
		err = true;
	}
  
	//Reset the connection back to autocommit
	ret = SQLSetConnectAttr(data->conn->m_hDBC, SQL_ATTR_AUTOCOMMIT, (SQLPOINTER) SQL_AUTOCOMMIT_ON, SQL_NTS);
  
	if (!SQL_SUCCEEDED(ret) && !err) {
		//there was not an earlier error,
		//so we shall pass the return code from
		//this last call.
		data->result = ret;
	}
}

/*
 * UV_AfterEndTransaction
 * 
 */
void ODBCConnection::UV_AfterEndTransaction(uv_work_t* req, int status) {
	DEBUG_PRINTF("ODBCConnection::UV_AfterEndTransaction\n");
	v8::Isolate* isolate = v8::Isolate::GetCurrent();
	v8::EscapableHandleScope scope(isolate);

	open_connection_work_data* data = (open_connection_work_data *)(req->data);
  
	Local<Value> argv[1];
  
	bool err = false;

	if (!SQL_SUCCEEDED(data->result)) {
		err = true;
		Local<Object> objError = ODBC::GetSQLError(SQL_HANDLE_DBC, data->conn->self()->m_hDBC);
		argv[0] = objError;
	}

	TryCatch try_catch;

	v8::Local<v8::Function> f = v8::Local<v8::Function>::New(isolate, data->cb);
	f->Call(isolate->GetCurrentContext()->Global(), err ? 1 : 0, argv);

	if (try_catch.HasCaught()) {
		FatalException(try_catch);
	}

	data->cb.Reset();
  
	free(data);
	free(req);
}
