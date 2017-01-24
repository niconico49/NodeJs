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

#ifndef _SRC_ODBC_STATEMENT_H
#define _SRC_ODBC_STATEMENT_H

class ODBCStatement : public node::ObjectWrap {
  public:
   static Persistent<FunctionTemplate> constructor_template;
   static void Init(v8::Handle<Object> target);
   
   void Free();
   
  protected:
    ODBCStatement() {};
    
    explicit ODBCStatement(HENV hENV, HDBC hDBC, HSTMT hSTMT): 
      ObjectWrap(),
      m_hENV(hENV),
      m_hDBC(hDBC),
      m_hSTMT(hSTMT) {};
     
    ~ODBCStatement();

    //constructor
	static void New(const v8::FunctionCallbackInfo<v8::Value>& info);

    //async methods
	static void Execute(const v8::FunctionCallbackInfo<v8::Value>& info);
    static void UV_Execute(uv_work_t* work_req);
    static void UV_AfterExecute(uv_work_t* work_req, int status);

	static void ExecuteDirect(const v8::FunctionCallbackInfo<v8::Value>& info);
    static void UV_ExecuteDirect(uv_work_t* work_req);
    static void UV_AfterExecuteDirect(uv_work_t* work_req, int status);

	static void ExecuteNonQuery(const v8::FunctionCallbackInfo<v8::Value>& info);
    static void UV_ExecuteNonQuery(uv_work_t* work_req);
    static void UV_AfterExecuteNonQuery(uv_work_t* work_req, int status);
    
	static void Prepare(const v8::FunctionCallbackInfo<v8::Value>& info);
    static void UV_Prepare(uv_work_t* work_req);
    static void UV_AfterPrepare(uv_work_t* work_req, int status);
    
	static void Bind(const v8::FunctionCallbackInfo<v8::Value>& info);
    static void UV_Bind(uv_work_t* work_req);
    static void UV_AfterBind(uv_work_t* work_req, int status);
    
    //sync methods
	static void CloseSync(const v8::FunctionCallbackInfo<v8::Value>& info);
	static void ExecuteSync(const v8::FunctionCallbackInfo<v8::Value>& info);
	static void ExecuteDirectSync(const v8::FunctionCallbackInfo<v8::Value>& info);
	static void ExecuteNonQuerySync(const v8::FunctionCallbackInfo<v8::Value>& info);
	static void PrepareSync(const v8::FunctionCallbackInfo<v8::Value>& info);
	static void BindSync(const v8::FunctionCallbackInfo<v8::Value>& info);
    
    struct Fetch_Request {
		Persistent<Function, CopyablePersistentTraits<v8::Function>> callback;
      ODBCStatement *objResult;
      SQLRETURN result;
    };
    
    ODBCStatement *self(void) { return this; }

  protected:
    HENV m_hENV;
    HDBC m_hDBC;
    HSTMT m_hSTMT;
    
    Parameter *params;
    int paramCount;
    
    uint16_t *buffer;
    int bufferLength;
    Column *columns;
    short colCount;
};

struct execute_direct_work_data {
	Persistent<Function, CopyablePersistentTraits<v8::Function>> cb;
  ODBCStatement *stmt;
  int result;
  void *sql;
  int sqlLen;
};

struct execute_work_data {
	Persistent<Function, CopyablePersistentTraits<v8::Function>> cb;
  ODBCStatement *stmt;
  int result;
};

struct prepare_work_data {
	Persistent<Function, CopyablePersistentTraits<v8::Function>> cb;
  ODBCStatement *stmt;
  int result;
  void *sql;
  int sqlLen;
};

struct bind_work_data {
	Persistent<Function, CopyablePersistentTraits<v8::Function>> cb;
  ODBCStatement *stmt;
  int result;
};

#endif
