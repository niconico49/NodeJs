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

#ifndef _SRC_ODBC_RESULT_H
#define _SRC_ODBC_RESULT_H

class ODBCResult : public node::ObjectWrap {
  public:
   static Persistent<String> OPTION_FETCH_MODE;
   static Persistent<FunctionTemplate> constructor_template;
   static void Init(v8::Handle<Object> target);
   
   void Free();
   
  protected:
    ODBCResult() {};
    
    explicit ODBCResult(HENV hENV, HDBC hDBC, HSTMT hSTMT, bool canFreeHandle): 
      ObjectWrap(),
      m_hENV(hENV),
      m_hDBC(hDBC),
      m_hSTMT(hSTMT),
      m_canFreeHandle(canFreeHandle) {};
     
    ~ODBCResult();

    //constructor
	static void New(const v8::FunctionCallbackInfo<v8::Value>& info);

    //async methods
	static void Fetch(const v8::FunctionCallbackInfo<v8::Value>& info);
    static void UV_Fetch(uv_work_t* work_req);
    static void UV_AfterFetch(uv_work_t* work_req, int status);

	static void FetchAll(const v8::FunctionCallbackInfo<v8::Value>& info);
    static void UV_FetchAll(uv_work_t* work_req);
    static void UV_AfterFetchAll(uv_work_t* work_req, int status);
    
    //sync methods
	static void CloseSync(const v8::FunctionCallbackInfo<v8::Value>& info);
	static void MoreResultsSync(const v8::FunctionCallbackInfo<v8::Value>& info);
	static void FetchSync(const v8::FunctionCallbackInfo<v8::Value>& info);
	static void FetchAllSync(const v8::FunctionCallbackInfo<v8::Value>& info);
	static void GetColumnNamesSync(const v8::FunctionCallbackInfo<v8::Value>& info);
    
    //property getter/setters
	static void FetchModeGetter(Local<String> property, const PropertyCallbackInfo<Value>& info);
	static void FetchModeSetter(Local<String> property, Local<Value> value, const PropertyCallbackInfo<Value>& info);
    
    struct fetch_work_data {
	  Persistent<Function, CopyablePersistentTraits<v8::Function>> cb;
      ODBCResult *objResult;
      SQLRETURN result;
      
      int fetchMode;
      int count;
      int errorCount;
	  Persistent<Array, CopyablePersistentTraits<v8::Array>> rows;
	  Persistent<Object, CopyablePersistentTraits<v8::Object>> objError;
    };
    
    ODBCResult *self(void) { return this; }

  protected:
    HENV m_hENV;
    HDBC m_hDBC;
    HSTMT m_hSTMT;
    bool m_canFreeHandle;
    int m_fetchMode;
    
    uint16_t *buffer;
    int bufferLength;
    Column *columns;
    short colCount;
};



#endif
