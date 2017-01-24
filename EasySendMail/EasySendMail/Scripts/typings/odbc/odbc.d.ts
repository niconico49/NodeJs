declare module "odbc" {
    //export = Database;

    //import bindings = require("bindings");
    //import SimpleQueue = require("simplequeue");
    //import util = require("util");

    //function odbc = bindings.bindings("odbc_bindings");

    //module.exports = function (options) {
    //    return new Database(options);
    //}
    export class Database {
        debug: boolean;
        constructor(options?: any);
        //<T>() : T;
        //open(connectionString: string, options: any, callback?: (error: any) => void): any;
        open(connectionString: string, callback: (error: any) => void): any;
        openSync(connectionString: string): any;
        query(sqlQuery: string, bindingParameters?: any, callback?: (error: any, rows: any, moreResultSets: any) => void): any;
        querySync(sqlQuery: string, bindingParameters?: any): any;
        close(callback?: (error: any) => void): any;
        closeSync(): any;
        prepare(sql: any, callback: (error: any, stmt: ODBCStatement) => void): any;
        prepareSync(sql: any): ODBCStatement;
        beginTransaction(callback: (error: any) => void): any;
        beginTransactionSync(): any;
        commitTransaction(callback: (error: any) => void): any;
        commitTransactionSync(): any;
        rollbackTransaction(callback: (error: any) => void): any;
        rollbackTransactionSync(): any;

        queryResult(sql: string, params: any, callback: (error: any, result: ODBCResult) => void): any;
        queryResultSync(sql: string, params: any): ODBCResult;
        endTransactionSync(rollback: any): any;
        columns(catalog: any, schema: any, table: any, column: any, callback: (error: any, data: any, other: boolean) => void): any;
        tables(catalog: any, schema: any, table: any, type: any, callback: (error: any, data: any, other: boolean) => void): any;
        describe(obj: any, callback: (error: any, data: any, other: boolean) => void): any;
    }
    //export interface ODBC {
    //}
    export interface ODBCStatement {
        execute(params: any, callback: (error: any, result: ODBCResult) => void): any;
        executeDirect(sql: string, callback: (error: any, result: ODBCResult) => void): any;
        executeNonQuery(params: any, callback: (error: any, result: ODBCResult) => void): any;
        prepare(sql: string, callback: (error: any) => void): any;
        bind(ary: any, callback: (error: any) => void): any;
    }
    //export interface ODBCConnection {
    //}
    export interface ODBCResult {
        fetch(callback: (error: any, data: any) => void): any;
        fetch(options: any, callback: (error: any, data: any) => void): any;
        fetchAll(callback: (error: any, result: any) => void): any;
        fetchAll(options: any, callback: (error: any, result: any) => void): any;
        fetchSync(options?: any): any;
        fetchAllSync(options?: any): any;
        //getColumnNamesSync(callback: (error: any, data: any) => void): any;
        getColumnNamesSync(): any;
    }
    //export interface loadODBCLibrary {
    //}
    export interface Pool {
        count: number;
        constructor(options: any);
        open(connectionString: string, callback: (error: any, db : any) => void): any;
        close(callback: (error: any) => void): any;
    }

    //Expose constants
    //Object.keys(odbc.ODBC).forEach(function (key) {
    //    if (typeof odbc.ODBC[key] !== "function") {
    //        //On the database prototype
    //        Database.prototype[key] = odbc.ODBC[key];
    
    //        //On the exports
    //        module.exports[key] = odbc.ODBC[key];
    //    }
    //});
}