declare module "squel" {
    export function expr(): Expression;
    export function select(): Select;
    export function update(options?: DefaultUpdateBuilderOptions): Update;
    export function insert(options?: DefaultInsertBuilderOptions): Insert;
    export function remove(): Delete;
    //export function delete: Delete;

    interface DefaultBuilderOptions {
        usingValuePlaceholders: boolean;
    }

    export interface DefaultUpdateBuilderOptions extends DefaultBuilderOptions {

    }

    export interface DefaultInsertBuilderOptions extends DefaultBuilderOptions {

    }

    export interface Cloneable {
        clone(): any;
    }
    export interface Expression {
        and_begin(): Expression;
        or_begin(): Expression;
        end(): Expression;
        and(expr: string): Expression;
        and(expr: Expression): Expression;
        or(expr: string): Expression;
        or(expr: Expression): Expression;
        toString(): string;
    }
    export interface QueryBuilder extends Cloneable {

    }
    export interface WhereOrderLimit extends QueryBuilder {
        where(condition: string): WhereOrderLimit;
        where(condition: Expression): WhereOrderLimit;
        order(field: string, asc?: boolean): WhereOrderLimit;
        limit(max: number): WhereOrderLimit;
    }
    export interface JoinWhereOrderLimit extends WhereOrderLimit {
        join(table: string, alias?: string, condition?: string, type?: string): JoinWhereOrderLimit;
        join(table: string, alias?: string, condition?: Expression, type?: string): JoinWhereOrderLimit;

        left_join(table: string, alias?: string, condition?: string): JoinWhereOrderLimit;
        right_join(table: string, alias?: string, condition?: string): JoinWhereOrderLimit;
        outer_join(table: string, alias?: string, condition?: string): JoinWhereOrderLimit;

        left_join(table: string, alias?: string, condition?: Expression): JoinWhereOrderLimit;
        right_join(table: string, alias?: string, condition?: Expression): JoinWhereOrderLimit;
        outer_join(table: string, alias?: string, condition?: Expression): JoinWhereOrderLimit;

        // Inherited from WhereOrderLimit
        // Change return type for Squels fluent interface (no other way possible in TypeScript?)
        where(condition: string): JoinWhereOrderLimit;
        where(condition: Expression): JoinWhereOrderLimit;
        order(field: string, asc?: boolean): JoinWhereOrderLimit;
        limit(max: number): JoinWhereOrderLimit;
    }
    export interface Select extends JoinWhereOrderLimit {
        distinct(): Select;
        from(table: string, alias?: string): Select;
        field(field: string, alias?: string): Select;
        group(field: string): Select;
        offset(start: number): Select;
        toString(): string;

    }
    export interface Update extends JoinWhereOrderLimit {
        table(table: string, alias?: string): Update;
        set(field: string, value: string): Update;
        set(field: string, value: number): Update;
        set(field: string, value: boolean): Update;
        toString(): string;
    }
    export interface Delete extends JoinWhereOrderLimit {
        from(table: string, alias?: string): Delete;
        toString(): string;
    }
    export interface Insert extends QueryBuilder {
        into(table: string): Insert;
        set(field: string, value: string): Insert;
        set(field: string, value: number): Insert;
        set(field: string, value: boolean): Insert;
        toString(): string;
    }
}
