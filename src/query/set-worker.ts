/**
 * Perform insert, upsert, or update
 */
import {
  DatabaseTransactionConnection, FragmentSqlToken, IdentifierSqlToken, ListSqlToken, QueryResult, QuerySqlToken, sql,
  SqlFragment
} from 'slonik';
import { DatabasePoolPlus } from '@/pool';
import { ColumnDetail, DbRecord } from '@/db-record';
import { NoColumnsToUpdateError } from '@/errors';
import { raw as sqlRaw } from 'slonik-sql-tag-raw'
import { isValidPostgresIdentifier, OneOrMore } from '@/utils/general';


/* ****************************************************************************************************************** */
// region: Locals
/* ****************************************************************************************************************** */

interface PrepareQueryOptions {
  keyIdentifiers: ListSqlToken;
  updatableKeyIdentifiers: ListSqlToken;
  tableIdentifier: IdentifierSqlToken;
  isResultTypeQuery: boolean;
  upsertStatement?: FragmentSqlToken;
  updateSetClauseTokens?: FragmentSqlToken[];
  updateWhereClauseTokens?: FragmentSqlToken[];
  fromAlias: FragmentSqlToken;
  primaryKeyColumns: ListSqlToken;
  selectColumns: ListSqlToken;
}

// endregion


/* ****************************************************************************************************************** */
// region: Types
/* ****************************************************************************************************************** */

export enum SetQueryResultType {
  /**
   * Do not return result
   */
  None,
  /**
   * Return the full updated record
   */
  Record,
  /**
   * Return primary keys of each records
   */
  PrimaryKeys
}

export interface SetQueryOptions<T extends typeof DbRecord | unknown = unknown> {
  startingQuery?: QuerySqlToken
  finalQuery?: QuerySqlToken
  transaction?: DatabaseTransactionConnection
  /**
   * On conflict keys (for upsert)
   */
  conflictKey?: OneOrMore<string>
  /**
   * Columns to update
   * @default For insert/upsert: 'all' - For 'update': determines columns for what has been changed
   */
  columns?: 'all' | (T extends DbRecord ? Exclude<keyof T, keyof DbRecord>[] : string[])
  /**
   * Override table name
   */
  tableName?: string
  /**
   * Alter SQL statement (useful for wrapping WITH statements, etc)
   */
  alterSql?: (query: QuerySqlToken) => QuerySqlToken
  /**
   * Sync DBRecord objects with DB values after updating / inserting
   * Note: This sets resultType to Record by default, but you can can manually override it to PrimaryKeys
   */
  syncRecords?: boolean
  /**
   * Return detail on inserted/updated records (Note: this can be slower)
   * @default SetQueryResultType.None
   */
  resultType?: SetQueryResultType
}

// endregion


/* ****************************************************************************************************************** *
 * dbSetWorker (util) - Worker Utility that performs Update, Insert, Upsert
 * ****************************************************************************************************************** */

// @formatter:off
export async function dbSetWorker<TOptions extends Partial<SetQueryOptions>>(
  this: DatabasePoolPlus,
  record: DbRecord | DbRecord[],
  mode: 'insert' | 'upsert' | 'update',
  options?: TOptions
): Promise<
  TOptions['syncRecords'] extends SetQueryResultType.Record | SetQueryResultType.PrimaryKeys
     ? QueryResult<{ [p:string]: any, __dbRecordIndex__: number }>[]
     : QueryResult<any>[]
  >
// @formatter:on

export async function dbSetWorker(
  this: DatabasePoolPlus,
  record: DbRecord | DbRecord[],
  mode: 'insert' | 'upsert' | 'update',
  opt?: Partial<SetQueryOptions>
): Promise<QueryResult<any>[]> {
  const options = { ...opt };
  const config = this.config;

  options.resultType ??= SetQueryResultType.None;

  if (options.syncRecords && options.resultType === SetQueryResultType.None)
    options.resultType = SetQueryResultType.Record

  record = [ record ].flat();

  /* Validate */
  if (!record.length)
    throw new Error(`No records provided for DB ${mode}! Check the length of your array before passing to DB.`);

  if (options.alterSql && options.syncRecords)
    throw new Error(`Cannot pass both alterSql and syncRecords, as syncRecords relies on using WITH`);

  const ctor = record[0].constructor as typeof DbRecord;
  const tableName = options.tableName || ctor.getConfig().table;
  if (!tableName) throw new Error(`Cannot perform insert/upsert for ${ctor.name}. No table specified!`);

  /* Determine usable column info */
  const primaryKeys = ctor.getPrimaryKeys();
  const primaryKeyColumns = ctor.getSelectColumns().filter(c => primaryKeys.includes(c.dbName!));
  const knownKeys = new Set<string>();

  /* Validate individual records & add known keys */
  for (const rec of record) {
    if (rec.constructor !== ctor) throw new Error(`All DB Objects must be the same type for bulk ${mode}`);
    Object.keys(rec).forEach(key => key !== '_original' && knownKeys.add(key));
  }

  /* Get columns to update & key mapping info (name -> dbName) + column transformers */
  const { updatableColumns, insertableColumns } = getColumns(record);
  const keyMap = new Map(insertableColumns.map(({ name, dbName }) => [ name, dbName ]));
  const updatableKeyMap = new Map(updatableColumns.map(({ name, dbName }) => [ name, dbName ]));
  const transformerMap = new Map(insertableColumns.map(({ name, config }) => [ name, config.out ]));

  if ((mode === 'update' && !updatableColumns.length) || !insertableColumns.length)
    throw new NoColumnsToUpdateError(`Cannot ${mode} data for ${ctor.name}. No columns need to be updated!`);

  // Create output objects and column definition list
  const { colDefinitionList, outputObjects } = createOutputObjects(record);

  const queries = createQueries();

  /* Perform queries */
  const res = await this.performQueries(queries, options.startingQuery, options.finalQuery, options.transaction);

  if (options.resultType !== SetQueryResultType.None)
    updateDbRecordObjects(<QueryResult<{ [p:string]: any, __dbRecordIndex__: number }>[]>res);

  record.forEach(rec => rec.saveState());

  return res;

  /* ********************************************************* *
   * Helpers
   * ********************************************************* */

  function updateDbRecordObjects(results: QueryResult<{ [p:string]: any, __dbRecordIndex__: number }>[]) {
    const { resultType } = options!;
    const selectColumns = ctor.getSelectColumns();

    for (const result of results) {
      for (const row of result.rows) {
        const dbRecordObject = (<DbRecord[]>record)[row.__dbRecordIndex__];
        const columnSet = resultType === SetQueryResultType.PrimaryKeys
                          ? primaryKeyColumns
                          : selectColumns;


        const columnData: any = {};
        for (const col of columnSet) {
          let [ dbKey, objKey ] = [ col.dbName, col.name ];
          if (!row.hasOwnProperty(dbKey)) return;
          if (col.config.in === false) return; // Don't update columns configured to exclude in

          const value = row[dbKey];
          const transformer = col.config.in;
          columnData[objKey] = (value != null && typeof transformer === 'function') ? transformer(value) : value;
        }

        Object.assign(dbRecordObject, columnData);
      }
    }
  }

  function createQueries(): QuerySqlToken[] {
    let res: QuerySqlToken[] = [];
    const addQuery = (q: QuerySqlToken) => res.push(options?.alterSql ? options.alterSql(q) : q);

    const isResultTypeQuery = options!.resultType !== SetQueryResultType.None;
    const fromAlias = sql.fragment`__source__`;

    const prepareQueryOptions: PrepareQueryOptions = {
      fromAlias,
      isResultTypeQuery,
      primaryKeyColumns: sql.join(primaryKeyColumns.map(pk => pk.dbName), sql.fragment`, `),
      selectColumns: sql.join(ctor.getSelectColumns().map(pk => pk.dbName), sql.fragment`, `),
      keyIdentifiers: sql.join([ ...keyMap.values() ].map(k => sql.identifier([ k ])), sql.fragment`, `),
      updatableKeyIdentifiers:
        sql.join([ ...updatableKeyMap.values() ].map(k => sql.identifier([ k ])), sql.fragment`, `),
      tableIdentifier: sql.identifier(tableName!.split('.'))
    };

    /* Prepare options for insert / upsert */
    if ((mode === 'insert') || (mode === 'upsert')) {
      const conflictKeys = options!.conflictKey ? [ options!.conflictKey ].flat() : primaryKeys;

      /* Prepare upsert clause */
      let upsertStatement: FragmentSqlToken = sql.fragment``;
      if (mode === 'upsert') {
        const parenthesizeIfMultipleKeys = (map: Map<any, any>, s: QuerySqlToken | ListSqlToken) =>
          map.size < 2 ? s : sql.fragment`(${s})`;

        if (conflictKeys.length) {
          const updateSetValues = sql.join(
            [ ...updatableKeyMap.values() ].map(k => sql.fragment`EXCLUDED.${sql.identifier([ k ])}`),
            sql.fragment`, `
          );
          upsertStatement = sql.fragment`
            ON CONFLICT (${sql.join(conflictKeys.map(k => sql.identifier([ k ])), sql.fragment`, `)}) DO
            UPDATE SET
              ${parenthesizeIfMultipleKeys(keyMap, prepareQueryOptions.updatableKeyIdentifiers)} =
              ${parenthesizeIfMultipleKeys(updatableKeyMap, updateSetValues)}
          `;
        }
      }

      prepareQueryOptions.upsertStatement = upsertStatement;
    }
    /*  Prepare options for update */
    else if (mode === 'update') {
      if (!primaryKeys.length) throw new Error(`Cannot update ${ctor.name}. No primary keys specified for record.`);

      prepareQueryOptions.updateSetClauseTokens = [ ...updatableKeyMap.values() ].map(k => {
        const id = sql.identifier([ k ]);
        return sql.fragment`${id} = ${fromAlias}.${id}`
      });

      prepareQueryOptions.updateWhereClauseTokens = primaryKeys.map(k => sql.fragment`
        __update_table__.${sql.identifier([ k ])} = ${fromAlias}.${sql.identifier([ `original_${k}` ])}
      `);
    }

    /* Construct queries */
    const { insertChunkMax } = config;
    for (let i = 0; i < outputObjects.length; i += insertChunkMax) {
      const nextI = i + insertChunkMax;
      const objectBlock = outputObjects.slice(i, nextI);

      addQuery(prepareQuery(objectBlock, prepareQueryOptions));
    }

    return res;
  }

  function prepareQuery(
    objectBlock: any[],
    prepareQueryOptions: PrepareQueryOptions
  ): QuerySqlToken {
    const {
      keyIdentifiers,
      updateSetClauseTokens,
      upsertStatement,
      updateWhereClauseTokens,
      isResultTypeQuery,
      tableIdentifier,
      fromAlias
    } = prepareQueryOptions;

    const dbRecordsJsonbSql = sql.fragment`
      jsonb_to_recordset(${sql.jsonb(objectBlock)}) AS ${!isResultTypeQuery ? fromAlias : sql.fragment`__db_records__`}
      (${sql.join(colDefinitionList, sql.fragment`, `)})
    `;

    let returningClause = sql.fragment``;
    const returningColumns =
      !isResultTypeQuery ? void 0 :
      (options!.resultType === SetQueryResultType.Record) ? ctor.getSelectColumns() :
      primaryKeyColumns;

    let setSql: QuerySqlToken;
    switch (mode) {
      case 'insert':
      case 'upsert':
        if (isResultTypeQuery)
          returningClause = sql.fragment`
            RETURNING ${sql.join(
              returningColumns!.map(c => sql.fragment`${sql.identifier([ c.dbName ])}`),
              sql.fragment`, `
            )}
          `;
        setSql = sql.unsafe`
          INSERT INTO ${tableIdentifier} (${keyIdentifiers})
          SELECT ${keyIdentifiers}
          FROM ${!isResultTypeQuery
                 ? dbRecordsJsonbSql
                 : sql.fragment`__db_records__ as ${fromAlias} ORDER BY ${fromAlias}."__row_number__"`
          }
          ${upsertStatement!}
          ${returningClause}
        `;
        break;
      case 'update':
        if (isResultTypeQuery)
          returningClause = sql.fragment`
            RETURNING ${sql.join(
              returningColumns!.map(c => sql.fragment`__update_table__.${sql.identifier([ c.dbName ])}`),
              sql.fragment`, `
            )}
          `;
        setSql = sql.unsafe`
          UPDATE ${tableIdentifier} __update_table__
          SET ${sql.join(updateSetClauseTokens!, sql.fragment`, `)}
          FROM ${!isResultTypeQuery
                 ? dbRecordsJsonbSql
                 : sql.fragment`(SELECT * FROM __db_records__ ORDER BY __db_records__."__row_number__") ${fromAlias}`
          }
          WHERE ${sql.join(updateWhereClauseTokens!, sql.fragment` AND `)}
          ${returningClause}
        `;
    }

    if (!isResultTypeQuery) return setSql;

    // noinspection SqlResolve
    return sql.unsafe`
      WITH __db_records__ AS (
        SELECT
          __db_records__.*,
          row_number() OVER(ORDER BY __db_records__."__dbRecordIndex__") as __row_number__
        FROM ${dbRecordsJsonbSql}
      ),
      set_res as (${setSql}),
      new_records as (select set_res.*, row_number() over() as __row_number__ from set_res)
      SELECT
        rec."__dbRecordIndex__",
        new_records.*
      FROM __db_records__ rec
      INNER JOIN new_records USING(__row_number__)
    `;
  }

  function getColumns(records: DbRecord[]): { insertableColumns: ColumnDetail[], updatableColumns: ColumnDetail[] } {
    let columns = ctor.getInsertColumns().filter(c => knownKeys.has(c.name));

    /* Use specified, if provided */
    if (Array.isArray(options!.columns)) columns = columns.filter(c => options!.columns!.includes(c.name));
    /* Otherwise, if update, determine based on what has changed */
    else if (mode === 'update') {
      const allChangedNames = new Set<string>();
      for (const rec of records) rec.getChangedColumns().forEach(c => allChangedNames.add(c.name));

      columns = columns.filter(c => allChangedNames.has(c.name));
    }

    const updatableColumns = columns.filter(c => !c.config.excludeFromUpdate);

    return { insertableColumns: columns, updatableColumns };
  }

  function createOutputObjects(records: DbRecord[]) {
    const validateName = (s: string) => {
      if (!isValidPostgresIdentifier(s)) throw new Error(`Invalid Postgres identifier: ${s} for ${ctor.name}`);
      return s;
    }

    /* Setup output columns */
    const outputColumns = new Map(insertableColumns.map(c => [ c.name, c ]));
    if (((mode === 'upsert') && (!options!.conflictKey)) || (mode === 'update'))
      primaryKeyColumns.forEach(pkCol => outputColumns.set(pkCol.name, pkCol));

    /* Create json object type definition SQL for all output columns */
    const colDefinitionList: SqlFragment[] = [];
    for (const { dbName, config } of outputColumns.values()) {
      validateName(dbName);
      colDefinitionList.push(sqlRaw(`"${dbName}" ${config.type}`));

      // If update mode, primary key may have changed. In order to make WHERE clause work, we include the original value
      if (mode === 'update' && config.primaryKey !== undefined) colDefinitionList.push(sqlRaw(`"original_${dbName}" ${config.type}`));
    }

    colDefinitionList.push(sqlRaw(`"__dbRecordIndex__" int4`));

    /* Create json objects with normalized db names and values */
    const outputObjects = records.map((rec, recordIndex) => {
      const res: any = {};
      for (const { name: recordKey, dbName: dbKey, config } of outputColumns.values()) {
        const isPrimaryKey = config.primaryKey !== undefined;
        const value = (<any>rec)[recordKey] ?? (knownKeys.has(recordKey) ? null : void 0);

        /* Transform if applicable transformer is defined */
        const transformer = transformerMap.get(recordKey);
        res[dbKey] = (value != null && typeof transformer === 'function') ? transformer(value) : value;

        // If update mode, primary key may have changed. In order to make WHERE clause work, we include the original value
        if ((mode === 'update') && isPrimaryKey) res[`original_${dbKey}`] = (rec._original || rec)[recordKey];
      }

      return Object.assign(res, { __dbRecordIndex__: recordIndex });
    });

    return { colDefinitionList, outputObjects };
  }
}
