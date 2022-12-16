import { DatabasePoolPlus } from '@/pool';
import { DatabaseTransactionConnection, QueryResult, QuerySqlToken, sql, SqlFragment } from 'slonik';
import { DbRecord } from '@/db-record';
import { OneOrMore } from '@/utils/general';


/* ****************************************************************************************************************** */
// region: Types
/* ****************************************************************************************************************** */

export interface DeleteQueryOptions {
  startingQuery?: QuerySqlToken
  finalQuery?: QuerySqlToken
  transaction?: DatabaseTransactionConnection
  /**
   * Override table name
   */
  tableName?: string
  /**
   * Override where clause
   */
  whereClause?: SqlFragment
}

// endregion


/* ****************************************************************************************************************** */
// region: Utils
/* ****************************************************************************************************************** */

export async function dbDeleteWorker(
  this: DatabasePoolPlus,
  recordOrCtor: typeof DbRecord | OneOrMore<DbRecord>,
  opt?: Partial<DeleteQueryOptions>
): Promise<QueryResult<any>[]>
{
  const options = { ...opt };

  /* Determine input type */
  let record: OneOrMore<DbRecord> | undefined = void 0;
  let ctor: typeof DbRecord | undefined;

  if (DbRecord.isPrototypeOf(recordOrCtor)) ctor = <typeof DbRecord>recordOrCtor;
  else {
    record = [ <OneOrMore<DbRecord>>recordOrCtor ].flat();
    ctor = record[0]?.constructor as typeof DbRecord;
  }

  /* Validate */
  if (record && !record.length)
    throw new Error(`No records provided for DB Delete! Check the length of your array before passing to DB.`);

  if (!record && !options.whereClause)
    throw new Error(
      `Cannot delete from table without a where clause. If you're sure you want to delete all records, ` +
      `supply an empty whereClause property (ie. sql\`\`)`
    );

  const tableName = options.tableName || ctor.getConfig().table;
  if (!tableName) throw new Error(`Cannot perform insert/upsert for ${ctor.name}. No table specified!`);

  /* Construct queries */
  let queries: QuerySqlToken[] = [];
  if (!options.whereClause) {
    const { deleteChunkMax } = this.config;
    for (let i = 0; i < record!.length; i += deleteChunkMax) {
      const nextI = i + deleteChunkMax;
      const whereClauseSql = record!.slice(i, nextI).map(r => sql.fragment`(${r.getWhereClause(true)!})`);

      queries.push(sql.unsafe`
        DELETE FROM ${sql.identifier(tableName.split('.'))}
        WHERE ${sql.join(whereClauseSql, sql.fragment` OR `)};
      `);
    }
  } else {
    queries.push(sql.unsafe`
      DELETE FROM ${sql.identifier(tableName.split('.'))}
      WHERE ${options.whereClause};
    `);
  }

  /* Query */
  const res = await this.performQueries(queries, options.startingQuery, options.finalQuery, options.transaction);
  record?.forEach(rec => rec.saveState());

  return res;
}

// endregion
