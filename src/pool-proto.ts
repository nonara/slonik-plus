import { DbRecord } from "@/db-record";
import { DatabasePool, DatabaseTransactionConnection, QueryResult, QuerySqlToken } from "slonik";
import { dbSetWorker, SetQueryOptions } from "@/query/set-worker";
import { dbDeleteWorker, DeleteQueryOptions } from "@/query/delete-worker";
import { OneOrMore } from '@/utils/general';


/* ****************************************************************************************************************** */
// region: DatabasePoolPlus (class)
/* ****************************************************************************************************************** */

export abstract class DatabasePoolPlusBase {
  /**
   * Perform multiple SQL queries as transaction (and optionally run a stats timer)
   */
  async performQueries(
    queries: QuerySqlToken[],
    startingQuery?: QuerySqlToken,
    finalQuery?: QuerySqlToken,
    transaction?: DatabaseTransactionConnection
  ): Promise<QueryResult<any>[]> {
    return await (transaction
      ? handleTransaction(transaction)
      : (this as unknown as DatabasePool).transaction(handleTransaction));

    async function handleTransaction(t: DatabaseTransactionConnection): Promise<QueryResult<any>[]> {
      let res: QueryResult<Record<string, any>>[] = [];
      if (startingQuery) res.push(await t.query(startingQuery));

      /* Add a task for each query to the queue */
      // TODO - The private code this was ported from used multiple connections for speed. It could easily be
      //  expanded to do so again at some point
      for (const query of queries) res.push(await t.query<any>(query));

      if (finalQuery) res.push(await t.query(finalQuery));

      return res;
    }
  }

  /**
   * Insert one or more DbRecords
   */
  async insert<T extends DbRecord, TOptions extends Partial<Omit<SetQueryOptions<T>, 'conflictKey'>>>
  (dbRecord: T | T[], options?: TOptions) {
    return await dbSetWorker.call(this as any, dbRecord, 'insert', options);
  }

  /**
   * Upsert one or more DbRecords
   */
  async upsert<T extends DbRecord, TOptions extends Partial<SetQueryOptions<T>>>(dbRecord: T | T[], options?: TOptions) {
    return await dbSetWorker.call(this as any, dbRecord, 'upsert', options);
  }

  /**
   * Upsert one or more DbRecords
   */
  async update<T extends DbRecord, TOptions extends Partial<Omit<SetQueryOptions<T>, 'conflictKey'>>>
  (dbRecord: T | T[], options?: TOptions) {
    return await dbSetWorker.call(this as any, dbRecord, 'update', options);
  }

  /**
   * Delete one or more DbRecords
   */
  async delete(ctor: typeof DbRecord, options?: Partial<DeleteQueryOptions>): ReturnType<typeof dbDeleteWorker>
  async delete<T extends DbRecord>(dbRecord: T | T[], options?: Partial<DeleteQueryOptions>):
    ReturnType<typeof dbDeleteWorker>
  async delete(dbRecordOrCtor: OneOrMore<DbRecord> | typeof DbRecord, options?: Partial<DeleteQueryOptions>):
    ReturnType<typeof dbDeleteWorker>
  {
    return await dbDeleteWorker.call(this as any, dbRecordOrCtor, options);
  }
}

// endregion
