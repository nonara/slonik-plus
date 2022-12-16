import { DatabasePoolPlus, DbRecord } from '..';
import { DatabaseTransactionConnection, FragmentSqlToken, QuerySqlToken, sql } from 'slonik';
import { mergeConfigs } from '@/utils/object-utils';


/* ****************************************************************************************************************** */
// region: Locals
/* ****************************************************************************************************************** */

declare class DbRecordType extends DbRecord {}

// endregion


/* ****************************************************************************************************************** */
// region: Types
/* ****************************************************************************************************************** */

export interface GetQueryOptions<T extends typeof DbRecord | unknown = unknown> {
  override?: QuerySqlToken
  append?: FragmentSqlToken
  transaction?: DatabaseTransactionConnection
  /**
   * Columns to select (specify 'all' to get all, including those with autoLoad set to false)
   * @default all columns configured with autoLoad = true
   */
  columns?: 'all' | (T extends DbRecord ? Exclude<keyof T, keyof DbRecord>[] : string[])
  /**
   * Override table name
   */
  tableName?: string
}

// endregion


/* ****************************************************************************************************************** */
// region: Utils
/* ****************************************************************************************************************** */

/**
 * @internal
 */
export async function dbGetWorker(
  this: DatabasePoolPlus,
  poolMethodKey: any,
  ctor: typeof DbRecord,
  opt?: GetQueryOptions
)
{
  const ctorConfig = ctor.getConfig();
  const options = mergeConfigs<GetQueryOptions>([ ctorConfig.query, opt ]);

  /* Validate */
  const tableName = options.tableName || ctorConfig.table;
  if (!tableName && !options.override)
    throw new Error(`No table specified for DbRecord ${ctor.name}. Must either specify a table name or override query`);

  /* Determine usable column info */
  let columns = ctor.getSelectColumns().filter(c =>
    (options.columns === 'all') ? true :
      options.columns ? options.columns.includes(c.name) :
        c.config.autoLoad
  );
  const keyMap = new Map(columns.map(({ dbName, name }) => [ dbName, name ]));
  const transformerMap = new Map(columns.map(({ dbName, config }) => [ dbName, config.in ]));

  /* Setup query */
  const query = options.override
    ? sql.unsafe`${options.override} ${options.append ? options.append : sql.fragment``}`
    : sql.unsafe`
        SELECT ${sql.join(columns.map(({ dbName }) => sql.identifier([ dbName ])), sql.fragment`, `)}
        FROM ${sql.identifier(tableName!.split('.'))}
        ${options.append ? options.append : sql.fragment``}
      `;

  // Fire query
  const res = await ((options.transaction || <any>this)[poolMethodKey](query));

  return ((typeof res !== 'object') || (res === null)) ? res :
    Array.isArray(res) ? res.map(fixupResult) :
      fixupResult(res);

  /* ********************************************************* *
   * Helpers
   * ********************************************************* */

  /**
   * Map result keys to proper names & add DbRecord prototype
   */
  function fixupResult(obj: any) {
    const fixedObj: any = {};
    for (const [ key, value ] of Object.entries(obj)) {
      const maybeTransformer = transformerMap.get(key);
      const resKey = keyMap.get(key) ?? key;
      fixedObj[resKey] = (typeof maybeTransformer === 'function') ? maybeTransformer(value) : value ?? undefined;
    }

    const res = new (<typeof DbRecordType>ctor)(fixedObj);
    res.saveState();

    return res;
  }
}

// endregion
