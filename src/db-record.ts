import { camelToSnake, CamelToSnakeCase } from '@/utils/case-converters';
import { SetQueryOptions } from '@/query/set-worker';
import { DeleteQueryOptions } from '@/query/delete-worker';
import { PartialSome, RequireSome } from '@/utils/general';
import { FragmentSqlToken, ListSqlToken, sql, TypeNameIdentifier } from 'slonik';
import { GetQueryOptions } from '@/query/get-worker';
import { DatabasePoolPlus } from '@/pool';
import { mergeConfigs, omit } from '@/utils/object-utils';
import 'reflect-metadata';


/* ****************************************************************************************************************** */
// region: Config
/* ****************************************************************************************************************** */

export namespace ColumnConfig {
  export const defaults = {
    exclude: false,
    in: true,
    out: true,
    autoLoad: true,
    transformTimestamps: true
  } // satisfies Omit<ColumnConfig, 'type'>
}
export namespace RecordConfig {
  export const defaults = {
    convertCases: true
  } // satisfies RecordConfig

  export interface WithColumns extends RecordConfig {
    columns: { [column: string]: ColumnConfig }
  }
}

// endregion


/* ****************************************************************************************************************** */
// region: Locals
/* ****************************************************************************************************************** */

type UnknownToNever<T> = unknown extends T ? never :
  T extends undefined ? never : T

type FunctionKeys<T> = { [K in keyof T]: 0 extends (1 & T[K]) ? never : T[K] extends Function ? K : never }[keyof T]

// endregion


/* ****************************************************************************************************************** */
// region: Types
/* ****************************************************************************************************************** */

export type RecordIdentifier =
  TypeNameIdentifier | 'bool' | 'bytea' | 'float4' | 'float8' | 'int2' | 'int4' | 'json' | 'text' | 'timestamptz'
  | 'uuid' | 'int8' | 'text[]' | 'timestamp' | 'int8[]' | 'numeric' | 'jsonb' | 'interval' | 'int'

export type ColumnDetail = { name: string, dbName: string, config: ColumnConfig }

export type DbObject<T extends DbRecord> = {
  [K in keyof Omit<T, keyof DbRecord> as CamelToSnakeCase<K>]: T[K]
}

export interface ColumnConfig {
  /**
   * Exclude from db (same as setting { in: false, out: false })
   */
  exclude?: boolean

  /**
   * Boolean indicates whether to include in select
   * Function is a transformer to perform on data coming *from* the DB
   */
  in?: boolean | ((v: any) => any)

  /**
   * Boolean indicates whether to include in update / insert to database
   * Function is a transformer to perform on data coming *from* the DB
   */
  out?: boolean | ((v: any) => any);

  /**
   * Exclude column during UPDATE / UPSERT
   */
  excludeFromUpdate?: boolean

  /**
   * Manually specify name in DB
   * @default infers from property name
   */
  name?: string

  /**
   * Type on DB
   */
  type: RecordIdentifier;

  /**
   * Load automatically when selecting an object (without manually specifying columns)
   * @default true
   */
  autoLoad?: boolean

  /**
   * Primary Key (either use true or numeric order for multiple keys)
   */
  primaryKey?: true | number

  /**
   * Automatically transform timestamp to Date and vice-versa (applies as in/out settings)
   * @default true
   */
  transformTimestamps?: boolean
}

export interface RecordConfig {
  /**
   * Table record belongs to (ie. table or my_schema.table)
   */
  table?: string

  /**
   * Use snake_case on DB and camelCase for item
   * @default true
   */
  convertCases?: boolean

  query?: GetQueryOptions
}

// endregion


/* ****************************************************************************************************************** */
// region: Class
/* ****************************************************************************************************************** */

// @formatter:off
/**
 * @param T Base Object
 * @param Options Key manipulation { exclude: 'key1' | 'key2', partial: 'key3' | 'key4', require: 'key5' | 'key6' }
 */
export abstract class DbRecord<
  T extends Record<string, any> = never,
  Options extends { exclude?: keyof T, partial?: keyof T, require?: keyof T } = never
> {
// @formatter:on
  static _config: RecordConfig.WithColumns;

  /**
   * @internal
   */
  _original: Record<string, any> | undefined;

  /**
   * @internal
   */
  _metadata: Map<string, any> | undefined;

  /* ********************************************************* */
  // region: Constructor
  /* ********************************************************* */

  constructor(
    // @formatter:off
    data:
      Omit<
        RequireSome<PartialSome<T, UnknownToNever<Options['partial']>>, UnknownToNever<Options['require']>>,
        keyof DbRecord | UnknownToNever<Options['exclude']> | Exclude<FunctionKeys<T>, undefined>
      >
    // @formatter:on
  )
  {
    const hiddenPropDescriptor = { enumerable: false, writable: true, configurable: false, value: undefined };
    Object.defineProperties(this, {
      _original: hiddenPropDescriptor,
      _metadata: hiddenPropDescriptor
    });

    Object.assign(this, data);
  }

  // endregion

  /* ********************************************************* */
  // region: Static Methods
  /* ********************************************************* */

  static getConfig(): RecordConfig.WithColumns {
    return this._config;
  }

  static getInsertColumns(): ColumnDetail[] {
    const res: ColumnDetail[] = [];
    for (const [ name, config ] of Object.entries(this.getConfig().columns)) {
      if ((config.out !== false) && !config.exclude) res.push({ name, dbName: config.name!, config });
    }

    return res;
  }

  static getSelectColumns(): ColumnDetail[] {
    const res: ColumnDetail[] = [];
    for (const [ name, config ] of Object.entries(this.getConfig().columns)) {
      if ((config.in !== false) && !config.exclude) res.push({ name, dbName: config.name!, config });
    }

    return res;
  }

  static getPrimaryKeys(): string[] {
    return Object.values(this.getConfig().columns)
      .filter(({ primaryKey }) => +<any>primaryKey >= 0)
      .sort((a, b) => +a.primaryKey! - +b.primaryKey!)
      .map(({ name }) => name!);
  }

  // endregion

  /* ********************************************************* */
  // region: Methods
  /* ********************************************************* */

  getMetadata<T>(k: string): T {
    return this._metadata?.get(k);
  }

  setMetadata<T>(k: string, value: T) {
    if (!this._metadata) this._metadata = new Map();
    this._metadata.set(k, value);
  }

  /**
   * Create snake cased object for own enumerable properties
   */
  toDbObject(): DbObject<this> {
    return camelToSnake(this, { copyMethod: 'entries' }) as DbObject<this>;
  }

  async insert<TOptions extends Partial<Omit<SetQueryOptions, 'conflictKey'>>>(pool: DatabasePoolPlus, options?: TOptions) {
    return await pool.insert(this, options);
  }

  async upsert<TOptions extends Partial<SetQueryOptions>>(pool: DatabasePoolPlus, options?: TOptions) {
    return await pool.upsert(this, options);
  }

  async update<TOptions extends Partial<Omit<SetQueryOptions, 'conflictKey'>>>(pool: DatabasePoolPlus, options?: TOptions) {
    return await pool.update(this, options);
  }

  async delete<TOptions extends Partial<DeleteQueryOptions>>(pool: DatabasePoolPlus, options?: TOptions) {
    return await pool.delete(this, options);
  }

  /**
   * Get output value of column (runs through transformer if one is set)
   */
  getColumnOutputValue(key: keyof this): any {
    const transformer = (<typeof DbRecord>this.constructor)
      .getInsertColumns()
      .find(c => c.name === key)?.config.out;

    return (typeof transformer === 'function') ? transformer(this[key]) : this[key];
  }

  /**
   * Gets output key of column
   */
  getColumnOutputKey(key: keyof this): any {
    return (<typeof DbRecord>this.constructor).getInsertColumns().find(c => c.name === key)?.dbName
  }

  // endregion

  /* ********************************************************* */
  // region: Internal Methods
  /* ********************************************************* */

  /**
   * Save current state information
   * @internal
   */
  saveState() {
    this._original = omit(this, '_original');
  }

  /**
   * @internal
   */
  getChangedColumns() {
    const ctor = this.constructor as typeof DbRecord;
    return !this._original
           ? ctor.getInsertColumns()
           : ctor.getInsertColumns().filter(col => this._original![col.name] !== (<any>this)[col.name]);
  }

  getWhereClause(): FragmentSqlToken | undefined
  getWhereClause<T>(noLeadingWhere: T): (T extends true ? ListSqlToken : FragmentSqlToken) | undefined
  getWhereClause(noLeadingWhere?: boolean): FragmentSqlToken | ListSqlToken | undefined {
    const ctor = this.constructor as typeof DbRecord;
    const primaryKeys = ctor.getPrimaryKeys();
    if (!primaryKeys.length) return undefined;

    const columns = ctor.getInsertColumns();
    const conditions = primaryKeys.map(k => {
      const value = (<any>this)[columns.find(c => c.dbName === k)!.name];
      if (!value) throw new Error(`Cannot find value for primary key: ${k}\nRecord: ${JSON.stringify(this, null, 2)}`);
      return sql.fragment`${sql.identifier([ k ])} = ${value}`
    });

    return !noLeadingWhere ? sql.fragment`WHERE ${sql.join(conditions, sql.fragment` AND `)}` : sql.join(conditions, sql.fragment` AND `);
  }

  // endregion
}

// endregion


/* ****************************************************************************************************************** */
// region: Decorators
/* ****************************************************************************************************************** */

export namespace DbRecord {
  /**
   * (Decorator) Configure column
   */
  export function column(cfg: ColumnConfig | Omit<Partial<ColumnConfig>, 'exclude'> & { exclude: true }) {
    const config = mergeConfigs<ColumnConfig>([ ColumnConfig.defaults, cfg ]);

    if ([ 'timestamptz', 'timestamp' ].includes(<any>cfg.type) && config.transformTimestamps) {
      if (cfg.in === undefined) config.in = (v) => (typeof v === 'number') ? new Date(v) : v;
      if (cfg.out === undefined) config.out = (d: Date | null) => (d instanceof Date) ? d.toISOString() : d
    }

    return (target: any, propertyKey: string) => {
      const ctor = target.constructor;
      const columnsConfig: Map<string, ColumnConfig> =
        Reflect.getOwnMetadata('columnsConfig', ctor) || new Map();

      columnsConfig.set(propertyKey, config);

      if (!Reflect.hasOwnMetadata('columnsConfig', ctor))
        Reflect.defineMetadata('columnsConfig', columnsConfig, ctor)
    }
  }

  /**
   * (Decorator) Configure DB Object extension
   */
  export function record(cfg: RecordConfig) {
    return function (ctor: Function) {
      const recordConfig = mergeConfigs<RecordConfig>([ RecordConfig.defaults, cfg ]);

      /* Calculate base columns */
      const outputColumns: RecordConfig.WithColumns['columns'] = {};
      const ownColumnsConfig: Map<string, ColumnConfig> | undefined =
        Reflect.getOwnMetadata('columnsConfig', ctor);

      if (ownColumnsConfig)
        [ ...ownColumnsConfig.entries() ].forEach(([ key, config ]) => {
          if (!config.name) config.name = recordConfig.convertCases ? camelToSnake(key) : key;
          outputColumns[key] = { ...config };
        });

      /* Add lineage columns */
      for (let proto = Object.getPrototypeOf(ctor); DbRecord.isPrototypeOf(proto); proto = Object.getPrototypeOf(proto.prototype.constructor)) {
        const columns = (<typeof DbRecord>proto).getConfig().columns;
        Object.entries(columns).forEach(([ key, config ]) => {
          if (!outputColumns.hasOwnProperty(key)) outputColumns[key] = { ...config };
        });
      }

      (<any>ctor)._config = Object.assign(recordConfig, { columns: outputColumns });
    }
  }

  // endregion
}

// endregion
