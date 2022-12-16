import { ClientConfigurationInput, createPool, DatabasePool, NotFoundError } from 'slonik';
import { DbRecord } from './db-record';
import { dbGetWorker, GetQueryOptions } from './query/get-worker';
import { DatabasePoolPlusBase } from '@/pool-proto';
import { mergeConfigs } from '@/utils/object-utils';


/* ****************************************************************************************************************** */
// region: Locals
/* ****************************************************************************************************************** */

// Avoid infinite recursion issue
type DbRecordLike = Omit<DbRecord, 'toDbObject'>

type OverrideConfig = Record<string, { kind: 'single' | 'multi' | 'prop', nullable: boolean }>
type Overrides = typeof overrideConfig;

// endregion


/* ****************************************************************************************************************** */
// region: Config
/* ****************************************************************************************************************** */

const overrideConfig = {
  'maybeOne': {
    kind: 'single',
    nullable: true
  },
  'one': {
    kind: 'single',
    nullable: false
  },
  'any': {
    kind: 'multi',
    nullable: false
  },
  'many': {
    kind: 'multi',
    nullable: false
  }
} as const satisfies OverrideConfig;

const poolConfig: DatabasePoolPlusConfig = {
  deleteChunkMax: 2000,
  insertChunkMax: 2000
};

// endregion


/* ****************************************************************************************************************** */
// region: Types
/* ****************************************************************************************************************** */

// @formatter:off
export type DatabasePoolPlus = DatabasePool & {
  [K in keyof Overrides]:
    Overrides[K] extends { kind: infer Kind, nullable?: infer Nullable } ?
      {
        <T extends { new(...args: any[]): DbRecordLike }>(obj: T, options?: GetQueryOptions<InstanceType<T>>):
          Promise<
            ('single' extends Kind ? InstanceType<T> : InstanceType<T>[])   // One or multiple
            | (true extends Nullable ? undefined : never)                    // Add nullability
          >
      } & DatabasePool[K]
    : never
} & {
  maybeMany: {
    <T extends { new(...args: any[]): DbRecordLike }>(obj: T, options?: GetQueryOptions<InstanceType<T>>):
      Promise<InstanceType<T>[] | undefined>
  } & DatabasePool['any']
  config: DatabasePoolPlusConfig
} & DatabasePoolPlusBase
// @formatter:on

export interface DatabasePoolPlusConfig {
  deleteChunkMax: number
  insertChunkMax: number
}

// endregion


/* ****************************************************************************************************************** */
// region: Utils
/* ****************************************************************************************************************** */

export async function createPoolPlus(
  connectionUri: string,
  clientConfigurationInput?: ClientConfigurationInput,
  plusConfig?: DatabasePoolPlusConfig
): Promise<DatabasePoolPlus> {
  const pool = await createPool(connectionUri, clientConfigurationInput) as any as DatabasePoolPlus;

  /* Setup Overrides */
  for (const key of Object.keys(overrideConfig)) {
    const origFn = (<any>pool)[key].bind(pool);

    (<any>pool)[key] = async function (maybeCtor: any, options?: GetQueryOptions) {
      return (!DbRecord.isPrototypeOf(maybeCtor))
        ? origFn(...arguments)
        : await dbGetWorker.call(pool, key, maybeCtor, options);
    };
  }

  /* Add Extras */
  (<any>pool).maybeMany = async function () {
    try {
      return await (<any>pool).any(...arguments);
    }
    catch (e: any) {
      if (e instanceof NotFoundError) return void 0;
      else throw e;
    }
  }

  pool.config = mergeConfigs<DatabasePoolPlusConfig>([ poolConfig, plusConfig ]);

  /* Set Base Proto */
  Object.setPrototypeOf(pool, DatabasePoolPlusBase.prototype);

  return pool as unknown as DatabasePoolPlus;
}

// endregion
