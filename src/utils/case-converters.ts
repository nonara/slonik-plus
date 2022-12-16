
/* ****************************************************************************************************************** */
// region: Types
/* ****************************************************************************************************************** */

export type SnakeToCamelCase<S> =
  S extends `${infer T}_${infer U}` ? `${T}${Capitalize<SnakeToCamelCase<U>>}` : S

export type CamelToSnakeCase<S> =
  S extends `${infer T}${infer U}` ? `${T extends Capitalize<T> ? "_" : ""}${Lowercase<T>}${CamelToSnakeCase<U>}` : S

interface CaseConverterOptions {
  /**
   * @default false
   */
  copyPrototype?: boolean
  /**
   * @default 'descriptors'
   */
  copyMethod?: 'entries' | 'descriptors'
}

export interface CamelToSnakeOptions extends CaseConverterOptions {}
export interface SnakeToCamelOptions extends CaseConverterOptions {}

// endregion


/* ****************************************************************************************************************** */
// region: Helpers
/* ****************************************************************************************************************** */

function caseConverterWorker(
  obj: any,
  opt: CamelToSnakeOptions | SnakeToCamelOptions,
  converterFn: typeof snakeToCamel | typeof camelToSnake
): any {
  const { copyMethod, copyPrototype } = opt;

  let descEntries: [ string, PropertyDescriptor ][];
  switch (copyMethod) {
    case 'entries':
      descEntries = Object.entries(obj).map(([ key, value ]) => [
        key,
        { writable: true, enumerable: true, configurable: true, value }
      ]);
      break;
    default:
    case 'descriptors':
      descEntries = Object.entries(Object.getOwnPropertyDescriptors(obj));
  }

  const descriptors: PropertyDescriptorMap = {};
  for (const [k, v] of descEntries) descriptors[(<Function>converterFn)(k)] = v;

  const res = Object.defineProperties({}, descriptors);
  if (copyPrototype) Object.setPrototypeOf(res, Object.getPrototypeOf(obj));

  return res;
}

/**
 * @internal
 */
export const getConverterOptions = <T extends CaseConverterOptions | undefined>(overrides?: T): Exclude<T, undefined> => ({
  copyMethod: 'descriptors',
  ...overrides
}) as Exclude<T, undefined>;

// endregion


/* ****************************************************************************************************************** */
// region: Utils
/* ****************************************************************************************************************** */

/**
 * Converts string from snake_case to camelCase
 */
export function snakeToCamel<T extends string>(str: T): SnakeToCamelCase<T>
/**
 * Shallow copies object, converting property keys from snake_case to camelCase
 */
export function snakeToCamel<T extends object>(obj?: T, opt?: SnakeToCamelOptions):
// @formatter:off
  T extends Array<infer U> ? T :
  T extends null ? T :
  { [K in keyof T as SnakeToCamelCase<K>]: T[K] }
// @formatter:on
export function snakeToCamel<T extends string | object>(src: T, opt?: SnakeToCamelOptions): string | Record<string, any>
{
  // Handle string conversion
  if (typeof src === 'string')
    return src
      .toLowerCase()
      .replace(/[-_][a-zA-Z]/g, (group) => group.slice(-1).toUpperCase()) as string;

  // Handle object conversion
  return caseConverterWorker(src, getConverterOptions(opt), snakeToCamel);
}

/**
 * Converts string from camelCase to snake_case
 */
export function camelToSnake<T extends string>(str: T): CamelToSnakeCase<T>
/**
 * Shallow copies object, converting property keys from camelCase to snake_case
 */
export function camelToSnake<T extends object>(obj: T, opt?: CamelToSnakeOptions):
// @formatter:off
  T extends Array<infer U> ? T :
  T extends null ? T :
  { [K in keyof T as CamelToSnakeCase<K>]: T[K] }
// @formatter:on
export function camelToSnake<T extends string | object>(src: T, opt?: CamelToSnakeOptions): object | string
{
  // Handle string conversion
  if (typeof src === 'string')
    return src.replace(/[A-Z]/g, (group) => `_${group.toLowerCase()}`);

  // Handle object conversion
  return caseConverterWorker(src, getConverterOptions(opt), camelToSnake);
}

// endregion
