import deepMerge from 'deepmerge';


/* ****************************************************************************************************************** */
// region: Utils
/* ****************************************************************************************************************** */

/**
 * Filter object, only including specific properties (Based on TypeScript Pick)
 * @param obj - Object to filter
 * @param keys - Keys to extract
 * @example
 * let obj = { a: 1, b: 2, c: '3' }     // Type is { a: number, b: number, c: string }
 * obj = pick(obj, 'a', 'b')            // Type is { a: number, c: string }
 * @internal
 */
export function pick<T, K extends keyof T>(obj: T, ...keys: K[]): Pick<T, K> {
  const descriptors = Object.getOwnPropertyDescriptors(obj);

  const res = <typeof obj>{};
  for (const [ key, descriptor ] of Object.entries(descriptors)) {
    if (keys.includes(<any>key)) Object.defineProperty(res, key, descriptor);
  }

  return res;
}

/**
 * Filter object, excluding specific properties (Based on TypeScript Pick)
 * @param obj - Object to filter
 * @param keys - Keys to exclude
 * @example
 * const obj = { a: 1, b: 2, c: '3' }     // Type is { a: number, b: number, c: string }
 * const obj2 = omit(obj, 'a', 'c')       // Type is { b: number }
 * @internal
 */
export function omit<T, K extends keyof T>(obj: T, ...keys: K[]): Omit<T, K> {
  const descriptors = Object.getOwnPropertyDescriptors(obj);

  const res = <typeof obj>{};
  for (const [ key, descriptor ] of Object.entries(descriptors)) {
    if (!keys.includes(<any>key)) Object.defineProperty(res, key, descriptor);
  }

  return res;
}

/**
 * @internal
 */
export function mergeConfigs<T>(configs: (object | undefined)[]): T {
  return deepMerge.all(
    configs.filter(c => !!c) as object[],
    { arrayMerge: (a, b) => b }
  ) as T;
}

// endregion
