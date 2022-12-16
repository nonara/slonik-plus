/* ****************************************************************************************************************** */
// region: Types
/* ****************************************************************************************************************** */

export type OneOrMore<T> = T | T[]

/**
 * Make certain properties required
 */
export declare type RequireSome<T, K extends keyof T> = T & Pick<Required<T>, K>;
/**
 * Make certain properties partial
 */
export declare type PartialSome<T, K extends keyof T> = Omit<T, K> & Pick<Partial<T>, K>;

// endregion


/* ****************************************************************************************************************** */
// region: Utils
/* ****************************************************************************************************************** */

export const isValidPostgresIdentifier = (s: string) => /^[A-Za-z_][A-Za-z_0-9$]*$/.test(s);

// endregion
