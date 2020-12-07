/**
 * Returns true if the given object is a Function. Otherwise, returns false.
 */
// eslint-disable-next-line @typescript-eslint/ban-types
const isFunction = (value: any): value is Function =>
  typeof value === 'function';

/**
 * Returns true if the given object is strictly an Object and not a Function
 * (even though functions are objects in JavaScript). Otherwise, returns false.
 */
// eslint-disable-next-line @typescript-eslint/ban-types
const isObject = (value: any): value is Object =>
  value !== null && typeof value === 'object';

function snakeToCamel(name: string): string {
  return name.replace(/(_[a-z])/g, (x) => x.charAt(1).toUpperCase());
}

export { isFunction, isObject, snakeToCamel };
