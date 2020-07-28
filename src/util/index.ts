/**
 * Returns true if the given object is a Function. Otherwise, returns false.
 */
const isFunction = (value: any): value is Function => typeof value === 'function';

/**
 * Returns true if the given object is strictly an Object and not a Function
 * (even though functions are objects in JavaScript). Otherwise, returns false.
 */
const isObject = (value: any): value is Object => value !== null && typeof value === 'object';

export { isFunction, isObject };
