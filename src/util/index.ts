/**
 * Returns true if the given object is a Function. Otherwise, returns false.
 */
// eslint-disable-next-line @typescript-eslint/ban-types
const isFunction = (value: unknown): value is Function =>
  typeof value === 'function';

const objectToString = (o: unknown): string =>
  Object.prototype.toString.call(o);
/**
 * Returns true if the given object is strictly an Object and not a Function
 * (even though functions are objects in JavaScript). Otherwise, returns false.
 */
// eslint-disable-next-line @typescript-eslint/ban-types
const isObject = (value: unknown): value is Object =>
  value !== null && typeof value === 'object';

function delay(timeInMs: number): Promise<void> {
  return new Promise((resolve) => {
    setTimeout(resolve, timeInMs);
  });
}

function snakeToCamel(name: string): string {
  return name.replace(/(_[a-z])/g, (x) => x.charAt(1).toUpperCase());
}

function camelToSnake(name: string): string {
  return name.replace(/([A-Z])/g, (x) => `_${x.toLowerCase()}`);
}

const isDate = (arg: unknown): arg is Date =>
  isObject(arg) && objectToString(arg) === '[object Date]';

const isNativeError = (arg: unknown): arg is Error =>
  isObject(arg) &&
  (objectToString(arg) === '[object Error]' || arg instanceof Error);

function isPromise(obj: any): obj is Promise<unknown> {
  return (
    obj !== null && typeof obj === 'object' && typeof obj.then === 'function'
  );
}

export {
  camelToSnake,
  delay,
  isDate,
  isFunction,
  isNativeError,
  isObject,
  isPromise,
  snakeToCamel,
};
