/**
 * Returns true if the given object is a Function. Otherwise, returns false.
 */
// eslint-disable-next-line @typescript-eslint/ban-types
const isFunction = (arg: unknown): arg is Function => typeof arg === 'function';

const objectToString = (o: unknown): string =>
  Object.prototype.toString.call(o);
/**
 * Returns true if the given object is strictly an Object and not a Function
 * (even though functions are objects in JavaScript). Otherwise, returns false.
 */
// eslint-disable-next-line @typescript-eslint/ban-types
const isObject = (arg: unknown): arg is Object =>
  arg !== null && typeof arg === 'object';

function delay(timeInMs: number, options?: { unref: boolean }): Promise<void> {
  return new Promise((resolve) => {
    const timer = setTimeout(resolve, timeInMs);

    if (options && options.unref) {
      timer.unref();
    }
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

function isPromise(arg: any): arg is Promise<unknown> {
  return (
    arg !== null && typeof arg === 'object' && typeof arg.then === 'function'
  );
}

const ipv6Regex = /^((?=.*::)(?!.*::.+::)(::)?([\dA-F]{1,4}:(:|\b)|){5}|([\dA-F]{1,4}:){6})((([\dA-F]{1,4}((?!\3)::|:\b|$))|(?!\2\3)){2}|(((2[0-4]|1\d|[1-9])?\d|25[0-5])\.?\b){4})$/i;
const isIPv6 = (ip: string): boolean => ipv6Regex.test(ip);

export {
  camelToSnake,
  delay,
  isDate,
  isFunction,
  isIPv6,
  isNativeError,
  isObject,
  isPromise,
  snakeToCamel,
};
