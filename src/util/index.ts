const kCustomPromisifiedSymbol =
  typeof Symbol !== 'undefined' ? Symbol('util.promisify.custom') : undefined;

const getOwnPropertyDescriptors =
  Object.getOwnPropertyDescriptors ||
  function getOwnPropertyDescriptors(obj) {
    const keys = Object.keys(obj);
    const descriptors: Record<string, PropertyDescriptor | undefined> = {};
    for (let i = 0; i < keys.length; i += 1) {
      descriptors[keys[i]] = Object.getOwnPropertyDescriptor(obj, keys[i]);
    }
    return descriptors;
  };

/**
 * Returns true if the given object is a Function. Otherwise, returns false.
 */
// eslint-disable-next-line @typescript-eslint/ban-types
const isFunction = (value: any): value is Function =>
  typeof value === 'function';

const objectToString = (o: unknown): string =>
  Object.prototype.toString.call(o);
/**
 * Returns true if the given object is strictly an Object and not a Function
 * (even though functions are objects in JavaScript). Otherwise, returns false.
 */
// eslint-disable-next-line @typescript-eslint/ban-types
const isObject = (value: any): value is Object =>
  value !== null && typeof value === 'object';

function delay(timeInMs: number): Promise<void> {
  return new Promise((resolve) => {
    setTimeout(resolve, timeInMs);
  });
}

function snakeToCamel(name: string): string {
  return name.replace(/(_[a-z])/g, (x) => x.charAt(1).toUpperCase());
}

function camelToSnake(name: string) {
  return name.replace(/([A-Z])/g, (x) => `_${x.toLowerCase()}`);
}

const isDate = (arg: unknown): arg is Date =>
  isObject(arg) && objectToString(arg) === '[object Date]';

const isNativeError = (arg: unknown): arg is Error =>
  isObject(arg) &&
  (objectToString(arg) === '[object Error]' || arg instanceof Error);

function promisify(original: unknown) {
  if (typeof original !== 'function')
    throw new TypeError('The "original" argument must be of type Function');

  // eslint-disable-next-line @typescript-eslint/ban-types
  let fn: Function;
  if (kCustomPromisifiedSymbol && original[kCustomPromisifiedSymbol]) {
    fn = original[kCustomPromisifiedSymbol];
    if (typeof fn !== 'function') {
      throw new TypeError(
        'The "util.promisify.custom" argument must be of type Function',
      );
    }
    Object.defineProperty(fn, kCustomPromisifiedSymbol, {
      value: fn,
      enumerable: false,
      writable: false,
      configurable: true,
    });
    return fn;
  }

  fn = () => {
    let promiseResolve;
    let promiseReject;
    const promise = new Promise(function (resolve, reject) {
      promiseResolve = resolve;
      promiseReject = reject;
    });

    const args = [];
    for (let i = 0; i < arguments.length; i += 1) {
      args.push(arguments[i]);
    }
    args.push(function (err, value) {
      if (err) {
        promiseReject(err);
      } else {
        promiseResolve(value);
      }
    });

    try {
      original.apply(this, args);
    } catch (err) {
      promiseReject(err);
    }

    return promise;
  };

  Object.setPrototypeOf(fn, Object.getPrototypeOf(original));

  if (kCustomPromisifiedSymbol)
    Object.defineProperty(fn, kCustomPromisifiedSymbol, {
      value: fn,
      enumerable: false,
      writable: false,
      configurable: true,
    });
  return Object.defineProperties(fn, getOwnPropertyDescriptors(original));
}

export {
  camelToSnake,
  delay,
  isDate,
  isFunction,
  isNativeError,
  isObject,
  snakeToCamel,
};
