import { RethinkDBErrorType } from '../types';
import { TermJson } from '../internal-types';
import { ErrorType, ResponseType } from '../proto/enums';
import { globals } from '../query-builder/globals';
import { backtraceTerm } from './term-backtrace';

function pretty(query: string, mark: string) {
  let result = '';
  let indent = 0;
  const openIndentPos: number[] = [];
  let char = '';
  let newline = true;
  let inStr = false;
  let shouldEscape = false;
  let lastNewlinePos = 0;
  let lineMarkPos = 0;
  let lineMark = '';
  let nextSign = '';
  let isLastIndentDot = false;
  const openBrackets: string[] = [];
  for (let i = 0; i < query.length; i += 1) {
    char = query.charAt(i);
    if (!inStr) {
      if (['{', '(', '['].includes(char)) {
        openBrackets.unshift(char);
      } else if (
        (char === '}' && openBrackets[0] === '{') ||
        (char === ')' && openBrackets[0] === '(') ||
        (char === ']' && openBrackets[0] === '[')
      ) {
        openBrackets.shift();
      }
    }
    switch (char) {
      case '.':
        shouldEscape = false;
        newline = false;
        if (inStr || result.length - lastNewlinePos <= 80 + indent) {
          result += char;
        } else {
          if (!isLastIndentDot) {
            indent += 4;
          }
          lineMark += mark.substring(lineMarkPos, i);
          lineMarkPos = i + 1;
          result = result.trimRight();
          nextSign =
            lineMark.charAt(result.length - lastNewlinePos) || mark.charAt(i);
          lineMark = lineMark.substring(0, result.length - lastNewlinePos);
          result += lineMark.includes('^')
            ? `\n${lineMark}\n${' '.repeat(indent)}.`
            : `\n${' '.repeat(indent)}.`;
          lastNewlinePos = result.length - indent - 1;
          lineMark = ' '.repeat(indent) + nextSign;
          isLastIndentDot = true;
        }
        break;
      case ',':
        if (inStr || openBrackets[0] !== '{') {
          newline = false;
          result += char;
        } else {
          newline = true;
          lineMark += mark.substring(lineMarkPos, i + 1);
          lineMarkPos = i + 1;
          result += lineMark.includes('^')
            ? `,\n${lineMark}\n${' '.repeat(indent)}`
            : `,\n${' '.repeat(indent)}`;
          lastNewlinePos = result.length - indent;
          lineMark = ' '.repeat(indent);
        }
        break;
      case '{':
        shouldEscape = false;
        if (inStr || query.charAt(i + 1) === '}') {
          newline = false;
          result += char;
        } else {
          newline = true;
          openIndentPos.push(indent);
          isLastIndentDot = false;
          indent += 4;
          lineMark += mark.substring(lineMarkPos, i + 1);
          lineMarkPos = i + 1;
          result += lineMark.includes('^')
            ? `{\n${lineMark}\n${' '.repeat(indent)}`
            : `{\n${' '.repeat(indent)}`;
          lastNewlinePos = result.length - indent;
          lineMark = ' '.repeat(indent);
        }
        break;
      case '}':
        newline = false;
        shouldEscape = false;
        if (inStr || query.charAt(i - 1) === '{') {
          result += char;
        } else {
          indent = openIndentPos.pop() || 0;
          lineMark += mark.substring(lineMarkPos, i);
          lineMarkPos = i + 1;
          result = result.trimRight();
          nextSign =
            lineMark.charAt(result.length - lastNewlinePos) || mark.charAt(i);
          lineMark = lineMark.substring(0, result.length - lastNewlinePos);
          result += lineMark.includes('^')
            ? `\n${lineMark}\n${' '.repeat(indent)}}`
            : `\n${' '.repeat(indent)}}`;
          lastNewlinePos = result.length - indent - 1;
          lineMark = ' '.repeat(indent) + nextSign;
        }
        break;
      case ' ':
        shouldEscape = false;
        if (newline) {
          lineMarkPos += 1;
        } else {
          result += char;
        }
        break;
      case '"':
        if (shouldEscape) {
          shouldEscape = false;
        } else {
          inStr = !inStr;
        }
        newline = false;
        result += char;
        break;
      case '\\':
        shouldEscape = !escape;
        newline = false;
        result += char;
        break;
      default:
        shouldEscape = false;
        newline = false;
        result += char;
        break;
    }
  }
  lineMark += mark.substring(lineMarkPos, query.length);
  result = result.trimRight();
  lineMark = lineMark.substring(0, result.length - lastNewlinePos);
  result += lineMark.includes('^') ? `\n${lineMark}\n` : '\n';
  return result;
}
function preparseMessage(message: string): string {
  if (message.charAt(message.length - 1) === ':') {
    return message;
  }
  if (message.charAt(message.length - 1) === '.') {
    return `${message.substring(0, message.length - 1)} in:`;
  }
  return `${message} in:`;
}
function buildMessage(
  messageString: string,
  term?: TermJson,
  backtrace?: Array<number | string>,
) {
  let message = messageString;
  const t = term;
  if (t) {
    message = preparseMessage(message);
    const [str, mark] = backtraceTerm(t, true, backtrace);
    if (globals.pretty) {
      message += `\n${pretty(str, mark)}`;
    } else {
      message += `\n${str}\n`;
      if (backtrace) {
        message += `${mark}\n`;
      }
    }
  }
  return message;
}

export interface RethinkDBErrorArgs {
  cause?: Error;
  type?: RethinkDBErrorType;
  errorCode?: number;
  term?: TermJson;
  backtrace?: Array<number | string>;
  responseType?: ResponseType;
  responseErrorType?: ErrorType;
}

interface ErrorGetterOptions {
  errorCode?: number;
  type?: RethinkDBErrorType;
  responseErrorType?: ErrorType;
}

interface ErrorGetterResult {
  name: string;
  type: RethinkDBErrorType;
}
function getErrorNameAndType({
  errorCode,
  type,
  responseErrorType,
}: ErrorGetterOptions): ErrorGetterResult {
  if (type) {
    return { name: 'ReqlDriverError', type };
  }
  if (errorCode && errorCode >= 10 && errorCode <= 20) {
    // https://rethinkdb.com/docs/writing-drivers/
    // A ReqlAuthError should be thrown if the error code is between 10 and 20 (inclusive)
    // what about other error codes?
    return { name: 'ReqlAuthError', type: RethinkDBErrorType.AUTH };
  }
  switch (responseErrorType) {
    case ErrorType.INTERNAL:
      return {
        name: 'ReqlInternalError',
        type: RethinkDBErrorType.INTERNAL,
      };
    case ErrorType.NON_EXISTENCE:
      return {
        name: 'ReqlNonExistanceError',
        type: RethinkDBErrorType.NON_EXISTENCE,
      };
    case ErrorType.OP_FAILED:
      return {
        name: 'ReqlOpFailedError',
        type: RethinkDBErrorType.OP_FAILED,
      };
    case ErrorType.OP_INDETERMINATE:
      return {
        name: 'ReqlOpIndeterminateError',
        type: RethinkDBErrorType.OP_INDETERMINATE,
      };
    case ErrorType.PERMISSION_ERROR:
      return {
        name: 'ReqlPermissionError',
        type: RethinkDBErrorType.PERMISSION_ERROR,
      };
    case ErrorType.QUERY_LOGIC:
      return {
        name: 'ReqlLogicError',
        type: RethinkDBErrorType.QUERY_LOGIC,
      };
    case ErrorType.RESOURCE_LIMIT:
      return {
        name: 'ReqlResourceError',
        type: RethinkDBErrorType.RESOURCE_LIMIT,
      };
    case ErrorType.USER:
      return {
        name: 'ReqlUserError',
        type: RethinkDBErrorType.USER,
      };
    default:
      return { name: 'ReqlUnknownError', type: RethinkDBErrorType.UNKNOWN };
  }
}

export class RethinkDBError extends Error {
  public readonly cause: Error | undefined;

  public readonly type: RethinkDBErrorType = RethinkDBErrorType.UNKNOWN;

  private term?: TermJson;

  constructor(public msg: string, args: RethinkDBErrorArgs = {}) {
    const { cause, type, term, errorCode, backtrace, responseErrorType } = args;
    super(buildMessage(msg, term, backtrace));
    this.cause = cause;
    this.name = 'ReqlDriverError';
    this.msg = msg;
    this.term = term;

    const { name, type: returnedType } = getErrorNameAndType({
      errorCode,
      responseErrorType,
      type,
    });
    this.name = name;
    this.type = returnedType;
    Error.captureStackTrace(this, RethinkDBError);
  }
}

export function isRethinkDBError(error: unknown): error is RethinkDBError {
  return error instanceof RethinkDBError;
}
