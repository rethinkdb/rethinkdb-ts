import { RethinkDBError, RethinkDBErrorType } from '../error';
import type { RunOptions } from '../types';

export function parseRawResponse(
  obj: any,
  {
    binaryFormat = 'native',
    groupFormat = 'native',
    timeFormat = 'native',
  }: Pick<RunOptions, 'binaryFormat' | 'groupFormat' | 'timeFormat'> = {},
): any {
  if (Array.isArray(obj)) {
    return obj.map((item) =>
      parseRawResponse(item, { binaryFormat, groupFormat, timeFormat }),
    );
  }
  if (obj === null || typeof obj !== 'object') {
    return obj;
  }
  if (obj.$reql_type$) {
    switch (obj.$reql_type$) {
      case 'TIME':
        if (timeFormat === 'native') {
          return new Date(obj.epoch_time * 1000);
        }
        if (timeFormat === 'ISO8601') {
          const {
            epoch_time,
            timezone,
          }: { epoch_time: number; timezone: string } = obj;
          const [hour, minute] = timezone
            .split(':')
            .map((num) => parseInt(num, 10));
          const fixedEpoch =
            (epoch_time + hour * 60 * 60 + Math.sign(hour) * minute * 60) *
            1000;
          return new Date(fixedEpoch).toISOString().replace('Z', timezone);
        }
        break;
      case 'BINARY':
        if (binaryFormat === 'native') {
          return Buffer.from(obj.data, 'base64');
        }
        break;
      case 'GROUPED_DATA':
        if (groupFormat === 'native') {
          return obj.data.map(([group, reduction]: any) => ({
            group: parseRawResponse(group, {
              binaryFormat,
              groupFormat,
              timeFormat,
            }),
            reduction: parseRawResponse(reduction, {
              binaryFormat,
              groupFormat,
              timeFormat,
            }),
          }));
        }
        break;
      case 'GEOMETRY':
        break;
      default:
        throw new RethinkDBError('Unexpected value of $reql_type', {
          type: RethinkDBErrorType.PARSE,
        });
    }
  }
  return Object.entries(obj).reduce((acc: any, [key, val]) => {
    acc[key] = parseRawResponse(val);
    return acc;
  }, {});
}
