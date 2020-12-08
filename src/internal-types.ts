import {
  ErrorType,
  QueryType,
  ResponseNote,
  ResponseType,
  TermType,
} from './proto/enums';

export type DeepPartial<T> =
  | T
  | {
      [P in keyof T]?: T[P] extends Array<infer U1>
        ? Array<DeepPartial<U1>>
        : T[P] extends ReadonlyArray<infer U2>
        ? ReadonlyArray<DeepPartial<U2>>
        : DeepPartial<T[P]>;
    };

export type OptargsJson = Record<string, unknown> | undefined;

export type TermJson =
  | [TermType, TermJson[]?, OptargsJson?]
  | string
  | number
  | boolean
  | Record<string, unknown>
  | null;

export type ComplexTermJson = [TermType, TermJson[]?, OptargsJson?];

export type QueryJson = [QueryType, TermJson?, OptargsJson?];

export interface ResponseJson {
  t: ResponseType;
  r: any[];
  n: ResponseNote[];
  e?: ErrorType;
  p?: any;
  b?: Array<number | string>;
}
