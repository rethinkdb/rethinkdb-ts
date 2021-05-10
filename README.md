# rethinkdb-ts
[![npm][npm]][npm-url]
[![node][node]][node-url]
[![deps][deps]][deps-url]
[![licenses][licenses]][licenses-url]
[![downloads][downloads]][downloads-url]
[![size][size]][size-url]
[![build][build]][build-url]

## Install

`npm i rethinkdb-ts`

or

`yarn add rethinkdb-ts`

## Import

```ts
// if you support import
import { r } from 'rethinkdb-ts';
// if you dont
const { r } = require('rethinkdb-ts');
```

## Initialize

```ts
// in an async context
// if you want to initialize a connection pool
await r.connectPool(options);
// if you want to initialize a single connection
const conn = await r.connect(options);
```

# STATUS:

- Fully working typescript driver!
- Rebuilt from scratch using the latest ES/TS features for readability and maintainability
- Drop-in replacement for rethinkdbdash with only some minor changes

# CHANGES FROM RETHINKDBDASH
- Support for complex socket configuration + tls (notice that for SSL/TLS or any configuration more complex than `{ host: '...', port: '...' }` you'll have to encapsulate in a server/servers property: 
```ts
{ 
   server: {
      host: '172.23.12.2',
      port: 21085,
      tls: true,
      ca: caCert,
      rejectUnauthorized: false
   } 
}
```
The options for standard connections is described [here](https://nodejs.org/dist/latest-v10.x/docs/api/net.html#net_net_createconnection_options_connectlistener).

If you want an SSL/TLS, add `tls: true` and the options described [here](https://nodejs.org/dist/latest-v10.x/docs/api/tls.html#tls_tls_connect_options_callback) and 
- Importing property instead of entire library: `const {r} = require('rethinkdb-ts')` or `import {r} from 'rethinkdb-ts'` instead of `const r = require('rethinkdbdash')(options)`
- No top level initialization, initializing a pool is done by `await r.connectPool()`
- No `{ cursor: true }` option, for getting a cursor use `.getCursor(runOptions)` instead of `.run(runOptions)`
  - `.run()` will coerce streams to array by default feeds will return a cursor like rethinkdbdash
- Uses native promises instead of `bluebird`
- A cursor is already a readable stream, no need for `toStream()`
- A readable stream is already an async iterator in node 10 no need for `.asyncIterator()`
- In a connection pool, reusing open connections that already run queries instead of making queries wait for a connection when max connections exceeded
- Integrated fully encompasing type definitions

# NEW FEATURES

- serialize / deserialize. You can store the query as a string like this `const serializedQuery = r.table(...).filter(...).map(...).serialize()` and get it like this `r.deserialize(serializedQuery).run()` or even `r.deserialize<RStream>(serializedQuery).reduce(...).run()` the serialized query is a normal string so you can store it in the DB. No need for ugly workarounds like `.toString` and `eval` anymore. Also the serialized query is the actual JSON that gets sent to the server so it should be cross-language compatible if any other driver cares to implement it.

# DROPPING SUPPORT:

- Support node < 8
- Support callbacks
- Support using `.then()` directly on a query (optionalRun), it can confuse users that queries are promises leading to false assumptions:
  - Queries are not promises since they are not eagerly evaluated and therefore they can:
    - `.run()` as many times as you want (promises run only once and return the same value without running other times)
    - be stored for future evaluation (promises run as you create them)
- Support browsers (Unless it's the only demand of making this driver used instead of rethinkdbdash)
- Support write streams (Does anyone use it? Will add it if its a popular demand)
- Multiple connection pools (if someone has a good usecase I'll support it)

[npm]: https://img.shields.io/npm/v/rethinkdb-ts.svg
[npm-url]: https://www.npmjs.com/package/rethinkdb-ts
[node]: https://img.shields.io/node/v/rethinkdb-ts.svg
[node-url]: https://nodejs.org
[deps]: https://img.shields.io/david/rethinkdb/rethinkdb-ts.svg
[deps-url]: https://david-dm.org/rethinkdb/rethinkdb-ts
[licenses-url]: https://opensource.org/licenses/Apache-2.0
[licenses]: https://img.shields.io/npm/l/rethinkdb-ts.svg
[downloads-url]: https://npmcharts.com/compare/rethinkdb-ts?minimal=true
[downloads]: https://img.shields.io/npm/dm/rethinkdb-ts.svg
[size-url]: https://packagephobia.com/result?p=rethinkdb-ts
[size]: https://packagephobia.com/badge?p=rethinkdb-ts
[build]: https://github.com/rethinkdb/rethinkdb-ts/workflows/Test%20and%20Publish/badge.svg
[build-url]: https://github.com/rethinkdb/rethinkdb-ts/actions?query=workflow%3A%22Test+and+Publish%22
[coverage]: https://coveralls.io/repos/github/rethinkdb/rethinkdb-ts/badge.svg?branch=master
[coverage-url]: https://coveralls.io/github/rethinkdb/rethinkdb-ts?branch=master
