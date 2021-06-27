# rethinkdb-ts
[![npm][npm]][npm-url]
[![node][node]][node-url]
[![deps][deps]][deps-url]
[![licenses][licenses]][licenses-url]
[![downloads][downloads]][downloads-url]
[![size][size]][size-url]
[![build][build]][build-url]

Most supported rethinkdb driver with the best typings and maintenance. Initially based on `rethinkdbdash` but drastically changed from it, so do not expect the same behaviour with that package. 
It is rebuilt from scratch using the latest ES/TS features for readability and maintainability.


## Install

`npm i rethinkdb-ts`

or

`yarn add rethinkdb-ts`

## Import

```typescript
// if you support import
import { r } from 'rethinkdb-ts';
// if you dont
const { r } = require('rethinkdb-ts');
```

## Initialize

```typescript
import { connect, connectPool, MasterConnectionPool, RethinkDBConnection } from 'rethinkdb-ts';

// in an async context

// Old methods

// if you want to initialize a single connection
const connection = await connect(options);

// if you want to initialize a connection pool
const poll = await connectPool(options);

// New methods with separate connection
const newConnection = new RethinkDBConnection({}, { db: 'test' });
await newConnection.reconnect(); // connected

const newPool = new MasterConnectionPool([], { db: 'test' }); // connects instantly
await newPool.waitForHealthy(); // wait till connection is ready
```

# Features:

- `r` object is now only used for query generation, most of the connection logic is moved out of `r` context.

- Queries as objects. Queries can be generated once and used as many times as needed. Queries are not runnable, not promises, all the execution (`.run`) is done by connection objects.

- Query serialization. You can store the query by calling `import { deserialize, serialize } from 'rethinkdb-ts'` functions. and get it like this `deserialize(serializedQuery)` or even `deserialize<RStream>(serializedQuery).reduce(...).run()` the serialized query is a normal string so you can store it in the DB. No need for ugly workarounds like `.toString` and `eval` anymore. Also the serialized query is the actual JSON that gets sent to the server so it should be cross-language compatible if any other driver cares to implement it.

- Query generation in browsers. Though it is mostly a bad idea to make calls to the DB in browsers, you can generate and send a serialized query from browser.

- Worth admitting that serialization under the hood only consumes `query.term` field, which contains plain json query object, which can also be saved separately and converted back to query with `toQuery` function from `rethinkdb-ts/lib/query-builder/query`

- Multiple connection pools.

# Changes from `rethinkdbdash`

- Support for complex socket configuration + tls (notice that for SSL/TLS or any configuration more complex than `{ host: '...', port: '...' }` you'll have to encapsulate in a server/servers property: 
```typescript
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
Connection inherits nodejs net/tls connections, so you can provide any options they consume.
If you want an SSL/TLS, add `tls: true` and the options described [here](https://nodejs.org/dist/latest-v10.x/docs/api/tls.html#tls_tls_connect_options_callback).

- Importing property instead of entire library: `const {r} = require('rethinkdb-ts')` or `import {r} from 'rethinkdb-ts'` instead of `const r = require('rethinkdbdash')(options)`
- No top level initialization, initializing a pool is done by `await connectPool()`
- No `{ cursor: true }` option, for getting a cursor use `.getCursor(query, runOptions)` instead of `.run(runOptions)`
  - `.run(query)` will coerce streams to array by default. Feeds will return a cursor like `rethinkdbdash`
- Uses native promises instead of `bluebird`
- A cursor is already a readable stream, no need for `toStream()`
- A readable stream is already an async iterator in node 10 no need for `.asyncIterator()`
- In a connection pool, reusing open connections that already run queries instead of making queries wait for a connection when max connections exceeded
- Integrated fully encompassing type definitions

# DROPPING SUPPORT:

- Support node < 10
- Support callbacks
- Support browsers (Unless it's the only demand of making this driver used instead of `rethinkdbdash`)
- Support write streams (Does anyone use it? Will add it if it's a popular demand)

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
