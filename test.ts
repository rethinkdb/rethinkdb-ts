import { r } from './src';

// export class Server {
//   private pool!: MasterPool;

//   public async connect() {
//     const options = {
//       host: 'localhost',
//       port: 28015,
//       db: 'catalog',
//       silent: false,
//       pingInterval: 1,
//       waitForHealthy: false
//     };

//     // these are never called even though the server are taken down,
//     // or not started at program start but are made available later...

//     this.pool = await r.connectPool(options);
//     this.pool.on('error', e => {
//       console.log('error', e);
//     });
//     this.pool.on('size', e => {
//       console.log('size', e);
//     });
//     this.pool.on('available-size', e => {
//       console.log('available-size', e);
//     });
//     this.pool.on('healthy', e => {
//       console.log('healthy', e);
//     });
//   }

//   public check() {
//     setInterval(async () => {
//       // this will continue to print true, 1, 1 even if the server goes down...
//       console.log(this.pool.isHealthy);
//       console.log(this.pool.getAvailableLength());
//       console.log(this.pool.getLength());
//       const result = await r
//         .table('PhoneCatalog')
//         .filter({ callsign: 'xxx' })
//         .run()
//         .catch(what => console.log('catch', what));
//       console.log(result);
//     }, 1000 * 5);
//   }
// }

// const s = new Server();
// s.connect();
// s.check();
r.connectPool({
  host: '127.0.0.1',
  port: 28015,
  db: 'test',
  silent: true,
});
(async function getData() {
  const pool = r.getPoolMaster();
  if (pool) {
    await pool.waitForHealthy();
  }
  const result = await r.table('test').run();
  console.log(JSON.stringify(result));
})();
