import { r, connectPool } from './src';

(async function getData() {
  const pool = await connectPool({
    db: 'root',
    silent: true,
  });
  const result = await pool.run(r.table('test').changes());
  result.on('data', console.log);
  console.log(result);
})();
