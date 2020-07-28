import { createHash, createHmac, pbkdf2, randomBytes } from 'crypto';
import { promisify } from 'util';
import { RethinkDBErrorType } from '..';
import { RethinkDBError } from '../error/error';
import { Version } from '../proto/enums';

export const NULL_BUFFER = Buffer.from('\0', 'binary');
const PROTOCOL_VERSION = 0;
const AUTHENTIFICATION_METHOD = 'SCRAM-SHA-256';
const KEY_LENGTH = 32; // Because we are currently using SHA 256
const CACHE_PBKDF2: { [cacheKey: string]: Buffer } = {};

function xorBuffer(a: Buffer, b: Buffer) {
  const result = [];
  const len = Math.min(a.length, b.length);
  for (let i = 0; i < len; i++) {
    // tslint:disable-next-line:no-bitwise
    result.push(a[i] ^ b[i]);
  }
  return Buffer.from(result);
}

export function buildAuthBuffer(user: string) {
  const versionBuffer = Buffer.alloc(4);
  versionBuffer.writeInt32LE(Version.V1_0, 0);
  const randomString = randomBytes(18).toString('base64');
  const mainBuffer = Buffer.from(
    JSON.stringify({
      protocol_version: PROTOCOL_VERSION,
      authentication_method: AUTHENTIFICATION_METHOD,
      authentication: `n,,n=${user},r=${randomString}`,
    }),
  );
  return {
    randomString,
    authBuffer: Buffer.concat([versionBuffer, mainBuffer, NULL_BUFFER]),
  };
}

export function validateVersion(msg: {
  max_protocol_version: number;
  min_protocol_version: number;
  server_version: string;
}) {
  if (
    msg.max_protocol_version < PROTOCOL_VERSION ||
    msg.min_protocol_version > PROTOCOL_VERSION
  ) {
    throw new RethinkDBError('Unsupported protocol version', {
      type: RethinkDBErrorType.UNSUPPORTED_PROTOCOL,
    });
  }
}

export async function computeSaltedPassword(
  authentication: string,
  randomString: string,
  user: string,
  password: Buffer,
) {
  const [randomNonce, s, i] = authentication
    .split(',')
    .map((part) => part.substring(2));
  const salt = Buffer.from(s, 'base64');
  const iterations = parseInt(i, 10);
  if (randomNonce.substring(0, randomString.length) !== randomString) {
    throw new RethinkDBError('Invalid nonce from server', {
      type: RethinkDBErrorType.AUTH,
    });
  }
  const cacheKey = `${password.toString('base64')},${salt.toString(
    'base64',
  )},${iterations}`;
  const saltedPassword =
    CACHE_PBKDF2[cacheKey] ||
    (await promisify(pbkdf2)(password, salt, iterations, KEY_LENGTH, 'sha256'));
  CACHE_PBKDF2[cacheKey] = saltedPassword;

  const clientFinalMessageWithoutProof = `c=biws,r=${randomNonce}`;
  const clientKey = createHmac('sha256', saltedPassword)
    .update('Client Key')
    .digest();
  const storedKey = createHash('sha256').update(clientKey).digest();

  const authMessage = `n=${user},r=${randomString},${authentication},${clientFinalMessageWithoutProof}`;

  const clientSignature = createHmac('sha256', storedKey)
    .update(authMessage)
    .digest();
  const clientProof = xorBuffer(clientKey, clientSignature);

  const serverKey = createHmac('sha256', saltedPassword)
    .update('Server Key')
    .digest();
  const serverSignature = createHmac('sha256', serverKey)
    .update(authMessage)
    .digest()
    .toString('base64');

  return {
    serverSignature,
    proof: Buffer.concat([
      Buffer.from(
        JSON.stringify({
          authentication: `${clientFinalMessageWithoutProof},p=${clientProof.toString(
            'base64',
          )}`,
        }),
      ),
      NULL_BUFFER,
    ]),
  };
}

export function compareDigest(authentication: string, serverSignature: string) {
  if (
    authentication.substring(authentication.indexOf('=') + 1) !==
    serverSignature
  ) {
    throw new RethinkDBError('Invalid server signature', {
      type: RethinkDBErrorType.AUTH,
    });
  }
}
