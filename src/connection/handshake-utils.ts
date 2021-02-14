import { createHash, createHmac, pbkdf2, randomBytes } from 'crypto';
import { promisify } from 'util';
import { RethinkDBError, RethinkDBErrorType } from '../error';
import { Version } from '../proto/enums';

export const NULL_BUFFER = Buffer.from('\0', 'binary');
const PROTOCOL_VERSION = 0;
const AUTHENTICATION_METHOD = 'SCRAM-SHA-256';
const KEY_LENGTH = 32; // Because we are currently using SHA 256
const CACHE_PBKDF2: { [cacheKey: string]: Buffer } = {};

const pbkdf2Async = promisify(pbkdf2);

function xorBuffer(a: Buffer, b: Buffer) {
  const result = [];
  const len = Math.min(a.length, b.length);
  for (let i = 0; i < len; i += 1) {
    // eslint-disable-next-line no-bitwise
    result.push(a[i] ^ b[i]);
  }
  return Buffer.from(result);
}

type AuthBufferPair = { authBuffer: Buffer; randomString: string };

export function buildAuthBuffer(user: string): AuthBufferPair {
  const versionBuffer = Buffer.alloc(4);
  versionBuffer.writeInt32LE(Version.V1_0, 0);
  const randomString = randomBytes(18).toString('base64');
  const mainBuffer = Buffer.from(
    JSON.stringify({
      protocol_version: PROTOCOL_VERSION,
      authentication_method: AUTHENTICATION_METHOD,
      authentication: `n,,n=${user},r=${randomString}`,
    }),
  );

  const authBuffer = Buffer.concat([versionBuffer, mainBuffer, NULL_BUFFER]);
  return { authBuffer, randomString };
}

type VersionMessage = {
  // eslint-disable-next-line camelcase
  max_protocol_version: number;
  // eslint-disable-next-line camelcase
  min_protocol_version: number;
  // eslint-disable-next-line camelcase
  server_version: string;
};

export function validateVersion(msg: VersionMessage): void {
  if (
    msg.max_protocol_version < PROTOCOL_VERSION ||
    msg.min_protocol_version > PROTOCOL_VERSION
  ) {
    throw new RethinkDBError('Unsupported protocol version', {
      type: RethinkDBErrorType.UNSUPPORTED_PROTOCOL,
    });
  }
}

async function getSaltedPassword(
  password: Buffer,
  salt: Buffer,
  iterations: number,
): Promise<Buffer> {
  const cacheKey = `${password.toString('base64')},${salt.toString(
    'base64',
  )},${iterations}`;
  if (!CACHE_PBKDF2[cacheKey]) {
    CACHE_PBKDF2[cacheKey] = await pbkdf2Async(
      password,
      salt,
      iterations,
      KEY_LENGTH,
      'sha256',
    );
  }
  return CACHE_PBKDF2[cacheKey];
}

export async function computeSaltedPassword(
  authString: string,
  randomString: string,
  user: string,
  password: Buffer,
): Promise<{ serverSignature: string; proof: Buffer }> {
  const [randomNonce, s, i] = authString
    .split(',')
    .map((part) => part.substring(2));
  const salt = Buffer.from(s, 'base64');
  if (randomNonce.substring(0, randomString.length) !== randomString) {
    throw new RethinkDBError('Invalid nonce from server', {
      type: RethinkDBErrorType.AUTH,
    });
  }

  const saltedPassword = await getSaltedPassword(
    password,
    salt,
    Number.parseInt(i, 10),
  );

  const clientFinalMessageWithoutProof = `c=biws,r=${randomNonce}`;
  const clientKey = createHmac('sha256', saltedPassword)
    .update('Client Key')
    .digest();
  const storedKey = createHash('sha256').update(clientKey).digest();

  const authMessage = `n=${user},r=${randomString},${authString},${clientFinalMessageWithoutProof}`;

  const clientSignature = createHmac('sha256', storedKey)
    .update(authMessage)
    .digest();

  const serverKey = createHmac('sha256', saltedPassword)
    .update('Server Key')
    .digest();

  const serverSignature = createHmac('sha256', serverKey)
    .update(authMessage)
    .digest()
    .toString('base64');

  const clientProof = xorBuffer(clientKey, clientSignature).toString('base64');
  const authentication = `${clientFinalMessageWithoutProof},p=${clientProof}`;
  return {
    serverSignature,
    proof: Buffer.concat([
      Buffer.from(JSON.stringify({ authentication })),
      NULL_BUFFER,
    ]),
  };
}

export function compareDigest(
  authentication: string,
  serverSignature: string,
): void {
  if (
    authentication.substring(authentication.indexOf('=') + 1) !==
    serverSignature
  ) {
    throw new RethinkDBError('Invalid server signature', {
      type: RethinkDBErrorType.AUTH,
    });
  }
}
