/// <reference types="node"/>
/* eslint-disable no-use-before-define */
/**
 * @file configure-prerelease.ts
 *
 * Copied from https://github.com/microsoft/TypeScript/blob/756392c1f514fc3242f16424b1c0857bb6f16421/scripts/build/utils.js
 */

import { normalize, relative } from 'path';
import { readFileSync, writeFileSync } from 'fs';
import assert = require('assert');

/**
 * A minimal description for a parsed package.json object.
 */
interface PackageJson {
  name: string;
  version: string;
  keywords: string[];
}

function main(): void {
  const args = process.argv.slice(2);
  if (args.length < 2) {
    const thisProgramName = relative(process.cwd(), __filename);
    console.log('Usage:');
    console.log(`\tnode ${thisProgramName} <next> <package.json location>`);
    return;
  }

  const tag = args[0];
  if (tag !== 'next' && tag !== 'insiders' && tag !== 'experimental') {
    throw new Error(`Unexpected tag name '${tag}'.`);
  }

  // Acquire the version from the package.json file and modify it appropriately.
  const packageJsonFilePath = normalize(args[1]);
  const packageJsonValue: PackageJson = JSON.parse(
    readFileSync(packageJsonFilePath).toString(),
  );

  const { majorMinor, patch } = parsePackageJsonVersion(
    packageJsonValue.version,
  );
  const prereleasePatch = getPrereleasePatch(tag, patch);

  // Finally write the changes to disk.
  // Modify the package.json structure
  packageJsonValue.version = `${majorMinor}.${prereleasePatch}`;
  writeFileSync(
    packageJsonFilePath,
    JSON.stringify(packageJsonValue, /* replacer: */ undefined, /* space: */ 2),
  );
}

function parsePackageJsonVersion(
  versionString: string,
): { majorMinor: string; patch: string } {
  const versionRgx = /(\d+\.\d+)\.(\d+)($|-)/;
  const match = versionString.match(versionRgx);
  assert(
    match !== null,
    `package.json 'version' should match ${versionRgx.toString()}`,
  );
  return { majorMinor: match![1], patch: match![2] };
}

/** e.g. 0-dev.20170707 */
function getPrereleasePatch(tag: string, plainPatch: string): string {
  // We're going to append a representation of the current time at the end of the current version.
  // String.prototype.toISOString() returns a 24-character string formatted as 'YYYY-MM-DDTHH:mm:ss.sssZ',
  // but we'd prefer to just remove separators and limit ourselves to YYYYMMDD.
  // UTC time will always be implicit here.
  const now = new Date();
  const timeStr = now
    .toISOString()
    .replace(/:[T.-]/g, '')
    .slice(0, 10);

  return `${plainPatch}-${tag}.${timeStr}`;
}

main();
