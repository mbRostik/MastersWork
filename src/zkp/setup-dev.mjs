import { mkdirSync, mkdtempSync, rmSync, copyFileSync } from 'node:fs';
import { join, relative, resolve } from 'node:path';
import { randomBytes } from 'node:crypto';
import { execFileSync } from 'node:child_process';

const artifacts = resolve('artifacts');
mkdirSync(artifacts, { recursive: true });
const work = mkdtempSync(join(artifacts, 'setup-'));
const local = (name) => join(work, name);
const snark = (...args) => execFileSync(process.execPath, [
  resolve('node_modules/snarkjs/build/cli.cjs'), ...args,
], { stdio: 'inherit' });
const contribution = () => `-e=${randomBytes(32).toString('hex')}`;

try {
  execFileSync(process.execPath, [resolve('node_modules/circom2/cli.js'),
    'reputation.circom', '--r1cs', '--wasm', '--sym', '-o', relative(process.cwd(), work),
  ], { stdio: 'inherit' });

  snark('powersoftau', 'new', 'bn128', '11', local('initial.ptau'));
  snark('powersoftau', 'contribute', local('initial.ptau'), local('contributed.ptau'),
    '--name=Local development phase 1', contribution());
  snark('powersoftau', 'prepare', 'phase2', local('contributed.ptau'), local('final.ptau'));
  snark('groth16', 'setup', local('reputation.r1cs'), local('final.ptau'), local('initial.zkey'));
  snark('zkey', 'contribute', local('initial.zkey'), local('reputation.zkey'),
    '--name=Local development phase 2', contribution());
  snark('zkey', 'verify', local('reputation.r1cs'), local('final.ptau'), local('reputation.zkey'));
  snark('zkey', 'export', 'verificationkey', local('reputation.zkey'), local('verification_key.json'));

  mkdirSync(join(artifacts, 'reputation_js'), { recursive: true });
  for (const file of ['reputation.r1cs', 'reputation_js/reputation.wasm',
    'reputation.zkey', 'verification_key.json']) {
    copyFileSync(local(file), join(artifacts, file));
  }
} finally {
  rmSync(work, { recursive: true });
}
