import { readFile } from 'node:fs/promises';
import { groth16 } from 'snarkjs';

const [, , command, artifactDir] = process.argv;

async function run() {
  const artifacts = (file) => `${artifactDir}/${file}`;
  if (command === 'check-prover') {
    await Promise.all([
      readFile(artifacts('reputation_js/reputation.wasm')),
      readFile(artifacts('reputation.zkey')),
    ]);
    return true;
  }

  if (command === 'check-verifier' || command === 'verify') {
    const key = JSON.parse(await readFile(artifacts('verification_key.json'), 'utf8'));
    if (key.protocol !== 'groth16' || key.curve !== 'bn128' || key.nPublic !== 2) {
      throw new Error('Unexpected reputation verification key');
    }
    if (command === 'check-verifier') return true;
    const { public_signals, proof_data } = await readInput();
    return groth16.verify(key, public_signals, proof_data);
  }

  if (command === 'prove') {
    const witness = await readInput();
    return groth16.fullProve(
      witness,
      artifacts('reputation_js/reputation.wasm'),
      artifacts('reputation.zkey'),
      undefined,
      undefined,
      { singleThread: true },
    );
  }

  throw new Error('Unknown reputation ZKP command');
}

async function readInput() {
  const chunks = [];
  for await (const chunk of process.stdin) chunks.push(chunk);
  return JSON.parse(Buffer.concat(chunks));
}

try {
  const result = await run();
  // Wait for stdout before terminating snarkjs's worker threads.
  process.stdout.write(JSON.stringify(result), () => process.exit(0));
} catch {
  // A witness may appear in library errors. Keep it out of service logs.
  process.stderr.write('Reputation ZKP operation failed\n', () => process.exit(1));
}
