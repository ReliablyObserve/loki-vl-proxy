// Temporary, integrity-checked patch for image-size 2.0.2. See ../SECURITY.md.
const fs = require('node:fs');
const path = require('node:path');
const crypto = require('node:crypto');
const manifest = require('./image-size-patch-manifest.json');
const root = path.dirname(require.resolve('image-size'));
const pkg = JSON.parse(fs.readFileSync(path.join(root, '../package.json'), 'utf8'));
if (pkg.version !== '2.0.2') throw new Error('Reassess the image-size security patch after dependency upgrades');
const hash = (value) => crypto.createHash('sha256').update(value).digest('hex');
const updates = [];
for (const [file, expected] of Object.entries(manifest)) {
  const target = path.join(root, file);
  const original = fs.readFileSync(target, 'utf8');
  if (hash(original) === expected.after) continue;
  if (hash(original) !== expected.before) throw new Error(`Unexpected image-size source: ${file}`);
  const patched = original
    .replaceAll('if (input.length - offset < 4) return;', 'if (input.length - offset < 8) return;')
    .replaceAll('const boxSize = readUInt32BE(input, offset);',
      'const declaredSize = readUInt32BE(input, offset);\n  const boxSize = declaredSize === 0 ? input.length - offset : declaredSize;\n  if (boxSize < 8) return;')
    .replaceAll('const imageHeader = readImageHeader(input, imageOffset);',
      'if (imageOffset + SIZE_HEADER > inputLength) throw new TypeError("Invalid ICNS entry header");\n      const imageHeader = readImageHeader(input, imageOffset);\n      if (imageHeader[1] < SIZE_HEADER || imageHeader[1] > fileLength - imageOffset) throw new TypeError("Invalid ICNS entry length");');
  if (hash(patched) !== expected.after) throw new Error(`Incomplete image-size patch: ${file}`);
  updates.push([target, patched]);
}
// Validate the entire installation before making any change. Never accept a
// partially matching future package or silently skip a bundled entry point.
for (const [target, patched] of updates) fs.writeFileSync(target, patched);
console.log(`image-size: ${Object.keys(manifest).length} patched bundles verified`);
