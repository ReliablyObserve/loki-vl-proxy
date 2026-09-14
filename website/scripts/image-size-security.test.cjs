const test = require('node:test');
const assert = require('node:assert/strict');
const { spawnSync } = require('node:child_process');
const path = require('node:path');

// A regressed parser must kill only this short-lived child, never hang CI.
for (const mode of ['require', 'import']) {
  test(`image-size malformed containers terminate (${mode})`, () => {
    const script = `
      const assert = require('node:assert/strict');
      (async () => {
        const { imageSize } = ${mode === 'require' ? 'require("image-size")' : 'await import("image-size")'};
        const box = (name, payload = Buffer.alloc(0), declared) => {
          const b = Buffer.alloc(8 + payload.length); b.writeUInt32BE(declared ?? b.length); b.write(name, 4); payload.copy(b, 8); return b;
        };
        const icns = (length) => { const b=Buffer.alloc(16); b.write('icns'); b.writeUInt32BE(16,4); b.write('icp4',8); b.writeUInt32BE(length,12); return b; };
        for (const size of [0,1,7,100]) assert.throws(() => imageSize(icns(size)));
        assert.equal(imageSize(icns(8)).width,16);
        const jxl = Buffer.concat([box('JXL ',Buffer.from([13,10,135,10])),box('ftyp',Buffer.from('jxl ')),box('jxlp',Buffer.alloc(4),0)]);
        assert.throws(() => imageSize(jxl));
        const heif = Buffer.concat([box('ftyp',Buffer.from('heic')),box('meta',Buffer.concat([Buffer.alloc(4),box('iprp',box('ipco',box('ispe',Buffer.alloc(12),0)))]))]);
        // A size-zero BMFF box means through EOF: valid zero dimensions here,
        // but must never revisit the same ispe box indefinitely.
        const result = imageSize(heif); assert.equal(result.width,0);
        const png = Buffer.from('iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+aS9sAAAAASUVORK5CYII=', 'base64');
        assert.equal(imageSize(png).width,1);
      })().catch(err => { console.error(err); process.exitCode=1 });
    `;
    const result = spawnSync(process.execPath, ['-e', script], {
      cwd: path.join(__dirname, '..'), timeout: 3000, encoding: 'utf8', maxBuffer: 1024 * 1024,
    });
    assert.ifError(result.error);
    assert.equal(result.status, 0, result.stderr);
  });
}
