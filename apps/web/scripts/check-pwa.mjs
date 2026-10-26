import assert from 'node:assert/strict';
import { readFile, readdir } from 'node:fs/promises';

const dist = new URL('../dist/', import.meta.url);
const publicData = await readdir(new URL('data/', dist));
assert.deepEqual(
  publicData,
  ['catalog.json'],
  'Only the production catalog may be published',
);
const catalog = JSON.parse(
  await readFile(new URL('data/catalog.json', dist), 'utf8'),
);
assert.equal(catalog.source, 'reference-catalog');
assert.ok(
  catalog.funds.every(
    (fund) =>
      fund.nav === null &&
      fund.changePct === null &&
      fund.history.length === 0 &&
      fund.sourceUrl,
  ),
);
assert.ok(!/DEMO|示例|虚构/.test(JSON.stringify(catalog)));
const manifest = JSON.parse(
  await readFile(new URL('manifest.webmanifest', dist), 'utf8'),
);
assert.equal(manifest.id, '/');
assert.equal(manifest.scope, '/');
assert.equal(manifest.start_url, '/');
assert.equal(manifest.display, 'standalone');
for (const [size, purpose] of [
  [192, undefined],
  [512, undefined],
  [512, 'maskable'],
]) {
  const icon = manifest.icons.find(
    (item) => item.sizes === `${size}x${size}` && item.purpose === purpose,
  );
  assert.ok(icon, `Missing ${size}px ${purpose ?? 'regular'} icon`);
  const png = await readFile(new URL(icon.src.slice(1), dist));
  assert.equal(png.subarray(0, 8).toString('hex'), '89504e470d0a1a0a');
  assert.equal(png.readUInt32BE(16), size);
  assert.equal(png.readUInt32BE(20), size);
}
const html = await readFile(new URL('index.html', dist), 'utf8');
assert.match(html, /rel="manifest"/);
assert.match(html, /apple-touch-icon/);
const sw = await readFile(new URL('sw.js', dist), 'utf8');
for (const path of [
  'index.html',
  'data/catalog.json',
  'manifest.webmanifest',
  'icons/icon-192.png',
  'icons/icon-512.png',
  'icons/maskable-512.png',
]) {
  assert.ok(sw.includes(path), `Offline precache missing ${path}`);
}
const assets = [...html.matchAll(/(?:src|href)="(\/assets\/[^\"]+)"/g)];
assert.ok(assets.some((asset) => asset[1].endsWith('.js')));
assert.ok(assets.some((asset) => asset[1].endsWith('.css')));
for (const asset of assets) {
  assert.ok(
    sw.includes(asset[1].slice(1)),
    `Offline precache missing ${asset[1]}`,
  );
}
assert.match(sw, /SKIP_WAITING/);
const headers = await readFile(new URL('_headers', dist), 'utf8');
assert.match(headers, /\/sw\.js\s+Cache-Control: no-store, must-revalidate/);
console.log(
  'PWA artifact checks passed: manifest, icons, offline shell, JSON, update message and headers.',
);
