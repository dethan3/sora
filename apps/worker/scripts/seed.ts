import { mkdirSync, readFileSync, writeFileSync } from 'node:fs';
import { catalogSeed, renderSql } from '../src/seed.ts';

const catalog = JSON.parse(
  readFileSync(
    new URL('../../web/public/data/catalog.json', import.meta.url),
    'utf8',
  ),
);
mkdirSync(new URL('../.wrangler/', import.meta.url), { recursive: true });
const target = new URL('../.wrangler/seed.sql', import.meta.url);
writeFileSync(
  target,
  renderSql(catalogSeed(catalog, new Date().toISOString())) + '\n',
);
console.log(`已生成种子 SQL：${target.pathname}`);
