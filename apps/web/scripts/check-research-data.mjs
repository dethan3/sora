import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';

const catalog = JSON.parse(
  await readFile(
    new URL('../public/data/catalog.json', import.meta.url),
    'utf8',
  ),
);
const aiChips = JSON.parse(
  await readFile(
    new URL('../src/features/domains/ai-chips.json', import.meta.url),
    'utf8',
  ),
);
const aiStorage = JSON.parse(
  await readFile(
    new URL('../src/features/domains/ai-storage.json', import.meta.url),
    'utf8',
  ),
);
const research = JSON.parse(
  await readFile(
    new URL('../src/features/research/research.json', import.meta.url),
    'utf8',
  ),
);

const fundCodes = new Set(catalog.funds.map((fund) => fund.code));
const moduleIds = new Set(catalog.modules.map((mod) => mod.id));
const signalIds = new Set(research.signals.map((signal) => signal.id));

for (const domain of [aiChips, aiStorage]) {
  assert.ok(
    domain.tldr && domain.tldr.length <= 120,
    `${domain.id}: missing or overlong tldr`,
  );
  assert.ok(
    Array.isArray(domain.nodes) && domain.nodes.length >= 1,
    `${domain.id}: must define value chain nodes`,
  );
  const nodeIds = new Set(domain.nodes.map((node) => node.id));
  const sourceIds = new Set(domain.sources.map((src) => src.id));

  for (const src of domain.sources) {
    assert.ok(
      src.url.startsWith('https://'),
      `${domain.id} source ${src.id}: must use https://`,
    );
  }

  for (const company of domain.companies) {
    assert.ok(
      nodeIds.has(company.nodeId),
      `${domain.id} company ${company.symbol}: invalid nodeId ${company.nodeId}`,
    );
    assert.ok(
      company.roleTag && company.roleTag.length <= 12,
      `${domain.id} company ${company.symbol}: roleTag must be <= 12 chars`,
    );
    assert.ok(
      company.purityTag && company.purityTag.length <= 12,
      `${domain.id} company ${company.symbol}: purityTag must be <= 12 chars`,
    );
    assert.ok(
      Array.isArray(company.impactTags) &&
        company.impactTags.length >= 2 &&
        company.impactTags.length <= 5,
      `${domain.id} company ${company.symbol}: must have 2..5 impactTags`,
    );
    for (const tag of company.impactTags) {
      assert.ok(
        tag.label.length >= 2 && tag.label.length <= 16,
        `${domain.id} company ${company.symbol}: impactTag "${tag.label}" exceeds 16 chars`,
      );
      assert.ok(
        ['positive', 'headwind', 'watch'].includes(tag.tone),
        `${domain.id} company ${company.symbol}: invalid tone ${tag.tone}`,
      );
    }
    assert.ok(
      sourceIds.has(company.sourceId),
      `${domain.id} company ${company.symbol}: missing sourceId`,
    );
  }

  for (const event of domain.events) {
    assert.ok(
      event.publishedAt <= domain.reviewedAt,
      `${domain.id} event ${event.id}: publishedAt > reviewedAt`,
    );
    assert.ok(
      Array.isArray(event.tags) &&
        event.tags.length >= 1 &&
        event.tags.length <= 4,
      `${domain.id} event ${event.id}: must have 1..4 concise tags`,
    );
    if (event.researchId !== null) {
      assert.ok(
        signalIds.has(event.researchId),
        `${domain.id} event ${event.id}: unknown researchId ${event.researchId}`,
      );
    }
  }

  for (const snapshot of domain.fundSnapshots) {
    assert.ok(
      fundCodes.has(snapshot.code),
      `${domain.id} snapshot ${snapshot.code}: not in catalog.json`,
    );
    assert.ok(
      snapshot.asOf <= domain.reviewedAt,
      `${domain.id} snapshot ${snapshot.code}: asOf > reviewedAt`,
    );
  }
}

for (const signal of research.signals) {
  assert.ok(
    ['ai-chips', 'ai-storage'].includes(signal.themeId),
    `signal ${signal.id}: missing or invalid themeId`,
  );
  assert.ok(
    Array.isArray(signal.prosTags) &&
      signal.prosTags.length >= 1 &&
      signal.prosTags.every((tag) => tag.length <= 20),
    `signal ${signal.id}: invalid prosTags`,
  );
  assert.ok(
    Array.isArray(signal.consTags) &&
      signal.consTags.length >= 1 &&
      signal.consTags.every((tag) => tag.length <= 20),
    `signal ${signal.id}: invalid consTags`,
  );
  for (const dirId of signal.directionIds) {
    assert.ok(
      moduleIds.has(dirId),
      `signal ${signal.id}: unknown directionId ${dirId}`,
    );
  }
  for (const fund of signal.funds) {
    assert.ok(
      fundCodes.has(fund.code),
      `signal ${signal.id}: unknown fund ${fund.code}`,
    );
  }
}

console.log(
  'Research & domain data checks passed: concise tags, dates, sources and fund mappings verified.',
);
