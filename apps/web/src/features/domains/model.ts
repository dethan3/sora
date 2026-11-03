import { z } from 'zod';
import rawChips from './ai-chips.json';
import rawStorage from './ai-storage.json';

const text = z.string().min(1);
const symbol = z.string().regex(/^[A-Z0-9.-]{1,16}$/);
const date = z.iso.date();

export const impactTagSchema = z.object({
  label: z.string().min(2).max(16),
  tone: z.enum(['positive', 'headwind', 'watch']),
});

export const domainSchema = z
  .object({
    schemaVersion: z.literal(1),
    id: z.enum(['ai-chips', 'ai-storage']),
    name: text,
    tldr: text.max(120).optional(),
    description: text,
    reviewedAt: date,
    nodes: z
      .array(
        z.object({
          id: text,
          title: z.string().min(2).max(16),
          subtitle: z.string().min(2).max(36),
        }),
      )
      .min(1)
      .optional(),
    companies: z
      .array(
        z.object({
          symbol,
          name: text,
          nodeId: text.optional(),
          role: text,
          roleTag: z.string().min(2).max(12).optional(),
          purityTag: z.string().min(2).max(12).optional(),
          description: text,
          impactTags: z.array(impactTagSchema).min(2).max(5).optional(),
          sourceId: text,
        }),
      )
      .min(1),
    events: z
      .array(
        z.object({
          id: text,
          company: symbol,
          publishedAt: date,
          period: text,
          title: text,
          tags: z.array(z.string().min(2).max(20)).min(1).max(4).optional(),
          fact: text,
          interpretation: text,
          watch: text,
          sourceId: text,
          researchId: z
            .string()
            .regex(/^[a-z0-9-]+$/)
            .nullable(),
        }),
      )
      .min(1),
    fundSnapshots: z
      .array(
        z.object({
          code: symbol,
          asOf: date,
          coverage: z.enum(['full-equity', 'top-ten']),
          reportedEquityCount: z.number().int().min(10).nullable(),
          sourceId: text,
          scope: text,
          description: text,
          holdings: z
            .array(
              z.object({
                symbol,
                name: text,
                weight: z.number().finite().positive().max(100),
              }),
            )
            .min(10),
        }),
      )
      .min(1),
    sources: z
      .array(
        z.object({
          id: text,
          name: text,
          url: z.url().refine((value) => value.startsWith('https://')),
          kind: z.enum(['company-release', 'issuer-holdings']),
        }),
      )
      .min(1),
  })
  .superRefine((domain, context) => {
    const fail = (message: string) =>
      context.addIssue({ code: 'custom', message });
    const unique = (ids: string[]) => new Set(ids).size === ids.length;
    if (
      !unique(domain.companies.map((item) => item.symbol)) ||
      !unique(domain.events.map((item) => item.id)) ||
      !unique(domain.fundSnapshots.map((item) => item.code)) ||
      !unique(domain.sources.map((item) => item.id)) ||
      (domain.nodes && !unique(domain.nodes.map((item) => item.id)))
    )
      fail('领域记录重复');
    if (
      domain.nodes &&
      domain.companies.some(
        (item) =>
          item.nodeId && !domain.nodes!.some((node) => node.id === item.nodeId),
      )
    )
      fail('公司所属产业链环节无效');
    const source = (id: string, kind: string) =>
      domain.sources.some((item) => item.id === id && item.kind === kind);
    if (
      domain.companies.some(
        (item) => !source(item.sourceId, 'company-release'),
      ) ||
      domain.events.some(
        (item) =>
          !source(item.sourceId, 'company-release') ||
          !domain.companies.some((company) => company.symbol === item.company),
      )
    )
      fail('公司或事件来源无效');
    if (
      domain.events.some((item) => item.publishedAt > domain.reviewedAt) ||
      domain.fundSnapshots.some((item) => item.asOf > domain.reviewedAt)
    )
      fail('资料日期不能晚于核对日期');
    for (const snapshot of domain.fundSnapshots) {
      const total = snapshot.holdings.reduce(
        (sum, item) => sum + item.weight,
        0,
      );
      if (
        !source(snapshot.sourceId, 'issuer-holdings') ||
        !unique(snapshot.holdings.map((item) => item.symbol))
      )
        fail('基金持仓来源或标识无效');
      if (
        snapshot.holdings.some(
          (item, index) =>
            index > 0 && item.weight > snapshot.holdings[index - 1].weight,
        )
      )
        fail('持仓须按权重降序');
      if (
        total > 101 ||
        (snapshot.coverage === 'full-equity' &&
          (snapshot.reportedEquityCount !== snapshot.holdings.length ||
            total < 99)) ||
        (snapshot.coverage === 'top-ten' && snapshot.holdings.length !== 10)
      )
        fail('持仓覆盖范围与记录不一致');
    }
  });

export const aiChips = domainSchema.parse(rawChips);
export const aiStorage = domainSchema.parse(rawStorage);
export const domains = [aiChips, aiStorage] as const;
export type Domain = typeof aiChips;
export type DomainId = Domain['id'];
export type DomainCompany = Domain['companies'][number];
export type HoldingSnapshot = Domain['fundSnapshots'][number];

export function getDomain(id?: string | null): Domain {
  return domains.find((item) => item.id === id) ?? aiChips;
}

export function allCompanies(): Array<
  DomainCompany & { domainId: DomainId; domainName: string }
> {
  return domains.flatMap((domain) =>
    domain.companies.map((company) => ({
      ...company,
      domainId: domain.id,
      domainName: domain.name,
    })),
  );
}

export function companyBySymbol(symbolText: string) {
  return allCompanies().find((item) => item.symbol === symbolText);
}

export function sourceById(id: string) {
  for (const domain of domains) {
    const found = domain.sources.find((item) => item.id === id);
    if (found) return found;
  }
  return undefined;
}

export function fundSnapshot(code: string, domain: Domain = aiChips) {
  return domain.fundSnapshots.find((snapshot) => snapshot.code === code);
}

export function holdingFor(snapshot: HoldingSnapshot, company: string) {
  const holding = snapshot.holdings.find((item) => item.symbol === company);
  if (holding) return { status: 'held' as const, weight: holding.weight };
  return {
    status:
      snapshot.coverage === 'full-equity'
        ? ('not-held' as const)
        : ('unknown' as const),
    weight: null,
  };
}

export function topTenWeight(snapshot: HoldingSnapshot) {
  return (
    Math.round(
      snapshot.holdings
        .slice(0, 10)
        .reduce((sum, item) => sum + item.weight, 0) * 100,
    ) / 100
  );
}

export function themeExposure(snapshot: HoldingSnapshot, domainId: DomainId) {
  const target = getDomain(domainId);
  const matched = target.companies.flatMap((company) => {
    const holding = snapshot.holdings.find(
      (item) => item.symbol === company.symbol,
    );
    return holding
      ? [{ symbol: company.symbol, name: company.name, weight: holding.weight }]
      : [];
  });
  const weight =
    Math.round(matched.reduce((sum, item) => sum + item.weight, 0) * 100) / 100;
  return {
    domainId,
    domainName: target.name,
    weight,
    matched,
    coverage: snapshot.coverage,
  };
}

export function fundIngredients(snapshot: HoldingSnapshot) {
  const storage = themeExposure(snapshot, 'ai-storage');
  const chips = themeExposure(snapshot, 'ai-chips');
  const totalReported =
    Math.round(
      snapshot.holdings.reduce((sum, item) => sum + item.weight, 0) * 100,
    ) / 100;
  const otherReported = Math.max(
    0,
    Math.round((totalReported - storage.weight - chips.weight) * 100) / 100,
  );
  return {
    storageWeight: storage.weight,
    storageHoldings: storage.matched,
    chipsWeight: chips.weight,
    chipsHoldings: chips.matched,
    otherReportedWeight: otherReported,
    totalReportedWeight: totalReported,
    coverage: snapshot.coverage,
  };
}

export function equityOverlap(first: HoldingSnapshot, second: HoldingSnapshot) {
  if (first.asOf !== second.asOf)
    return { weight: null, reason: '两份持仓日期不同，暂不计算重叠率。' };
  if (first.coverage !== 'full-equity' || second.coverage !== 'full-equity')
    return { weight: null, reason: '含持仓节选，无法计算完整重叠率。' };
  const weight = first.holdings.reduce(
    (sum, item) =>
      sum +
      Math.min(
        item.weight,
        second.holdings.find((other) => other.symbol === item.symbol)?.weight ??
          0,
      ),
    0,
  );
  return {
    weight: Math.round(weight * 100) / 100,
    reason: '同日权益持仓的最小权重之和，不含现金、衍生品或穿透持仓。',
  };
}

export function companyHref(company: string) {
  const found = companyBySymbol(company);
  const params = new URLSearchParams();
  if (found && found.domainId !== 'ai-chips') {
    params.set('theme', found.domainId);
  }
  params.set('company', company);
  return '/discover?' + params;
}

export type ImpactTag = z.infer<typeof impactTagSchema>;

export interface FundAccessProfile {
  code: string;
  accessTag: string;
  ingredientTag: string;
  recentDrivers: ImpactTag[];
}

const FUND_PROFILES: Record<string, FundAccessProfile> = {
  SOXX: {
    code: 'SOXX',
    accessTag: '美股 ETF · USD 计价',
    ingredientTag: '纯半导体硬件（含 AI 存储 13.6%）',
    recentDrivers: [
      { label: 'AI 算力与 HBM 景气', tone: 'positive' },
      { label: '传统消费芯片温和', tone: 'headwind' },
      { label: '盯晶圆厂资本开支', tone: 'watch' },
    ],
  },
  AIQ: {
    code: 'AIQ',
    accessTag: '美股 ETF · USD 计价',
    ingredientTag: 'AI 软件、云平台与硬件分散',
    recentDrivers: [
      { label: '云软件与 AI 应用落地', tone: 'positive' },
      { label: '前十硬件纯度较低', tone: 'headwind' },
      { label: '盯软件变现增速', tone: 'watch' },
    ],
  },
  QQQ: {
    code: 'QQQ',
    accessTag: '美股宽基 · USD 计价',
    ingredientTag: '纳指100 大型科技与云巨头',
    recentDrivers: [
      { label: '云巨头 AI 资本开支', tone: 'positive' },
      { label: '头部科技权重集中', tone: 'headwind' },
      { label: '盯美股科技财报季', tone: 'watch' },
    ],
  },
  IVV: {
    code: 'IVV',
    accessTag: '美股宽基 · USD 计价',
    ingredientTag: '标普500 美国大型龙头宽基',
    recentDrivers: [
      { label: '美股盈利基本面稳健', tone: 'positive' },
      { label: '受宏观利率预期牵动', tone: 'headwind' },
      { label: '盯非科技板块轮动', tone: 'watch' },
    ],
  },
  '2800.HK': {
    code: '2800.HK',
    accessTag: '港股 ETF · HKD 港元柜台',
    ingredientTag: '恒生指数蓝筹（金融+互联网）',
    recentDrivers: [
      { label: '互联网龙头回购分红', tone: 'positive' },
      { label: '外资流向与汇率波动', tone: 'headwind' },
      { label: '盯港股流动性与内需', tone: 'watch' },
    ],
  },
  IAU: {
    code: 'IAU',
    accessTag: '美股实物信托 · USD 计价',
    ingredientTag: '100% 实物黄金价格敞口',
    recentDrivers: [
      { label: '央行购金与避险需求', tone: 'positive' },
      { label: '美元与实际利率波动', tone: 'headwind' },
      { label: '不派息且非金矿股', tone: 'watch' },
    ],
  },
};

export function fundAccessProfile(code: string): FundAccessProfile | undefined {
  return FUND_PROFILES[code];
}

export function summarizePortfolioImpacts(fundCodes: string[]) {
  const uniqueCodes = Array.from(new Set(fundCodes));
  const companyMap = new Map<
    string,
    {
      symbol: string;
      name: string;
      roleTag?: string;
      funds: Array<{ code: string; weight: number }>;
      impactTags: ImpactTag[];
    }
  >();
  const verifiedFundCodes: string[] = [];
  const broadFundProfiles: FundAccessProfile[] = [];

  for (const code of uniqueCodes) {
    const snapshot = fundSnapshot(code);
    if (snapshot) {
      verifiedFundCodes.push(code);
      for (const company of allCompanies()) {
        const holding = snapshot.holdings.find(
          (item) => item.symbol === company.symbol,
        );
        if (!holding) continue;
        const existing = companyMap.get(company.symbol);
        if (existing) {
          if (!existing.funds.some((item) => item.code === code)) {
            existing.funds.push({ code, weight: holding.weight });
          }
        } else {
          companyMap.set(company.symbol, {
            symbol: company.symbol,
            name: company.name,
            roleTag: company.roleTag,
            funds: [{ code, weight: holding.weight }],
            impactTags: company.impactTags ?? [],
          });
        }
      }
    } else {
      const profile = fundAccessProfile(code);
      if (profile) broadFundProfiles.push(profile);
    }
  }

  const companies = Array.from(companyMap.values());
  const positiveTags: Array<{ source: string; label: string }> = [];
  const headwindTags: Array<{ source: string; label: string }> = [];
  const watchTags: Array<{ source: string; label: string }> = [];

  for (const company of companies) {
    for (const tag of company.impactTags) {
      const entry = { source: company.symbol, label: tag.label };
      if (tag.tone === 'positive') positiveTags.push(entry);
      else if (tag.tone === 'headwind') headwindTags.push(entry);
      else watchTags.push(entry);
    }
  }

  for (const profile of broadFundProfiles) {
    for (const tag of profile.recentDrivers) {
      const entry = { source: profile.code, label: tag.label };
      if (tag.tone === 'positive') positiveTags.push(entry);
      else if (tag.tone === 'headwind') headwindTags.push(entry);
      else watchTags.push(entry);
    }
  }

  return {
    verifiedFundCodes,
    broadFundProfiles,
    companies,
    positiveTags: positiveTags.slice(0, 16),
    headwindTags: headwindTags.slice(0, 16),
    watchTags: watchTags.slice(0, 16),
  };
}
