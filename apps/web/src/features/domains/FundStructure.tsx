import { Link } from 'react-router';
import {
  aiChips,
  allCompanies,
  companyHref,
  fundAccessProfile,
  fundIngredients,
  fundSnapshot,
  sourceById,
  summarizePortfolioImpacts,
  topTenWeight,
} from './model';
import './domains.css';

export function DomainSource({ id }: { id: string }) {
  const source = sourceById(id) ?? aiChips.sources[0];
  return (
    <a
      className="domain-source"
      href={source.url}
      target="_blank"
      rel="noreferrer"
    >
      {source.name} ↗
    </a>
  );
}

export function ImpactPill({
  label,
  tone,
}: {
  label: string;
  tone: 'positive' | 'headwind' | 'watch';
}) {
  const prefix =
    tone === 'positive' ? '🟢 ' : tone === 'headwind' ? '🟠 ' : '🔍 ';
  return (
    <span className={`impact-pill impact-${tone}`}>
      {prefix}
      {label}
    </span>
  );
}

export function PortfolioImpactHealthCard({
  codes,
  title = '组合近期核心影响体检',
}: {
  codes: string[];
  title?: string;
}) {
  const summary = summarizePortfolioImpacts(codes);
  if (
    summary.companies.length === 0 &&
    summary.broadFundProfiles.length === 0
  ) {
    return null;
  }
  return (
    <section
      className="portfolio-health-card domain-surface"
      aria-label={title}
    >
      <div className="domain-section-title">
        <div>
          <p className="eyebrow">IMPACT HEALTH CHECK</p>
          <h2>{title}</h2>
        </div>
      </div>
      <p className="domain-lead">
        {summary.verifiedFundCodes.length > 0 ? (
          <>
            已穿透 <strong>{summary.verifiedFundCodes.join('、')}</strong>{' '}
            底层关联的 <strong>{summary.companies.length}</strong> 家核心公司
            {summary.broadFundProfiles.length > 0
              ? `，并结合 ${summary.broadFundProfiles.map((item) => item.code).join('、')} 的宽基/资产属性`
              : ''}
            ，汇总近期主要驱动与风险：
          </>
        ) : (
          <>
            基于{' '}
            <strong>
              {summary.broadFundProfiles.map((item) => item.code).join('、')}
            </strong>{' '}
            的跟踪基准与资产特征，近期主要关注以下因素：
          </>
        )}
      </p>
      <div className="portfolio-health-grid">
        {summary.positiveTags.length > 0 && (
          <div className="health-col">
            <small>🟢 顺风利好驱动</small>
            <div className="impact-pill-row">
              {summary.positiveTags.map((item) => (
                <ImpactPill
                  key={`${item.source}-${item.label}`}
                  label={`${item.source} · ${item.label}`}
                  tone="positive"
                />
              ))}
            </div>
          </div>
        )}
        {summary.headwindTags.length > 0 && (
          <div className="health-col">
            <small>🟠 逆风风险提醒</small>
            <div className="impact-pill-row">
              {summary.headwindTags.map((item) => (
                <ImpactPill
                  key={`${item.source}-${item.label}`}
                  label={`${item.source} · ${item.label}`}
                  tone="headwind"
                />
              ))}
            </div>
          </div>
        )}
        {summary.watchTags.length > 0 && (
          <div className="health-col">
            <small>🔍 下一步盯防</small>
            <div className="impact-pill-row">
              {summary.watchTags.map((item) => (
                <ImpactPill
                  key={`${item.source}-${item.label}`}
                  label={`${item.source} · ${item.label}`}
                  tone="watch"
                />
              ))}
            </div>
          </div>
        )}
      </div>
      {summary.broadFundProfiles.length > 0 && (
        <p className="domain-footnote">
          边界说明：
          {summary.broadFundProfiles.map((item) => item.code).join('、')}{' '}
          暂未收录单股持仓明细，仅展示其跟踪基准层面的方向影响，不估算单股配料权重。
        </p>
      )}
    </section>
  );
}

export function FundStructure({ code }: { code: string }) {
  const snapshot = fundSnapshot(code);
  const profile = fundAccessProfile(code);
  if (!snapshot) {
    if (!profile) return null;
    return (
      <section
        className="fund-structure domain-surface"
        aria-label="基金配料与近期影响"
      >
        <div className="domain-section-title">
          <div>
            <p className="eyebrow">FUND PROFILE & DRIVERS</p>
            <h2>几秒看清：这只基金买什么、近期受什么影响</h2>
          </div>
        </div>
        <div className="fund-access-badges">
          <span className="role-badge">{profile.accessTag}</span>
          <span className="purity-badge">{profile.ingredientTag}</span>
        </div>
        <div className="holding-impacts-panel" aria-label="基金近期影响">
          <h3>近期核心影响标签</h3>
          <div className="impact-pill-row">
            {profile.recentDrivers.map((tag) => (
              <ImpactPill key={tag.label} label={tag.label} tone={tag.tone} />
            ))}
          </div>
        </div>
        <p className="domain-footnote">
          边界说明：当前未收录 {code}{' '}
          带日期的单股持仓明细，不推算底层单一科技主题权重。
        </p>
      </section>
    );
  }
  const ingredients = fundIngredients(snapshot);
  const coveredHoldings = allCompanies().flatMap((company) => {
    const match = snapshot.holdings.find(
      (item) => item.symbol === company.symbol,
    );
    return match ? [{ ...company, weight: match.weight }] : [];
  });
  return (
    <section
      className="fund-structure domain-surface"
      aria-label="基金持仓结构"
    >
      <div className="domain-section-title">
        <div>
          <p className="eyebrow">FUND COMPOSITION</p>
          <h2>先看这只基金，实际持有什么</h2>
        </div>
        <div className="fund-structure-links">
          <Link className="text-link" to="/discover?theme=ai-storage">
            AI 存储产业链 →
          </Link>
          <Link className="text-link" to="/discover">
            AI 芯片领域 →
          </Link>
        </div>
      </div>
      <p className="domain-lead">{snapshot.description}</p>

      <div className="ingredient-card" aria-label="基金主题配料表">
        <div className="ingredient-header">
          <strong>基金配料表（按已核对持仓拆解）</strong>
          <span>
            {snapshot.coverage === 'full-equity'
              ? '基于全部 30 只权益持仓'
              : '基于前十大持仓节选'}
          </span>
        </div>
        <div className="ingredient-bar" aria-hidden="true">
          {ingredients.storageWeight > 0 && (
            <span
              className="ingredient-seg seg-storage"
              style={{ width: `${ingredients.storageWeight}%` }}
            />
          )}
          {ingredients.chipsWeight > 0 && (
            <span
              className="ingredient-seg seg-chips"
              style={{ width: `${ingredients.chipsWeight}%` }}
            />
          )}
          <span
            className="ingredient-seg seg-other"
            style={{
              width: `${Math.max(8, 100 - ingredients.storageWeight - ingredients.chipsWeight)}%`,
            }}
          />
        </div>
        <div className="ingredient-legend">
          <span>
            <i className="dot-storage" aria-hidden="true" />
            AI 存储链{' '}
            <b>
              {ingredients.storageWeight > 0
                ? `${ingredients.storageWeight.toFixed(2)}%`
                : snapshot.coverage === 'top-ten'
                  ? '前十未见'
                  : '0.00%'}
            </b>
          </span>
          <span>
            <i className="dot-chips" aria-hidden="true" />
            AI 芯片与光刻 <b>{ingredients.chipsWeight.toFixed(2)}%</b>
          </span>
          <span>
            <i className="dot-other" aria-hidden="true" />
            其他已披露持仓 <b>{ingredients.otherReportedWeight.toFixed(2)}%</b>
          </span>
        </div>
      </div>

      {coveredHoldings.length > 0 && (
        <div className="holding-impacts-panel" aria-label="重仓股近期影响">
          <h3>重仓股近期受什么影响？（几个字看清驱动与风险）</h3>
          <div className="holding-impacts-grid">
            {coveredHoldings.map((item) => (
              <div className="holding-impact-item" key={item.symbol}>
                <div className="holding-impact-top">
                  <Link to={companyHref(item.symbol)}>
                    <strong>{item.symbol}</strong> · {item.name}
                  </Link>
                  <span className="holding-impact-weight">
                    占 {item.weight.toFixed(2)}%
                  </span>
                </div>
                <div className="impact-pill-row">
                  {item.roleTag && (
                    <span className="role-badge">{item.roleTag}</span>
                  )}
                  {item.purityTag && (
                    <span className="purity-badge">{item.purityTag}</span>
                  )}
                  {item.impactTags?.slice(0, 3).map((tag) => (
                    <ImpactPill
                      key={tag.label}
                      label={tag.label}
                      tone={tag.tone}
                    />
                  ))}
                </div>
              </div>
            ))}
          </div>
        </div>
      )}

      <div className="structure-metrics">
        <div>
          <span>前十大合计</span>
          <strong>{topTenWeight(snapshot).toFixed(2)}%</strong>
          <small>按披露权重相加</small>
        </div>
        <div>
          <span>持仓日期</span>
          <strong>{snapshot.asOf}</strong>
          <small>
            {snapshot.coverage === 'full-equity'
              ? `${snapshot.holdings.length} 项权益持仓`
              : '前十大持仓节选'}
          </small>
        </div>
        <div>
          <span>覆盖范围</span>
          <strong className="structure-scope">{snapshot.scope}</strong>
          <small>资料核对 {aiChips.reviewedAt}</small>
        </div>
      </div>
      <div className="domain-top-holdings">
        {snapshot.holdings.slice(0, 10).map((holding, index) => {
          const covered = allCompanies().find(
            (company) => company.symbol === holding.symbol,
          );
          return (
            <div className="domain-holding-row" key={holding.symbol}>
              <span className="domain-rank">{index + 1}</span>
              <div>
                {covered ? (
                  <Link to={companyHref(holding.symbol)}>
                    {holding.name}{' '}
                    <span className="domain-ticker">{holding.symbol} ↗</span>
                  </Link>
                ) : (
                  <span>
                    {holding.name}{' '}
                    <span className="domain-ticker">{holding.symbol}</span>
                  </span>
                )}
                <div className="domain-weight-track" aria-hidden="true">
                  <span style={{ width: holding.weight + '%' }} />
                </div>
              </div>
              <strong>{holding.weight.toFixed(2)}%</strong>
            </div>
          );
        })}
      </div>
      <details className="domain-details">
        <summary>查看 {snapshot.holdings.length} 项已核对持仓及来源</summary>
        <p>
          {snapshot.coverage === 'full-equity'
            ? '已核对发行人 CSV 中的全部权益行；未纳入现金、货币基金、抵押品与期货。'
            : '仅收录发行人页面列出的前十大；未出现的公司不能视为未持有。'}
        </p>
        <div className="domain-table-scroll">
          <table>
            <caption>
              {code} · {snapshot.asOf} ·{' '}
              {snapshot.coverage === 'full-equity' ? '权益持仓' : '前十大节选'}
            </caption>
            <thead>
              <tr>
                <th scope="col">公司 / 代码</th>
                <th scope="col">占基金权重</th>
              </tr>
            </thead>
            <tbody>
              {snapshot.holdings.map((holding) => (
                <tr key={holding.symbol}>
                  <th scope="row">
                    {holding.name} · {holding.symbol}
                  </th>
                  <td>{holding.weight.toFixed(2)}%</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </details>
      <p className="domain-footnote">
        权重为对应披露日的数据，前十大合计不等于单一主题收入占比。资料经人工核对，未自动更新，也不用于持仓估值。
      </p>
      <DomainSource id={snapshot.sourceId} />
    </section>
  );
}
