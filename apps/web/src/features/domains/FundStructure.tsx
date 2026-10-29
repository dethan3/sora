import { Link } from 'react-router';
import { aiChips, companyHref, fundSnapshot, topTenWeight } from './model';
import './domains.css';

export function DomainSource({ id }: { id: string }) {
  const source = aiChips.sources.find((item) => item.id === id)!;
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

export function FundStructure({ code }: { code: string }) {
  const snapshot = fundSnapshot(code);
  if (!snapshot) return null;
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
        <Link className="text-link" to="/discover">
          AI 芯片领域 →
        </Link>
      </div>
      <p className="domain-lead">{snapshot.description}</p>
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
        {snapshot.holdings.slice(0, 10).map((holding, index) => (
          <div className="domain-holding-row" key={holding.symbol}>
            <span className="domain-rank">{index + 1}</span>
            <div>
              {aiChips.companies.some(
                (company) => company.symbol === holding.symbol,
              ) ? (
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
        ))}
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
        权重为对应披露日的数据，前十大合计不等于 AI
        芯片占比。资料经人工核对，未自动更新，也不用于持仓估值。
      </p>
      <DomainSource id={snapshot.sourceId} />
    </section>
  );
}
