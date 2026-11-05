import { Link } from 'react-router';
import type { DemoData } from '../../data/demo';
import { EmptyState, PageHeader, Panel } from '../../components/ui';
import { FundTable } from '../../components/FundTable';
import { usePersonal } from '../local/PersonalProvider';
import { BackupTools } from '../local/BackupTools';
import { MarketNotice } from '../market/MarketProvider';
import { SavedResearch } from '../research/Research';
import { allCompanies, companyHref, fundSnapshot } from '../domains/model';
import {
  ImpactPill,
  PortfolioImpactHealthCard,
} from '../domains/FundStructure';

export function WatchlistPage({ data }: { data: DemoData }) {
  const local = usePersonal();
  const missing = local.state.watchlist.filter(
    (code) => !data.funds.some((fund) => fund.code === code),
  );
  const watchedHoldingsImpacts = local.state.watchlist.flatMap((code) => {
    const snapshot = fundSnapshot(code);
    if (!snapshot) return [];
    const matched = allCompanies().flatMap((company) => {
      const h = snapshot.holdings.find(
        (item) => item.symbol === company.symbol,
      );
      return h ? [{ ...company, weight: h.weight }] : [];
    });
    return matched.length ? [{ code, matched }] : [];
  });
  return (
    <>
      <PageHeader
        eyebrow="WATCHLIST"
        title="我的关注"
        description="用简单标签盯住关注线索的下次验证大考，以及已关注基金重仓股的近期利弊。"
      />
      <SavedResearch />
      {local.state.watchlist.length > 0 && <MarketNotice />}
      {local.state.watchlist.length > 0 && (
        <PortfolioImpactHealthCard
          codes={local.state.watchlist}
          title="关注组合 · 近期核心影响体检"
        />
      )}
      {watchedHoldingsImpacts.length > 0 && (
        <Panel title="已关注基金 · 重仓股近期影响速览">
          <div className="watchlist-impact-list">
            {watchedHoldingsImpacts.map((group) => (
              <div key={group.code} className="watchlist-impact-group">
                <div className="watchlist-impact-head">
                  <strong>{group.code} 核心重仓股近期风向</strong>
                  <Link className="text-link" to={`/funds/${group.code}`}>
                    查看完整配料表 →
                  </Link>
                </div>
                <div className="holding-impacts-grid">
                  {group.matched.map((item) => (
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
            ))}
          </div>
        </Panel>
      )}
      <Panel title={`已关注 · ${local.state.watchlist.length}`}>
        {local.state.watchlist.length ? (
          <>
            <FundTable
              funds={data.funds.filter((fund) =>
                local.state.watchlist.includes(fund.code),
              )}
            />
            {missing.map((code) => (
              <div className="unavailable-row" key={code}>
                <span>{code} · 目录中已无此基金</span>
                <button
                  className="button-quiet"
                  disabled={!local.canWrite}
                  onClick={() =>
                    local.commit({
                      ...local.state,
                      watchlist: local.state.watchlist.filter(
                        (item) => item !== code,
                      ),
                    })
                  }
                >
                  移除 {code}
                </button>
              </div>
            ))}
          </>
        ) : (
          <EmptyState title="还没有关注的基金">
            从基金目录开始，点击“关注”即可加入列表。
            <br />
            <Link className="text-link" to="/funds">
              浏览基金 →
            </Link>
          </EmptyState>
        )}
      </Panel>
      <details
        className="data-management"
        open={local.issue ? true : undefined}
      >
        <summary>备份与数据管理</summary>
        <BackupTools />
      </details>
    </>
  );
}
