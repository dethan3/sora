import { Link } from 'react-router';
import type { DemoData } from '../../data/demo';
import { EmptyState, PageHeader, Panel } from '../../components/ui';
import { FundTable } from '../../components/FundTable';
import { usePersonal } from '../local/PersonalProvider';
import { BackupTools } from '../local/BackupTools';

export function WatchlistPage({ data }: { data: DemoData }) {
  const local = usePersonal();
  const missing = local.state.watchlist.filter(
    (code) => !data.funds.some((fund) => fund.code === code),
  );
  return (
    <>
      <PageHeader
        eyebrow="WATCHLIST"
        title="我的关注"
        description="集中回看关心的基金，关注列表自动保存在此浏览器。"
      />
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
