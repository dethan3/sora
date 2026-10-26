import { Link, useParams, useSearchParams } from 'react-router';
import type { DemoData } from '../../data/demo';
import { EmptyState } from '../../components/ui';
import { FundTable } from '../../components/FundTable';
import { listSource } from '../../navigation/flow';
import { directionKind, FollowDirection } from './DirectionCard';
import './discover.css';
export function DirectionDetailPage({ data }: { data: DemoData }) {
  const { id } = useParams();
  const [params] = useSearchParams();
  const direction = data.modules.find((item) => item.id === id);
  const from = listSource(params.get('from'));
  const source =
    from?.startsWith('/discover?') || from === '/discover' ? from : '/discover';
  if (!direction)
    return (
      <EmptyState title="没有找到这个观察方向">
        <Link to={source}>返回发现市场</Link>
      </EmptyState>
    );
  const funds = data.funds.filter((fund) =>
    direction.fundCodes.includes(fund.code),
  );
  return (
    <div className="direction-detail">
      <Link className="back-link" to={source} state={{ restoreList: true }}>
        ← 返回发现市场
      </Link>
      <header className="direction-detail-heading">
        <div>
          <p className="direction-kind">{directionKind(direction.kind)}</p>
          <h1>{direction.title}</h1>
          <p>{direction.description}</p>
        </div>
        <FollowDirection direction={direction} />
      </header>
      <div className="direction-detail-grid">
        <section
          className="direction-benchmark"
          aria-labelledby="benchmark-title"
        >
          <p className="eyebrow">REFERENCE INDICATOR</p>
          <h2 id="benchmark-title">{direction.benchmark ?? direction.title}</h2>
          <div className="direction-quote-placeholder">
            <strong>—</strong>
            <span>暂无数据</span>
          </div>
          <p>{direction.benchmarkNote ?? '参考指标与关联基金分别展示。'}</p>
          <div className="direction-chart-empty">
            <span aria-hidden="true">∿</span>
            <p>暂无走势数据</p>
            <small>有可用的历史记录后，将在这里展示。</small>
          </div>
          <p className="direction-indicator-note">
            指标数据尚未接入，不以关联基金涨跌替代。
          </p>
        </section>
        <aside className="direction-scope">
          <h2>这个方向包含什么</h2>
          <ul>
            {direction.scope.map((item) => (
              <li key={item}>{item}</li>
            ))}
          </ul>
          <h3>资料与来源</h3>
          {direction.sourceUrl ? (
            <a
              className="landing-inline-link"
              href={direction.sourceUrl}
              target="_blank"
              rel="noreferrer"
            >
              {direction.sourceName} <span aria-hidden="true">↗</span>
            </a>
          ) : (
            <p>暂无来源资料</p>
          )}
          <p className="direction-source-date">
            资料核对日期 {data.asOf}
            <br />
            此日期不代表行情更新时间。
          </p>
        </aside>
      </div>
      <section
        className="direction-related"
        aria-labelledby="direction-funds-title"
      >
        <div className="discover-section-heading">
          <div>
            <h2 id="direction-funds-title">相关基金与产品</h2>
            <p>了解关联依据，再进入产品详情。</p>
          </div>
          <Link
            className="text-link"
            to={`/funds?${new URLSearchParams({ module: direction.id, from: '/discover/' + direction.id + '?' + params.toString() })}`}
          >
            在基金目录查看 →
          </Link>
        </div>
        {funds.length ? (
          <>
            <div className="direction-reasons">
              {funds.map((fund) => (
                <div key={fund.code}>
                  <strong>{fund.code}</strong>
                  <p>{direction.fundReasons[fund.code] ?? fund.note}</p>
                </div>
              ))}
            </div>
            <FundTable funds={funds} />
          </>
        ) : (
          <EmptyState title="暂无关联基金">
            可以先关注这个方向，稍后回来查看。
          </EmptyState>
        )}
      </section>
      <div className="direction-detail-footer">
        <p>关注方向不会自动关注其中的基金，也不会创建持仓。</p>
        <Link
          className="landing-inline-link"
          to={source}
          state={{ restoreList: true }}
        >
          继续探索其他方向 →
        </Link>
      </div>
    </div>
  );
}
