import { useSearchParams } from 'react-router';
import { useMarket } from './MarketProvider';
import { MarketQuote } from './MarketQuote';
import { MarketHistory } from './MarketHistory';
import { periods, type Period } from './model';
export function MarketPanel({
  id,
  benchmark,
  note,
}: {
  id: string;
  benchmark: string;
  note: string;
}) {
  const market = useMarket();
  const series = market.data?.series.find((item) => item.id === id);
  const [params, setParams] = useSearchParams();
  const rawPeriod = params.get('period');
  const period: Period = periods.includes(rawPeriod as Period)
    ? (rawPeriod as Period)
    : '1M';
  return (
    <section className="direction-benchmark" aria-labelledby="benchmark-title">
      <p className="eyebrow">REFERENCE INDICATOR</p>
      <h2 id="benchmark-title">{series?.name ?? benchmark}</h2>
      <MarketQuote id={id} />
      <p>{note}</p>
      {series ? (
        <MarketHistory
          key={id}
          series={series}
          period={period}
          onPeriodChange={(next) => {
            const query = new URLSearchParams(params);
            query.set('period', next);
            setParams(query, { replace: true, preventScrollReset: true });
          }}
        />
      ) : market.status === 'loading' ? (
        <div className="market-chart-skeleton" aria-hidden="true" />
      ) : (
        <div className="market-chart-empty">
          <strong>暂无走势数据</strong>
          <p>有可用的历史记录后，将在这里展示。</p>
        </div>
      )}
      <p className="direction-indicator-note">
        {market.enabled
          ? '行情来源：Sora Mock · 人工生成的固定演示记录，未连接任何行情供应商。'
          : '指标数据尚未接入，不以关联基金涨跌替代。'}
      </p>
    </section>
  );
}
