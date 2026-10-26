import { useMarket } from './MarketProvider';
import {
  changePercent,
  numberText,
  observationTime,
  unitText,
  type MarketSeries,
} from './model';
export function MarketChange({
  value,
  suffix = '%',
}: {
  value: number | null;
  suffix?: string;
}) {
  const flat = value !== null && Math.abs(value) < 0.005;
  return (
    <span
      className={
        'market-change ' +
        (value === null || flat
          ? 'market-flat'
          : value > 0
            ? 'market-up'
            : 'market-down')
      }
    >
      {value === null
        ? '—'
        : (value > 0 && !flat ? '+' : '') +
          (flat ? '0.00' : value.toFixed(2)) +
          suffix}
    </span>
  );
}
export function MarketStatus({ series }: { series: MarketSeries }) {
  return (
    <span className="market-status">
      <span>
        {
          {
            open: '交易中',
            closed: '已收盘',
            holiday: '休市',
            unknown: '交易状态未知',
          }[series.session]
        }
      </span>
      {series.quote?.delayMinutes ? (
        <span>延迟 {series.quote.delayMinutes} 分钟</span>
      ) : null}
      {series.freshness === 'stale' && (
        <span className="market-stale">更新滞后</span>
      )}
    </span>
  );
}
export function MarketQuote({
  id,
  compact = false,
}: {
  id: string;
  compact?: boolean;
}) {
  const market = useMarket();
  const series = market.data?.series.find((item) => item.id === id);
  if (market.status === 'loading' && !series)
    return (
      <div
        className="market-quote-loading"
        role="status"
        aria-label="正在载入行情"
      >
        <span />
        <span />
      </div>
    );
  if (!series?.quote)
    return (
      <div className="market-quote-empty">
        <strong>—</strong>
        <span>{market.status === 'error' ? '行情暂不可用' : '暂无数据'}</span>
      </div>
    );
  const quote = series.quote;
  return (
    <div className={'market-quote' + (compact ? ' market-quote-compact' : '')}>
      <div className="market-quote-value">
        <strong>{numberText(quote.value)}</strong>
        <span>{unitText(series.unit)}</span>
      </div>
      <div className="market-day-change">
        <MarketChange value={changePercent(quote.value, quote.previousClose)} />
        <span>较前一交易日</span>
      </div>
      <MarketStatus series={series} />
      <time dateTime={quote.asOf} className="market-asof">
        {observationTime(series)}
      </time>
      {series.freshness === 'stale' && !compact && (
        <p className="market-stale-note">
          保留最近有效记录，最新行情尚未更新。
        </p>
      )}
      {market.status === 'error' && (
        <p className="market-stale-note">本次更新失败，正在显示上次记录。</p>
      )}
    </div>
  );
}
