import type { Fund } from '../../data/demo';
import { Change } from '../../components/ui';
import { useMarket } from './MarketProvider';
import { MarketChange } from './MarketQuote';
import {
  changePercent,
  fundPriceSeries,
  isStale,
  numberText,
  observationTime,
  unitText,
} from './model';

export function FundPrice({ fund }: { fund: Fund }) {
  const market = useMarket();
  const series = fundPriceSeries(market.data, fund.code, fund.currency);
  if (series?.quote)
    return (
      <div className="fund-market-price">
        <strong>
          {numberText(series.quote.value)} <span>{unitText(series.unit)}</span>
        </strong>
        <div>
          <MarketChange
            value={changePercent(
              series.quote.value,
              series.quote.previousClose,
            )}
          />{' '}
          <span>较前一交易日</span>
        </div>
        <small>
          <time dateTime={series.quote.asOf}>{observationTime(series)}</time>
        </small>
        {market.data?.source === 'mock' && <small>演示行情</small>}
        {(isStale(series) || market.status === 'error') && (
          <small className="market-stale">
            {series.provenance?.updateStatus === 'failed'
              ? '采集失败 · 保留上次记录'
              : market.status === 'error'
                ? '读取失败 · 保留上次记录'
                : '更新滞后'}
          </small>
        )}
      </div>
    );
  // Historical demo fixtures retain their independent NAV view.
  if (fund.nav !== null) return <Change value={fund.changePct} />;
  return (
    <span className="muted">
      {market.status === 'loading'
        ? '正在载入价格…'
        : market.status === 'error'
          ? '价格暂不可用'
          : '暂无交易价格'}
    </span>
  );
}
