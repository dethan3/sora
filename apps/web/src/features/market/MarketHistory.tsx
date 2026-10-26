import { useId, useState } from 'react';
import {
  changePercent,
  numberText,
  periodHistory,
  periodLabels,
  periods,
  unitText,
  type MarketSeries,
  type Period,
} from './model';
import { MarketChange } from './MarketQuote';

export function MarketHistory({
  series,
  period,
  onPeriodChange,
}: {
  series: MarketSeries;
  period: Period;
  onPeriodChange: (period: Period) => void;
}) {
  const id = useId();
  const [selected, setSelected] = useState<number | null>(null);
  const history = periodHistory(series.history, period);
  const first = history[0];
  const last = history.at(-1);
  const values = history.map((point) => point.value);
  const min = Math.min(...values);
  const max = Math.max(...values);
  const padding = Math.max((max - min) * 0.15, max * 0.002);
  const lower = min - padding;
  const upper = max + padding;
  const x = (index: number) =>
    12 + (index / Math.max(1, history.length - 1)) * 736;
  const y = (value: number) => 214 - ((value - lower) / (upper - lower)) * 192;
  const activeIndex = Math.min(
    selected ?? history.length - 1,
    history.length - 1,
  );
  const active = history[activeIndex];
  function selectPeriod(next: Period) {
    setSelected(null);
    onPeriodChange(next);
  }
  return (
    <div className="market-history">
      <div className="market-chart-toolbar">
        <h3 id={id + '-title'}>指标走势</h3>
        <div className="market-periods" role="group" aria-label="走势区间">
          {periods.map((item) => (
            <button
              type="button"
              key={item}
              aria-pressed={period === item}
              onClick={() => selectPeriod(item)}
            >
              {periodLabels[item]}
            </button>
          ))}
        </div>
      </div>
      {history.length < 2 ? (
        <div className="market-chart-empty">
          <strong>暂无走势数据</strong>
          <p>所选区间的历史记录不足，请尝试其他区间。</p>
        </div>
      ) : (
        <>
          <div className="market-period-summary">
            <span>{periodLabels[period]}区间表现</span>
            <MarketChange value={changePercent(last!.value, first.value)} />
          </div>
          <p
            className="market-chart-observation"
            aria-live="polite"
            aria-atomic="true"
          >
            <time dateTime={active.date}>{active.date}</time>
            <strong>{numberText(active.value)}</strong>
            <span>{unitText(series.unit)}</span>
          </p>
          <figure className="market-figure" aria-labelledby={id + '-title'}>
            <div className="market-axis">
              <span>{numberText(upper)}</span>
              <span>{unitText(series.unit)}</span>
            </div>
            <svg
              viewBox="0 0 760 240"
              role="img"
              aria-label={`${series.name}演示走势，${first.date}至${last!.date}`}
              onPointerMove={(event) => {
                if (event.pointerType === 'touch') return;
                const bounds = event.currentTarget.getBoundingClientRect();
                const position =
                  ((event.clientX - bounds.left) / bounds.width) * 760;
                setSelected(
                  Math.max(
                    0,
                    Math.min(
                      history.length - 1,
                      Math.round(
                        ((position - 12) / 736) * (history.length - 1),
                      ),
                    ),
                  ),
                );
              }}
              onPointerLeave={() => setSelected(null)}
            >
              <defs>
                <linearGradient id={id + '-fill'} x1="0" y1="0" x2="0" y2="1">
                  <stop offset="0%" stopColor="#635bff" stopOpacity="0.13" />
                  <stop offset="100%" stopColor="#635bff" stopOpacity="0" />
                </linearGradient>
              </defs>
              {[30, 122, 214].map((line) => (
                <line
                  key={line}
                  x1="12"
                  x2="748"
                  y1={line}
                  y2={line}
                  className="market-gridline"
                />
              ))}
              <path
                d={`M${x(0)},224 L${history.map((point, index) => `${x(index)},${y(point.value)}`).join(' L')} L${x(history.length - 1)},224 Z`}
                fill={`url(#${id}-fill)`}
              />
              <polyline
                points={history
                  .map((point, index) => `${x(index)},${y(point.value)}`)
                  .join(' ')}
                fill="none"
                stroke="#635bff"
                strokeWidth="2.5"
                strokeLinejoin="round"
                vectorEffect="non-scaling-stroke"
              />
              <line
                x1={x(activeIndex)}
                x2={x(activeIndex)}
                y1="12"
                y2="224"
                className="market-crosshair"
              />
              <circle
                cx={x(activeIndex)}
                cy={y(active.value)}
                r="4.5"
                fill="#635bff"
                stroke="white"
                strokeWidth="2"
              />
            </svg>
            <div className="market-axis">
              <span>{numberText(lower)}</span>
              <span>纵轴按区间缩放</span>
            </div>
            <figcaption>
              <time dateTime={first.date}>{first.date}</time>
              <time dateTime={last!.date}>{last!.date}</time>
            </figcaption>
          </figure>
          <label className="market-point-control" htmlFor={id + '-point'}>
            滑动查看日期与数值
            <input
              id={id + '-point'}
              type="range"
              min="0"
              max={history.length - 1}
              step="1"
              value={activeIndex}
              onChange={(event) => setSelected(Number(event.target.value))}
              aria-valuetext={`${active.date}，${numberText(active.value)} ${unitText(series.unit)}`}
            />
          </label>
          <p className="market-chart-note">
            按所选区间首末记录计算；日线仅连接已有记录，非交易日不补点。指标变化不代表基金收益。
          </p>
          <details className="market-history-records">
            <summary>查看 {history.length} 条历史记录</summary>
            <div className="market-history-scroll">
              <table>
                <caption className="sr-only">{series.name}演示历史记录</caption>
                <thead>
                  <tr>
                    <th scope="col">日期</th>
                    <th scope="col">数值（{unitText(series.unit)}）</th>
                  </tr>
                </thead>
                <tbody>
                  {[...history].reverse().map((point) => (
                    <tr key={point.date}>
                      <th scope="row">{point.date}</th>
                      <td>{numberText(point.value)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </details>
        </>
      )}
    </div>
  );
}
