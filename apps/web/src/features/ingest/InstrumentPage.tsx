import { useEffect, useState, type ReactNode } from 'react';
import { Link, useParams } from 'react-router';
import type { PublicInstrument } from '@sora/market-data/ingest';
import { authRequest } from '../auth/api';
import { Panel } from '../../components/ui';
import { ImpactPill } from '../domains/FundStructure';
import { aiChips, aiStorage } from '../domains/model';
import {
  publicInstrumentSchema,
  stateLabel,
  errorText,
  kindLabel,
} from './api';
import './ingest.css';
function nodeLabel(value: string | null) {
  if (!value) return '待分类';
  const [domain, node] = value.split(':');
  const model =
    domain === 'ai-storage'
      ? aiStorage
      : domain === 'ai-chips'
        ? aiChips
        : undefined;
  return model?.nodes?.find((item) => item.id === node)?.title ?? value;
}
export function InstrumentData({
  id,
  heading = true,
  fallback,
}: {
  id: string;
  heading?: boolean;
  fallback?: ReactNode;
}) {
  const [data, setData] = useState<PublicInstrument>(),
    [error, setError] = useState(''),
    [reload, setReload] = useState(0);
  useEffect(() => {
    const controller = new AbortController();
    setData(undefined);
    setError('');
    let timer: ReturnType<typeof setTimeout>;
    async function load() {
      try {
        const value = await authRequest(
          `/api/instruments/${encodeURIComponent(id)}`,
          publicInstrumentSchema,
          undefined,
          controller.signal,
        );
        if (controller.signal.aborted) return;
        setData(value);
        if (value.status === 'ready')
          window.dispatchEvent(new Event('sora-catalog-changed'));
        if (value.status === 'pending')
          timer = setTimeout(() => void load(), 3000);
      } catch (error) {
        if (!controller.signal.aborted) setError(errorText(error));
      }
    }
    void load();
    return () => {
      controller.abort();
      clearTimeout(timer);
    };
  }, [id, reload]);
  if (error)
    return (
      <>
        <p role="alert">{error}</p>
        <button onClick={() => setReload((n) => n + 1)}>重新读取标的</button>
        {fallback}
      </>
    );
  if (!data) return <p role="status">正在读取标的资料…</p>;
  const snapshot = data.holdings,
    series = data.series,
    last = series?.history.at(-1);
  const covered =
    snapshot?.holdings.reduce((sum, row) => sum + row.weightBp, 0) ?? 0;
  const grouped = new Map<string, number>();
  for (const row of snapshot?.holdings ?? []) {
    const profile = data.companies.find((item) => item.id === row.id);
    const key = profile?.chainNode ?? '';
    grouped.set(key, (grouped.get(key) ?? 0) + row.weightBp);
  }
  return (
    <>
      {heading && (
        <header className="page-header">
          <p>
            {data.symbol} · {data.market} · {kindLabel(data.kind)}
          </p>
          <h1>{data.name}</h1>
        </header>
      )}
      <p role="status">
        {stateLabel(data.status)}
        {data.lastError ? ` · 最近采集失败：${data.lastError}` : ''}
      </p>
      <div className="impact-pill-row">
        {data.impactTags.map((tag) => (
          <ImpactPill key={tag.label} {...tag} />
        ))}
        {data.chainNode && <span>产业链：{nodeLabel(data.chainNode)}</span>}
      </div>
      {data.warnings.map((warning) => (
        <p className="muted" key={warning}>
          {warning}
        </p>
      ))}
      {series && last ? (
        <Panel
          title={
            series.type === 'nav'
              ? '单位净值走势'
              : series.type === 'index'
                ? '指数日线'
                : '交易价格日线'
          }
        >
          <p className="fund-quote">
            <strong>{last.close}</strong> {series.currency} /{' '}
            {series.type === 'index' ? '点' : '份'}
          </p>
          <p>
            {last.date} · {series.timeZone} ·{' '}
            {series.type === 'nav' ? '单位净值' : '未复权收盘'}
            {Date.now() - Date.parse(last.date) > 4 * 86400000
              ? ' · 更新滞后'
              : ''}
          </p>
          <SeriesChart series={series} />
          <p className="muted">
            <a href={series.source.url} target="_blank" rel="noreferrer">
              {series.source.name}
            </a>{' '}
            · 采集于 {new Date(series.source.fetchedAt).toLocaleString('zh-CN')}
          </p>
        </Panel>
      ) : (
        <p className="muted">暂无已采集的日线数据。</p>
      )}
      {snapshot ? (
        <Panel title="基金配料与重仓股">
          <p>
            报告期 {snapshot.asOf} ·{' '}
            {snapshot.coverage === 'full'
              ? '全部披露持仓'
              : snapshot.coverage === 'top10'
                ? '前十大重仓'
                : '部分披露持仓'}{' '}
            · 已展示 {snapshot.holdings.length} 只，合计{' '}
            {(covered / 100).toFixed(2)}%
          </p>
          <div className="ingredient-summary">
            {[...grouped].map(([node, bp]) => (
              <span key={node}>
                {nodeLabel(node || null)}：{(bp / 100).toFixed(2)}%
              </span>
            ))}
            <span>
              未展示部分：{(Math.max(0, 10000 - covered) / 100).toFixed(2)}%
            </span>
          </div>
          <p className="muted">
            产业链归属和影响标签由管理员维护；未分类持仓不计入具体主题。
          </p>
          <div className="table-scroll">
            <table>
              <thead>
                <tr>
                  <th>证券</th>
                  <th>权重</th>
                  <th>产业链环节</th>
                  <th>影响标签</th>
                </tr>
              </thead>
              <tbody>
                {snapshot.holdings.map((row) => {
                  const profile = data.companies.find(
                    (item) => item.id === row.id,
                  );
                  return (
                    <tr key={row.id}>
                      <td>
                        {row.name}
                        <small className="ingest-symbol">
                          {row.market} · {row.symbol}
                        </small>
                      </td>
                      <td>{(row.weightBp / 100).toFixed(2)}%</td>
                      <td>{nodeLabel(profile?.chainNode ?? null)}</td>
                      <td>
                        <div className="impact-pill-row">
                          {profile?.impactTags.length
                            ? profile.impactTags.map((tag) => (
                                <ImpactPill key={tag.label} {...tag} />
                              ))
                            : '待标注'}
                        </div>
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
          <p className="muted">
            <a href={snapshot.source.url} target="_blank" rel="noreferrer">
              {snapshot.source.name}
            </a>{' '}
            · 采集于{' '}
            {new Date(snapshot.source.fetchedAt).toLocaleString('zh-CN')}
          </p>
        </Panel>
      ) : (
        (fallback ??
        (['fund', 'etf'].includes(data.kind) && (
          <Panel title="基金配料">
            <p>暂无自动采集的持仓明细。</p>
          </Panel>
        )))
      )}
    </>
  );
}
function SeriesChart({
  series,
}: {
  series: NonNullable<PublicInstrument['series']>;
}) {
  const [window, setWindow] = useState('90');
  const end = Date.parse(series.history.at(-1)!.date);
  const rows = series.history.filter(
    (row) =>
      window === 'all' ||
      Date.parse(row.date) >= end - Number(window) * 86400000,
  );
  const values = rows.map((row) => Number(row.close)),
    min = Math.min(...values),
    max = Math.max(...values);
  const points = rows
    .map(
      (row, index) =>
        `${12 + (index / Math.max(1, rows.length - 1)) * 736},${200 - ((Number(row.close) - min) / (max - min || 1)) * 180}`,
    )
    .join(' ');
  return (
    <div>
      <div className="ingest-actions" role="group" aria-label="日线区间">
        {[
          ['30', '近一月'],
          ['90', '近三月'],
          ['all', '全部'],
        ].map(([value, label]) => (
          <button
            key={value}
            aria-pressed={window === value}
            onClick={() => setWindow(value)}
          >
            {label}
          </button>
        ))}
      </div>
      {rows.length > 1 ? (
        <>
          <svg
            className="ingest-chart"
            viewBox="0 0 760 220"
            role="img"
            aria-label={`${series.type === 'nav' ? '单位净值' : '收盘'}走势，${rows[0].date} 至 ${rows.at(-1)!.date}`}
          >
            <polyline
              fill="none"
              stroke="currentColor"
              strokeWidth="2.5"
              points={points}
            />
          </svg>
          <p className="muted">
            {rows[0].date} → {rows.at(-1)!.date} · {min.toFixed(4)}～
            {max.toFixed(4)}
          </p>
        </>
      ) : (
        <p>所选区间数据不足。</p>
      )}
      <details>
        <summary>查看日线记录</summary>
        <div className="ingest-history table-scroll">
          <table>
            <thead>
              <tr>
                <th>日期</th>
                <th>{series.type === 'nav' ? '单位净值' : '收盘值'}</th>
              </tr>
            </thead>
            <tbody>
              {[...rows].reverse().map((row) => (
                <tr key={row.date}>
                  <td>{row.date}</td>
                  <td>{row.close}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </details>
    </div>
  );
}
export function InstrumentPage() {
  const { id } = useParams();
  return (
    <>
      <Link className="back-link" to="/funds">
        ← 基金与证券目录
      </Link>
      <InstrumentData key={id} id={id ?? ''} />
    </>
  );
}
