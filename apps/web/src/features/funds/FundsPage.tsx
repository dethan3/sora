import { useState } from 'react';
import { useSearchParams, Link, useLocation } from 'react-router';
import type { DemoData } from '../../data/demo';
import { FundTable } from '../../components/FundTable';
import { PageHeader, Panel } from '../../components/ui';
import { detailUrl, listSource, rememberList } from '../../navigation/flow';
import { MarketNotice } from '../market/MarketProvider';
import {
  aiStorage,
  equityOverlap,
  fundAccessProfile,
  fundIngredients,
  fundSnapshot,
  topTenWeight,
} from '../domains/model';
import { ImpactPill } from '../domains/FundStructure';

export function FundsPage({ data }: { data: DemoData }) {
  const [params, setParams] = useSearchParams();
  const [pkCodes, setPkCodes] = useState<string[]>(['SOXX', 'AIQ']);
  const location = useLocation();
  const source = location.pathname + location.search;
  const query = params.get('q') ?? '';
  const moduleId = params.get('module') ?? '';
  const module = data.modules.find((item) => item.id === moduleId);
  const from = listSource(params.get('from'));
  const funds = data.funds.filter(
    (fund) =>
      (!moduleId || !!module?.fundCodes.includes(fund.code)) &&
      `${fund.name} ${fund.code} ${fund.market}`
        .toLowerCase()
        .includes(query.trim().toLowerCase()),
  );
  function togglePk(code: string) {
    setPkCodes((current) => {
      if (current.includes(code)) {
        return current.length > 1
          ? current.filter((item) => item !== code)
          : current;
      }
      if (current.length >= 3) {
        return [...current.slice(1), code];
      }
      return [...current, code];
    });
  }
  function update(key: string, value: string) {
    const next = new URLSearchParams(params);
    if (value) next.set(key, value);
    else next.delete(key);
    setParams(next, { replace: true });
  }
  const pkSnapshots = pkCodes
    .map((code) => fundSnapshot(code, aiStorage))
    .filter((item): item is NonNullable<typeof item> => Boolean(item));
  const overlapInfo =
    pkSnapshots.length === 2
      ? equityOverlap(pkSnapshots[0], pkSnapshots[1])
      : undefined;
  return (
    <>
      {from?.startsWith('/discover/') && (
        <Link className="back-link" to={from} state={{ restoreList: true }}>
          ← 返回观察方向
        </Link>
      )}
      <PageHeader
        eyebrow="FUND LIBRARY"
        title="基金"
        description="先看基金配料表（含多少 AI 存储与算力芯片），再看产品价格与跟踪对象。"
      />
      <MarketNotice />
      {data.source === 'reference-catalog' && (
        <Panel title="热门科技基金配料一眼对比">
          <div
            className="fund-pk-selector"
            role="group"
            aria-label="选择并排对比的基金（最多3只）"
          >
            <span className="pk-selector-label">点选 2~3 只并排 PK：</span>
            {data.funds.map((fund) => {
              const active = pkCodes.includes(fund.code);
              return (
                <button
                  key={fund.code}
                  type="button"
                  className={`pk-select-pill${active ? ' is-active' : ''}`}
                  aria-pressed={active}
                  onClick={() => togglePk(fund.code)}
                >
                  {fund.code} · {fund.name}
                </button>
              );
            })}
          </div>
          <div className="fund-pk-grid">
            {pkCodes.map((code) => {
              const snapshot = fundSnapshot(code, aiStorage);
              const ingredients = snapshot
                ? fundIngredients(snapshot)
                : undefined;
              const profile = fundAccessProfile(code);
              const fund = data.funds.find((item) => item.code === code);
              return (
                <div className="fund-pk-card" key={code}>
                  <div className="fund-pk-top">
                    <div>
                      <strong>{code}</strong>
                      <span>{fund?.name ?? code}</span>
                    </div>
                    <small>
                      {snapshot
                        ? `前十集中度 ${topTenWeight(snapshot).toFixed(1)}%`
                        : (profile?.accessTag ?? fund?.market)}
                    </small>
                  </div>
                  {profile && (
                    <div className="fund-access-badges">
                      <span className="role-badge">{profile.accessTag}</span>
                      <span className="purity-badge">
                        {profile.ingredientTag}
                      </span>
                    </div>
                  )}
                  {snapshot && ingredients ? (
                    <>
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
                            width: `${Math.max(10, 100 - ingredients.storageWeight - ingredients.chipsWeight)}%`,
                          }}
                        />
                      </div>
                      <div className="fund-pk-tags">
                        <span className="mini-tag is-storage">
                          🟩 AI 存储链：
                          {ingredients.storageWeight > 0
                            ? `${ingredients.storageWeight.toFixed(2)}% (MU/LRCX/SKHY)`
                            : '前十未见存储原厂'}
                        </span>
                        <span className="mini-tag is-chips">
                          🟦 AI 算力与设备：{ingredients.chipsWeight.toFixed(2)}
                          %
                        </span>
                      </div>
                      <p className="fund-pk-note">{snapshot.description}</p>
                    </>
                  ) : (
                    <p className="fund-pk-note">
                      {fund?.note ?? '跟踪宽基或实物资产基准。'}
                      （未收录单股持仓明细，不估算单一科技主题权重）
                    </p>
                  )}
                  {profile && (
                    <div
                      className="impact-pill-row"
                      aria-label={`${code} 近期影响标签`}
                    >
                      {profile.recentDrivers.map((tag) => (
                        <ImpactPill
                          key={tag.label}
                          label={tag.label}
                          tone={tag.tone}
                        />
                      ))}
                    </div>
                  )}
                  <Link
                    className="text-link"
                    to={detailUrl(code, source)}
                    onClick={() => rememberList(source, code)}
                  >
                    查看 {code} 完整配料与重仓影响 →
                  </Link>
                </div>
              );
            })}
          </div>
          {overlapInfo && (
            <p className="fund-pk-overlap">
              <strong>
                {pkSnapshots[0].code} vs {pkSnapshots[1].code} 持仓重叠说明：
              </strong>
              {overlapInfo.weight !== null
                ? `同日权益重叠率 ${overlapInfo.weight.toFixed(2)}%（${overlapInfo.reason}）`
                : overlapInfo.reason}
            </p>
          )}
        </Panel>
      )}
      <Panel title="基金目录">
        <div className="filter-row">
          <div>
            <label className="search-label" htmlFor="fund-search">
              搜索基金
            </label>
            <input
              id="fund-search"
              type="search"
              value={query}
              onChange={(event) => update('q', event.target.value)}
              placeholder="名称、代码或市场"
            />
          </div>
          <div>
            <label className="search-label" htmlFor="module-filter">
              地区与主题
            </label>
            <select
              id="module-filter"
              value={moduleId}
              onChange={(event) => update('module', event.target.value)}
            >
              <option value="">全部地区与主题</option>
              {moduleId && !module && (
                <option value={moduleId}>模块已不存在</option>
              )}
              {data.modules.map((item) => (
                <option key={item.id} value={item.id}>
                  {item.title}
                </option>
              ))}
            </select>
          </div>
          <span
            className="result-count"
            role="status"
            aria-live="polite"
            aria-atomic="true"
          >
            {funds.length} 只基金
          </span>
        </div>
        {funds.length ? (
          <FundTable funds={funds} />
        ) : (
          <div className="empty">
            <h2>暂无基金</h2>
            <p className="muted">
              没有符合当前条件的基金，试试清除搜索和筛选。
            </p>
            <button
              className="button-secondary"
              onClick={() => {
                setParams({}, { replace: true });
                document.getElementById('fund-search')?.focus();
              }}
            >
              清除筛选
            </button>
          </div>
        )}
      </Panel>
    </>
  );
}
