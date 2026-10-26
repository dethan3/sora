import { useSearchParams } from 'react-router';
import { useEffect, useRef, useState } from 'react';
import type { DemoData } from '../../data/demo';
import { DirectionCard } from './DirectionCard';
import { useDirections } from './DirectionProvider';
import { fundLinkId } from '../../navigation/flow';
import './discover.css';
import { MarketNotice } from '../market/MarketProvider';

export function DiscoverPage({ data }: { data: DemoData }) {
  const [params, setParams] = useSearchParams();
  const store = useDirections();
  const previousIds = useRef(store.ids);
  useEffect(() => {
    const previous = previousIds.current;
    previousIds.current = store.ids;
    if (
      document.activeElement !== document.body ||
      !/^已取消关注|^已恢复关注/.test(store.notice)
    )
      return;
    const removed = previous.find((id) => !store.ids.includes(id));
    const added = store.ids.find((id) => !previous.includes(id));
    if (removed || added)
      document
        .getElementById(
          fundLinkId('direction:' + (added ? 'saved:' + added : removed)),
        )
        ?.focus({ preventScroll: true });
  }, [store.ids, store.notice]);
  const [desktop, setDesktop] = useState(false);
  useEffect(() => {
    const media = window.matchMedia?.('(min-width: 768px)');
    const sync = () => setDesktop(media?.matches ?? false);
    sync();
    media?.addEventListener('change', sync);
    return () => media?.removeEventListener('change', sync);
  }, []);
  const query = params.get('q') ?? '';
  const market = params.get('market') ?? '';
  const kind = params.get('kind') ?? '';
  const markets = [...new Set(data.funds.map((fund) => fund.market))];
  const filtered = data.modules.filter(
    (item) =>
      (!kind || item.kind === kind) &&
      (!market ||
        data.funds.some(
          (fund) =>
            item.fundCodes.includes(fund.code) && fund.market === market,
        )) &&
      [
        item.id,
        item.title,
        item.description,
        item.symbol,
        item.benchmark,
        ...item.scope,
        ...item.keywords,
        ...item.fundCodes,
      ]
        .join(' ')
        .toLowerCase()
        .includes(query.trim().toLowerCase()),
  );
  function update(key: string, value: string) {
    const next = new URLSearchParams(params);
    if (value) next.set(key, value);
    else next.delete(key);
    setParams(next, { replace: true });
  }
  function clear() {
    setParams({}, { replace: true });
    document.getElementById('direction-search')?.focus();
  }
  const followed = store.ids
    .map((id) => data.modules.find((item) => item.id === id))
    .filter((item) => !!item);
  const missing = store.ids.filter(
    (id) => !data.modules.some((item) => item.id === id),
  );
  const groups = [
    {
      title: '主题与资产',
      description: '沿着兴趣，认识产业与资产。',
      items: filtered.filter((item) => item.kind !== 'index'),
    },
    {
      title: '宽基与市场',
      description: '通过指数，建立整体市场视角。',
      items: filtered.filter((item) => item.kind === 'index'),
    },
  ];
  return (
    <div className="discover-page">
      <header className="discover-heading">
        <div>
          <p className="eyebrow">YOUR NEXT PERSPECTIVE</p>
          <h1>发现市场</h1>
          <p>从你关心的方向出发，连接市场、主题与基金。</p>
        </div>
        <span className="discover-caption">保持好奇，逐步了解。</span>
      </header>
      <MarketNotice />
      <section
        className={
          'my-directions' + (store.ids.length ? '' : ' my-directions-empty')
        }
        aria-labelledby="my-directions-title"
      >
        <div className="discover-section-heading">
          <div>
            <h2 id="my-directions-title">
              我的观察
              <span>{store.ids.length.toString().padStart(2, '0')}</span>
            </h2>
            <p>你关心的方向，每次回来接着看。</p>
          </div>
          <a className="text-link" href="#explore-directions">
            选择方向 <span aria-hidden="true">↓</span>
          </a>
        </div>
        {followed.length ? (
          <div className="direction-grid direction-following-grid">
            {followed.map((direction) => (
              <DirectionCard direction={direction} key={direction.id} compact />
            ))}
          </div>
        ) : (
          <div className="direction-first-use">
            <span aria-hidden="true">✧</span>
            <div>
              <p>从下面选择 AI、半导体、黄金或宽基指数，点击“关注”即可加入。</p>
            </div>
          </div>
        )}
        {missing.map((id) => (
          <div className="unavailable-row" key={id}>
            <span>{id} · 方向暂不可用，关注记录仍保留</span>
            <button
              className="button-quiet"
              onClick={() => store.toggle(id, id)}
            >
              移除方向 {id}
            </button>
          </div>
        ))}
      </section>
      <div id="explore-directions" className="discover-explore-heading">
        <div>
          <p className="eyebrow">EXPLORE YOUR INTERESTS</p>
          <h2>找到想持续了解的方向。</h2>
        </div>
        <p>先选主题，再按市场缩小范围。</p>
      </div>
      <form
        className="direction-filters"
        role="search"
        aria-label="查找观察方向"
        onSubmit={(event) => event.preventDefault()}
      >
        <label className="direction-search-label" htmlFor="direction-search">
          <span>搜索方向</span>
          <input
            id="direction-search"
            type="search"
            value={query}
            placeholder="AI、芯片、黄金、标普500…"
            onChange={(event) => update('q', event.target.value)}
          />
        </label>
        <details
          className="direction-filter-options"
          open={desktop || market || kind ? true : undefined}
        >
          <summary>筛选{market || kind ? ' · 已设置' : ''}</summary>
          <div className="direction-filter-fields">
            <label>
              市场
              <select
                value={market}
                onChange={(event) => update('market', event.target.value)}
              >
                <option value="">全部市场</option>
                {market && !markets.includes(market) && (
                  <option value={market}>市场已不存在</option>
                )}
                {markets.map((value) => (
                  <option key={value}>{value}</option>
                ))}
              </select>
            </label>
            <label>
              方向类型
              <select
                value={kind}
                onChange={(event) => update('kind', event.target.value)}
              >
                <option value="">全部类型</option>
                <option value="theme">投资主题</option>
                <option value="asset">资产与商品</option>
                <option value="index">宽基指数</option>
                {kind && !['theme', 'asset', 'index'].includes(kind) && (
                  <option value={kind}>类型已不存在</option>
                )}
              </select>
            </label>
          </div>
        </details>
        {(query || market || kind) && (
          <button className="button-quiet" type="button" onClick={clear}>
            清除筛选
          </button>
        )}
      </form>
      <p className="direction-result-count" role="status" aria-live="polite">
        {filtered.length} 个方向{market ? ' · ' + market : ''}
      </p>
      {filtered.length ? (
        groups.map((group) =>
          group.items.length ? (
            <section
              className="direction-category"
              key={group.title}
              aria-label={group.title}
            >
              <div className="discover-section-heading">
                <div>
                  <h2>{group.title}</h2>
                  <p>{group.description}</p>
                </div>
              </div>
              <div className="direction-grid">
                {group.items.map((direction) => (
                  <DirectionCard key={direction.id} direction={direction} />
                ))}
              </div>
            </section>
          ) : null,
        )
      ) : (
        <div className="direction-empty">
          <h2>没有找到这个方向</h2>
          <p>试试其他名称，或清除筛选后重新浏览。</p>
          <button className="button-secondary" onClick={clear}>
            重新浏览全部方向
          </button>
        </div>
      )}
      <p className="direction-local-note">
        观察方向保存在此浏览器，与基金关注分别管理。市场筛选按关联产品的上市市场归类。
      </p>
    </div>
  );
}
