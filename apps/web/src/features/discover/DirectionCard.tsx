import { Link, useLocation } from 'react-router';
import type { DemoData } from '../../data/demo';
import { fundLinkId, rememberList } from '../../navigation/flow';
import { useDirections } from './DirectionProvider';
import { MarketQuote } from '../market/MarketQuote';
export type Direction = DemoData['modules'][number];
export const directionKind = (kind: Direction['kind']) =>
  kind === 'index' ? '宽基指数' : kind === 'asset' ? '资产与商品' : '投资主题';
export function FollowDirection({ direction }: { direction: Direction }) {
  const store = useDirections();
  const followed = store.ids.includes(direction.id);
  return (
    <button
      type="button"
      className="direction-follow"
      aria-pressed={followed}
      aria-label={`${followed ? '取消关注' : '关注'}方向：${direction.title}`}
      onClick={() => store.toggle(direction.id, direction.title)}
    >
      <span aria-hidden="true">{followed ? '★' : '☆'}</span>
      <span>{followed ? '已关注' : '关注'}</span>
    </button>
  );
}
export function DirectionCard({
  direction,
  compact = false,
}: {
  direction: Direction;
  compact?: boolean;
}) {
  const location = useLocation();
  const source = location.pathname + location.search;
  return (
    <article className={`direction-card ${compact ? 'direction-compact' : ''}`}>
      <div className="direction-card-top">
        <span
          className={`direction-symbol direction-${direction.kind}`}
          aria-hidden="true"
        >
          {direction.symbol}
        </span>
        <FollowDirection direction={direction} />
      </div>
      <p className="direction-kind">{directionKind(direction.kind)}</p>
      <Link
        className="direction-title"
        id={fundLinkId(`direction:${compact ? 'saved:' : ''}${direction.id}`)}
        to={`/discover/${direction.id}?${new URLSearchParams({ from: source })}`}
        onClick={() =>
          rememberList(
            source,
            `direction:${compact ? 'saved:' : ''}${direction.id}`,
          )
        }
      >
        {direction.title}
        <span aria-hidden="true">↗</span>
      </Link>
      {!compact && (
        <>
          <p className="direction-description">{direction.description}</p>
          <div className="direction-tags">
            {direction.scope.slice(0, 3).map((tag) => (
              <span key={tag}>{tag}</span>
            ))}
          </div>
        </>
      )}
      <div className="direction-card-quote market-card-quote">
        <div>
          <span>参考指标</span>
          <p>{direction.benchmark ?? direction.title}</p>
        </div>
        <MarketQuote id={direction.id} compact />
      </div>
      <p className="direction-fund-count">
        {direction.fundCodes.length} 项关联产品
        <span>{compact ? '继续观察 →' : '查看方向与关联基金 →'}</span>
      </p>
    </article>
  );
}
