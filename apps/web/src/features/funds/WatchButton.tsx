import type { Fund } from '../../data/demo';
import { usePersonal } from '../local/PersonalProvider';

export function WatchButton({ fund }: { fund: Fund }) {
  const local = usePersonal();
  const watched = local.state.watchlist.includes(fund.code);
  return (
    <button
      className={`watch-button ${watched ? 'is-watched' : ''}`}
      aria-label={`${watched ? '取消关注' : '关注'}${fund.name}`}
      aria-pressed={watched}
      disabled={!!local.issue}
      onClick={() =>
        local.commit(
          {
            ...local.state,
            watchlist: watched
              ? local.state.watchlist.filter((code) => code !== fund.code)
              : [...local.state.watchlist, fund.code],
          },
          watched ? '已取消关注。' : '已加入关注，保存在此浏览器。',
        )
      }
    >
      <span aria-hidden="true">{watched ? '✓' : '+'}</span>{' '}
      {watched ? '已关注' : '关注'}
    </button>
  );
}
