import { useEffect, useState } from 'react';
import { Link } from 'react-router';
import { useAuth } from '../auth/AuthProvider';
import { authRequest } from '../auth/api';
import {
  acceptedSchema,
  errorText,
  kindLabel,
  searchSchema,
  stateLabel,
  type SearchItem,
} from './api';
import './ingest.css';
export function DirectorySearch({ onQueued }: { onQueued?: () => void }) {
  const auth = useAuth();
  const [query, setQuery] = useState('');
  const [items, setItems] = useState<SearchItem[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');
  const [updated, setUpdated] = useState<string | null>();
  const [selected, setSelected] = useState<SearchItem>();
  const [reason, setReason] = useState('');
  const [busy, setBusy] = useState(false);
  const [notice, setNotice] = useState('');
  useEffect(() => {
    if (!auth.enabled || !query.trim()) {
      setItems([]);
      setLoading(false);
      return;
    }
    const controller = new AbortController();
    setLoading(true);
    setError('');
    setItems([]);
    const timer = setTimeout(() => {
      void authRequest(
        `/api/search?q=${encodeURIComponent(query.trim())}`,
        searchSchema,
        undefined,
        controller.signal,
      )
        .then((data) => {
          if (!controller.signal.aborted) {
            setItems(data.items);
            setUpdated(data.directoryUpdatedAt);
          }
        })
        .catch((error) => {
          if (!controller.signal.aborted) setError(errorText(error));
        })
        .finally(() => {
          if (!controller.signal.aborted) setLoading(false);
        });
    }, 300);
    return () => {
      clearTimeout(timer);
      controller.abort();
    };
  }, [query, auth.enabled]);
  if (!auth.enabled) return null;
  async function submit(item: SearchItem) {
    setBusy(true);
    setError('');
    setNotice('');
    try {
      const admin = auth.user?.role === 'admin';
      const result = await authRequest(
        admin ? '/api/admin/instruments' : '/api/me/requests',
        acceptedSchema,
        { directoryId: item.id, ...(!admin ? { reason } : {}) },
      );
      const dispatch = result.dispatch;
      setNotice(
        admin
          ? `已加入采集队列。${dispatch?.status === 'unconfigured' ? '执行器尚未配置，可在本机运行采集。' : dispatch?.status === 'failed' ? '触发失败，任务已保留，可在任务队列重试触发。' : '请在任务队列查看进度。'}`
          : '申请已提交，可在我的账号查看处理状态。',
      );
      setSelected(undefined);
      setReason('');
      onQueued?.();
    } catch (error) {
      setError(errorText(error));
    } finally {
      setBusy(false);
    }
  }
  return (
    <section className="panel directory-search" aria-label="在线搜索证券目录">
      <h2>在线搜索与收录</h2>
      <p className="muted">
        按代码、名称或拼音搜索。目录每周更新，持仓和日线在管理员收录后采集。
      </p>
      <label htmlFor="directory-query">搜索全市场目录</label>
      <input
        id="directory-query"
        type="search"
        maxLength={100}
        value={query}
        onChange={(event) => {
          setQuery(event.target.value);
          setSelected(undefined);
        }}
        placeholder="例如 510300、华夏、SOXX"
      />
      {loading && <p role="status">正在搜索…</p>}
      {error && <p role="alert">{error}</p>}
      {notice && <p role="status">{notice}</p>}
      {query.trim() && !loading && !error && !items.length && (
        <p>没有匹配的目录记录，请尝试其他代码或名称。</p>
      )}
      {!!items.length && (
        <ul className="ingest-list">
          {items.map((item) => (
            <li key={item.id}>
              <div>
                <strong>{item.name}</strong>
                <small>
                  {item.symbol} · {item.market} · {kindLabel(item.kind)} ·{' '}
                  {stateLabel(item.status)}
                </small>
              </div>
              {item.instrumentId && item.status !== 'archived' && (
                <Link
                  to={`/instruments/${encodeURIComponent(item.instrumentId)}`}
                >
                  查看详情
                </Link>
              )}
              {auth.user?.role === 'admin' ? (
                <button
                  disabled={
                    busy ||
                    item.status === 'pending' ||
                    item.status === 'archived'
                  }
                  onClick={() => void submit(item)}
                >
                  {item.status === 'ready' ? '重新采集' : '添加并采集'}
                </button>
              ) : (
                item.status !== 'ready' &&
                item.status !== 'archived' &&
                (auth.user ? (
                  <button
                    disabled={busy}
                    onClick={() => {
                      setSelected(item);
                      setReason('');
                    }}
                  >
                    申请收录
                  </button>
                ) : (
                  <Link to="/login?next=%2Ffunds">登录后申请</Link>
                ))
              )}
            </li>
          ))}
        </ul>
      )}
      {updated && query.trim() && (
        <p className="muted">
          目录更新时间：{new Date(updated).toLocaleString('zh-CN')}
        </p>
      )}
      {selected && (
        <form
          className="ingest-form"
          onSubmit={(event) => {
            event.preventDefault();
            void submit(selected);
          }}
        >
          <h3>申请收录 {selected.name}</h3>
          <p>每人每天最多申请 5 只，相同标的只计一次。</p>
          <label htmlFor="collection-reason">申请理由（选填）</label>
          <textarea
            id="collection-reason"
            maxLength={300}
            value={reason}
            onChange={(event) => setReason(event.target.value)}
          />
          <div className="ingest-actions">
            <button className="action" disabled={busy}>
              提交申请
            </button>
            <button
              type="button"
              disabled={busy}
              onClick={() => setSelected(undefined)}
            >
              取消
            </button>
          </div>
        </form>
      )}
    </section>
  );
}
