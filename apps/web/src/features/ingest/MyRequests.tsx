import { useEffect, useState } from 'react';
import { Link } from 'react-router';
import { authRequest } from '../auth/api';
import { myRequestsSchema, stateLabel, errorText } from './api';
import type { z } from '@sora/market-data/ingest';
export function MyRequests() {
  const [data, setData] = useState<z.infer<typeof myRequestsSchema>>();
  const [error, setError] = useState(''),
    [reload, setReload] = useState(0);
  useEffect(() => {
    const controller = new AbortController();
    setError('');
    void authRequest(
      '/api/me/requests',
      myRequestsSchema,
      undefined,
      controller.signal,
    )
      .then((data) => {
        if (!controller.signal.aborted) setData(data);
      })
      .catch((error) => {
        if (!controller.signal.aborted) setError(errorText(error));
      });
    return () => controller.abort();
  }, [reload]);
  return (
    <section aria-label="我的收录申请">
      <h2>我的收录申请</h2>
      <button onClick={() => setReload((n) => n + 1)}>刷新申请状态</button>
      {error && <p role="alert">{error}</p>}
      {!data && !error && <p role="status">正在读取申请…</p>}
      {data && !data.items.length && (
        <p>还没有收录申请，可在基金页搜索并提交。</p>
      )}
      <ul className="ingest-list">
        {data?.items.map((item) => (
          <li key={item.id}>
            <div>
              <strong>
                {item.name} · {item.symbol}
              </strong>
              <small>
                {item.status === 'pending'
                  ? '等待审核'
                  : stateLabel(item.status)}
                {item.status === 'approved'
                  ? ` · ${stateLabel(item.instrumentStatus)}`
                  : ''}
              </small>
              {item.reviewNote && <p>驳回理由：{item.reviewNote}</p>}
            </div>
            {item.instrumentId && item.instrumentStatus !== 'archived' && (
              <Link
                to={`/instruments/${encodeURIComponent(item.instrumentId)}`}
              >
                查看标的
              </Link>
            )}
          </li>
        ))}
      </ul>
    </section>
  );
}
