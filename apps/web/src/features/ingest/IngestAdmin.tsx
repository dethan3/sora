import { useEffect, useState, type FormEvent } from 'react';
import { Link } from 'react-router';
import { z, impactTagsSchema } from '@sora/market-data/ingest';
import { authRequest } from '../auth/api';
import { ConfirmDialog } from '../../components/ConfirmDialog';
import { aiChips, aiStorage } from '../domains/model';
import { DirectorySearch } from './DirectorySearch';
import {
  acceptedSchema,
  instrumentsSchema,
  jobsSchema,
  requestsSchema,
  okSchema,
  stateLabel,
  errorText,
  kindLabel,
  type EditableInstrument,
} from './api';
type Resource =
  | { tab: 'instruments'; data: z.infer<typeof instrumentsSchema> }
  | { tab: 'jobs'; data: z.infer<typeof jobsSchema> }
  | { tab: 'requests'; data: z.infer<typeof requestsSchema> };
export function IngestAdmin() {
  const [tab, setTab] = useState<Resource['tab']>('instruments'),
    [reload, setReload] = useState(0);
  const [resource, setResource] = useState<Resource>(),
    [error, setError] = useState(''),
    [notice, setNotice] = useState(''),
    [busy, setBusy] = useState(false);
  const [query, setQuery] = useState(''),
    [editing, setEditing] = useState<EditableInstrument>();
  const [reject, setReject] = useState<{ id: string; name: string }>(),
    [note, setNote] = useState('');
  useEffect(() => {
    const controller = new AbortController();
    setResource(undefined);
    setError('');
    let timer: ReturnType<typeof setTimeout>;
    async function load() {
      try {
        const path = `/api/admin/${tab}${tab === 'instruments' ? `?q=${encodeURIComponent(query)}` : ''}`;
        const data: Resource =
          tab === 'jobs'
            ? {
                tab,
                data: await authRequest(
                  path,
                  jobsSchema,
                  undefined,
                  controller.signal,
                ),
              }
            : tab === 'requests'
              ? {
                  tab,
                  data: await authRequest(
                    path,
                    requestsSchema,
                    undefined,
                    controller.signal,
                  ),
                }
              : {
                  tab,
                  data: await authRequest(
                    path,
                    instrumentsSchema,
                    undefined,
                    controller.signal,
                  ),
                };
        if (!controller.signal.aborted) {
          setResource(data);
          if (tab === 'jobs') timer = setTimeout(() => void load(), 4000);
        }
      } catch (error) {
        if (!controller.signal.aborted) setError(errorText(error));
      }
    }
    const debounce = setTimeout(
      () => void load(),
      tab === 'instruments' ? 250 : 0,
    );
    return () => {
      controller.abort();
      clearTimeout(timer);
      clearTimeout(debounce);
    };
  }, [tab, reload, query]);
  async function mutate(path: string, body: unknown = {}) {
    setBusy(true);
    setError('');
    setNotice('');
    try {
      const data = await authRequest(path, acceptedSchema, body);
      setNotice(
        data.dispatch?.status === 'unconfigured'
          ? '操作已保存。执行器尚未配置，可在本机运行 pnpm data:sync。'
          : data.dispatch?.status === 'failed'
            ? '操作已保存，触发失败；队列会保留，请重试触发。'
            : '操作已保存。',
      );
      setReject(undefined);
      setReload((n) => n + 1);
    } catch (error) {
      setError(errorText(error));
    } finally {
      setBusy(false);
    }
  }
  return (
    <section className="panel ingest-admin">
      <h1>公共库与采集管理</h1>
      <nav aria-label="采集管理分类">
        {(
          [
            ['instruments', '标的管理'],
            ['jobs', '任务队列'],
            ['requests', '收录申请'],
          ] as const
        ).map(([value, label]) => (
          <button
            key={value}
            aria-pressed={tab === value}
            onClick={() => {
              setTab(value);
              setError('');
              setNotice('');
            }}
          >
            {label}
          </button>
        ))}
        <button disabled={busy} onClick={() => setReload((n) => n + 1)}>
          刷新
        </button>
      </nav>
      {error && <p role="alert">{error}</p>}
      {notice && <p role="status">{notice}</p>}
      {tab === 'instruments' && (
        <>
          <DirectorySearch
            onQueued={() => {
              setTab('jobs');
              setReload((n) => n + 1);
            }}
          />
          <label htmlFor="managed-query">筛选已收录标的</label>
          <input
            id="managed-query"
            type="search"
            value={query}
            onChange={(event) => setQuery(event.target.value)}
          />
          <p className="muted">
            持仓中的公司也可单独维护标签与产业链归属。列表最多展示 200
            条，请按名称或代码筛选。
          </p>
        </>
      )}
      {!resource && !error && <p role="status">正在读取…</p>}
      {resource?.tab === 'instruments' && (
        <ul className="ingest-list">
          {resource.data.items.map((item) => (
            <li key={item.id}>
              <div>
                <strong>{item.name}</strong>
                <small>
                  {item.symbol} · {item.market} · {kindLabel(item.kind)} ·{' '}
                  {stateLabel(item.status)}
                </small>
              </div>
              {item.status !== 'archived' && (
                <Link to={`/instruments/${encodeURIComponent(item.id)}`}>
                  详情
                </Link>
              )}
              <button
                disabled={!['ready', 'archived'].includes(item.status) || busy}
                onClick={() => setEditing(item)}
              >
                编辑标的
              </button>
            </li>
          ))}
        </ul>
      )}
      {resource?.tab === 'requests' && (
        <>
          <p>同一标的的待审申请合并展示，按申请人数排序。</p>
          <ul className="ingest-list">
            {resource.data.items.map((item) => (
              <li key={item.id}>
                <div>
                  <strong>
                    {item.name} · {item.symbol}
                  </strong>
                  <small>
                    {item.market} · {kindLabel(item.kind)} ·{' '}
                    {item.requesterCount} 人申请
                  </small>
                  {item.reason && <p>申请理由示例：{item.reason}</p>}
                </div>
                <button
                  disabled={busy}
                  onClick={() =>
                    void mutate(
                      `/api/admin/requests/${encodeURIComponent(item.id)}/approve`,
                    )
                  }
                >
                  批准并入库
                </button>
                <button
                  disabled={busy}
                  onClick={() => {
                    setReject(item);
                    setNote('');
                  }}
                >
                  驳回
                </button>
              </li>
            ))}
          </ul>
          {!resource.data.items.length && <p>暂无待审申请。</p>}
        </>
      )}
      {resource?.tab === 'jobs' && (
        <>
          <p>
            执行器按批次处理队列。步骤与状态每 4 秒刷新，最多展示最近 100 条。
          </p>
          <button
            disabled={busy}
            onClick={() => void mutate('/api/admin/jobs/dispatch')}
          >
            触发执行器
          </button>
          {resource.data.dispatch && (
            <p>
              最近触发：
              {(
                {
                  sent: '已发送到 GitHub',
                  failed: '触发失败',
                  unconfigured: '尚未配置',
                } as Record<string, string>
              )[resource.data.dispatch.status] ?? resource.data.dispatch.status}
              {resource.data.dispatch.error
                ? ` · ${resource.data.dispatch.error}`
                : ''}
            </p>
          )}
          <ul className="ingest-list">
            {resource.data.items.map((item) => (
              <li key={item.id}>
                <div>
                  <strong>
                    {item.name} · {item.symbol}
                  </strong>
                  <small>
                    {stateLabel(item.status)} ·{' '}
                    {item.mode === 'refresh' ? '增量日线' : '完整采集'} · 尝试{' '}
                    {item.attempt} 次
                  </small>
                  <p>
                    {item.steps
                      .map(
                        (step) =>
                          `${{ metadata: '元数据', holdings: '重仓股', series: '日线' }[step.step]}：${{ pending: '等待', running: '采集中', done: '完成', failed: '失败' }[step.status]}`,
                      )
                      .join(' → ')}
                  </p>
                  {item.error && <p role="alert">{item.error}</p>}
                  <small>
                    {new Date(item.createdAt).toLocaleString('zh-CN')}
                  </small>
                </div>
                <Link
                  to={`/instruments/${encodeURIComponent(item.instrumentId)}`}
                >
                  详情
                </Link>
                {item.status === 'failed' && (
                  <button
                    disabled={busy}
                    onClick={() =>
                      void mutate(
                        `/api/admin/jobs/${encodeURIComponent(item.id)}/retry`,
                      )
                    }
                  >
                    重试任务
                  </button>
                )}
              </li>
            ))}
          </ul>
          {!resource.data.items.length && <p>暂无采集任务。</p>}
        </>
      )}
      {reject && (
        <ConfirmDialog
          title={`驳回 ${reject.name} 的收录申请`}
          onCancel={() => !busy && setReject(undefined)}
        >
          <form
            className="ingest-form"
            onSubmit={(event) => {
              event.preventDefault();
              void mutate(
                `/api/admin/requests/${encodeURIComponent(reject.id)}/reject`,
                { note },
              );
            }}
          >
            <label htmlFor="reject-note">驳回理由</label>
            <textarea
              id="reject-note"
              required
              maxLength={300}
              value={note}
              onChange={(event) => setNote(event.target.value)}
            />
            <button className="action" disabled={busy || !note.trim()}>
              确认驳回
            </button>
          </form>
        </ConfirmDialog>
      )}
      {editing && (
        <EditInstrument
          key={`${editing.id}:${editing.revision}`}
          item={editing}
          onClose={() => setEditing(undefined)}
          onSaved={() => {
            setEditing(undefined);
            setReload((n) => n + 1);
            setNotice('标的资料已更新。');
          }}
        />
      )}
    </section>
  );
}
function EditInstrument({
  item,
  onClose,
  onSaved,
}: {
  item: EditableInstrument;
  onClose: () => void;
  onSaved: () => void;
}) {
  const [tags, setTags] = useState(item.impactTags),
    [node, setNode] = useState(item.chainNode ?? ''),
    [status, setStatus] = useState(item.status),
    [busy, setBusy] = useState(false),
    [error, setError] = useState('');
  async function save(event: FormEvent) {
    event.preventDefault();
    setBusy(true);
    setError('');
    try {
      const parsed = impactTagsSchema.parse(tags);
      await authRequest(
        `/api/admin/instruments/${encodeURIComponent(item.id)}`,
        okSchema,
        {
          impactTags: parsed,
          chainNode: node || null,
          status,
          revision: item.revision,
        },
        undefined,
        'PATCH',
      );
      onSaved();
    } catch (error) {
      setError(errorText(error));
    } finally {
      setBusy(false);
    }
  }
  return (
    <ConfirmDialog
      title={`编辑 ${item.name}`}
      onCancel={() => !busy && onClose()}
    >
      <form className="ingest-form" onSubmit={(event) => void save(event)}>
        <label htmlFor="instrument-status">上架状态</label>
        <select
          id="instrument-status"
          value={status}
          onChange={(event) => setStatus(event.target.value)}
        >
          <option value="ready">上架</option>
          <option value="archived">下架</option>
        </select>
        <p className="muted">
          下架后公共目录与详情将不可见，已有采集不会恢复上架状态。
        </p>
        <label htmlFor="chain-node">产业链环节</label>
        <select
          id="chain-node"
          value={node}
          onChange={(event) => setNode(event.target.value)}
        >
          <option value="">待分类</option>
          {[aiStorage, aiChips].flatMap((domain) =>
            (domain.nodes ?? []).map((entry) => (
              <option
                key={`${domain.id}:${entry.id}`}
                value={`${domain.id}:${entry.id}`}
              >
                {domain.name} · {entry.title}
              </option>
            )),
          )}
          {node &&
            ![aiStorage, aiChips].some((domain) =>
              domain.nodes?.some(
                (entry) => `${domain.id}:${entry.id}` === node,
              ),
            ) && <option value={node}>{node}</option>}
        </select>
        <h3>影响胶囊（最多 5 个）</h3>
        {tags.map((tag, index) => (
          <div key={index} className="ingest-actions">
            <input
              aria-label={`标签 ${index + 1}`}
              value={tag.label}
              minLength={2}
              maxLength={16}
              required
              onChange={(event) =>
                setTags(
                  tags.map((row, i) =>
                    i === index ? { ...row, label: event.target.value } : row,
                  ),
                )
              }
            />
            <select
              aria-label={`标签 ${index + 1} 方向`}
              value={tag.tone}
              onChange={(event) =>
                setTags(
                  tags.map((row, i) =>
                    i === index
                      ? { ...row, tone: event.target.value as typeof tag.tone }
                      : row,
                  ),
                )
              }
            >
              <option value="positive">正向</option>
              <option value="headwind">压力</option>
              <option value="watch">观察</option>
            </select>
            <button
              type="button"
              onClick={() => setTags(tags.filter((_, i) => i !== index))}
            >
              移除标签
            </button>
          </div>
        ))}
        <button
          type="button"
          disabled={tags.length >= 5}
          onClick={() => setTags([...tags, { label: '', tone: 'watch' }])}
        >
          添加标签
        </button>
        {error && <p role="alert">{error}</p>}
        <button className="action" disabled={busy}>
          保存标的
        </button>
      </form>
    </ConfirmDialog>
  );
}
