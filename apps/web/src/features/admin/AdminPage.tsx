import { IngestAdmin } from '../ingest/IngestAdmin';
import { useEffect, useState, type FormEvent } from 'react';
import { Link, Navigate, useNavigate } from 'react-router';
import { z } from 'zod';
import { ConfirmDialog } from '../../components/ConfirmDialog';
import { ApiError, authRequest } from '../auth/api';
import { useAuth } from '../auth/AuthProvider';
import { InviteIssuer } from '../auth/InviteIssuer';
import {
  actionNames,
  auditSchema,
  invitesSchema,
  mutationSchema,
  usersSchema,
  type AdminUser,
  type Invitation,
  type MutationResult,
} from './api';
import './admin.css';

type Tab = 'users' | 'invites' | 'audit';
type Resource =
  | { tab: 'users'; data: z.infer<typeof usersSchema> }
  | { tab: 'invites'; data: z.infer<typeof invitesSchema> }
  | { tab: 'audit'; data: z.infer<typeof auditSchema> };
type Operation =
  | {
      title: string;
      description: string;
      user: AdminUser;
      body: Record<string, unknown>;
    }
  | { title: string; description: string; invite: Invitation };
const date = (value: string) => new Date(value).toLocaleString('zh-CN');
function auditDescription(action: string, detail: unknown) {
  const data =
    detail && typeof detail === 'object'
      ? (detail as Record<string, unknown>)
      : {};
  if (action === 'user.role') {
    const before = data.before as { role?: string } | undefined;
    const after = data.after as { role?: string } | undefined;
    if (before?.role && after?.role)
      return `角色：${before.role === 'admin' ? '管理员' : '普通用户'} → ${after.role === 'admin' ? '管理员' : '普通用户'}`;
  }
  if (action === 'user.password_reset') return '旧密码和旧会话已失效。';
  if (action === 'user.disable') return '旧会话已失效，该账号已停止登录。';
  if (['user.role', 'user.enable'].includes(action))
    return '该账号需重新登录后继续使用。';
  if (action === 'invite.create' && typeof data.expiresAt === 'string')
    return `有效期至 ${date(data.expiresAt)}`;
  return null;
}

export function AdminPage() {
  const auth = useAuth();
  const navigate = useNavigate();
  const [workspace, setWorkspace] = useState<'accounts' | 'ingest'>('accounts');
  const [outcome, setOutcome] = useState<
    MutationResult & { actorId: string }
  >();
  const [leaving, setLeaving] = useState(false);
  useEffect(() => {
    if (outcome && auth.user && auth.user.id !== outcome.actorId)
      setOutcome(undefined);
  }, [auth.user, outcome]);
  if (outcome?.signedOut && (!auth.user || auth.user.id === outcome.actorId))
    return (
      <section className="panel admin-result">
        <h1>当前账号已更新</h1>
        <p>旧会话已失效，请重新登录。</p>
        {outcome.newPassword && (
          <PasswordResult password={outcome.newPassword} />
        )}
        <button
          className="action"
          disabled={leaving}
          onClick={async () => {
            setLeaving(true);
            await auth.refresh();
            setOutcome(undefined);
            navigate('/login?next=%2Fadmin', { replace: true });
          }}
        >
          重新登录
        </button>
      </section>
    );
  if (!auth.enabled)
    return (
      <section className="panel admin-result">
        <h1>当前版本未启用管理后台</h1>
        <Link to="/funds">浏览基金</Link>
      </section>
    );
  if (auth.status === 'loading')
    return <p role="status">正在确认管理员身份…</p>;
  if (auth.status === 'error')
    return (
      <div role="alert">
        <p>{auth.error}</p>
        <button className="action" onClick={() => void auth.refresh()}>
          重试账号连接
        </button>
      </div>
    );
  if (!auth.user) return <Navigate replace to="/login?next=%2Fadmin" />;
  if (auth.user.role !== 'admin')
    return (
      <section className="panel admin-result">
        <h1>403 · 需要管理员权限</h1>
        <p>当前账号不能访问管理后台。</p>
        <Link to="/funds">返回基金目录</Link>
      </section>
    );
  return (
    <>
      <nav className="ingest-actions" aria-label="管理工作区">
        <button
          aria-pressed={workspace === 'accounts'}
          onClick={() => setWorkspace('accounts')}
        >
          账号与审计
        </button>
        <button
          aria-pressed={workspace === 'ingest'}
          onClick={() => setWorkspace('ingest')}
        >
          公共库与采集
        </button>
      </nav>
      {workspace === 'ingest' ? (
        <IngestAdmin key={auth.user.id} />
      ) : (
        <AdminWorkspace
          key={auth.user.id}
          onResult={(result) => {
            if (result.newPassword || result.signedOut)
              setOutcome({ ...result, actorId: auth.user!.id });
            if (result.signedOut) void auth.refresh();
          }}
        />
      )}
      {outcome?.newPassword && outcome.actorId === auth.user.id && (
        <ConfirmDialog
          title="密码已重置"
          cancelLabel="已保存，关闭"
          onCancel={() => setOutcome(undefined)}
        >
          <PasswordResult password={outcome.newPassword} />
        </ConfirmDialog>
      )}
    </>
  );
}
function PasswordResult({ password }: { password: string }) {
  return (
    <div className="password-result">
      <label htmlFor="reset-password">新密码（仅本次显示）</label>
      <input
        id="reset-password"
        value={password}
        readOnly
        autoComplete="off"
        onFocus={(event) => event.target.select()}
      />
      <p>请保存并交给账号使用者。旧密码和旧会话均已失效。</p>
    </div>
  );
}

function AdminWorkspace({
  onResult,
}: {
  onResult: (result: MutationResult) => void;
}) {
  const auth = useAuth();
  const [tab, setTab] = useState<Tab>('users');
  const [page, setPage] = useState(1);
  const [search, setSearch] = useState('');
  const [query, setQuery] = useState('');
  const [action, setAction] = useState('');
  const [reload, setReload] = useState(0);
  const [resource, setResource] = useState<Resource>();
  const [loading, setLoading] = useState(true);
  const [readError, setReadError] = useState('');
  const [writeError, setWriteError] = useState('');
  const [notice, setNotice] = useState('');
  const [pending, setPending] = useState<Operation>();
  const [busy, setBusy] = useState(false);
  useEffect(() => {
    const controller = new AbortController();
    setLoading(true);
    setReadError('');
    const params = new URLSearchParams({ page: String(page) });
    if (tab === 'users' && query) params.set('q', query);
    if (tab === 'audit' && action) params.set('action', action);
    const load = async () => {
      try {
        if (tab === 'users') {
          const data = await authRequest(
            `/api/admin/users?${params}`,
            usersSchema,
            undefined,
            controller.signal,
          );
          if (!controller.signal.aborted) setResource({ tab, data });
        } else if (tab === 'invites') {
          const data = await authRequest(
            `/api/admin/invites?${params}`,
            invitesSchema,
            undefined,
            controller.signal,
          );
          if (!controller.signal.aborted) setResource({ tab, data });
        } else {
          const data = await authRequest(
            `/api/admin/audit?${params}`,
            auditSchema,
            undefined,
            controller.signal,
          );
          if (!controller.signal.aborted) setResource({ tab, data });
        }
      } catch (cause) {
        if (controller.signal.aborted) return;
        setReadError(
          cause instanceof Error ? cause.message : '读取失败，请重试',
        );
        if (cause instanceof ApiError && [401, 403].includes(cause.status))
          void auth.refresh();
      } finally {
        if (!controller.signal.aborted) setLoading(false);
      }
    };
    void load();
    return () => controller.abort();
  }, [tab, page, query, action, reload]);
  function chooseTab(value: Tab) {
    setTab(value);
    setPage(1);
    setWriteError('');
    setNotice('');
  }
  async function apply(operation: Operation) {
    setPending(undefined);
    setBusy(true);
    setWriteError('');
    setNotice('');
    try {
      if ('user' in operation) {
        const result = await authRequest(
          `/api/admin/users/${operation.user.id}`,
          mutationSchema,
          { ...operation.body, revision: operation.user.revision },
          undefined,
          'PATCH',
        );
        onResult(result);
      } else
        await authRequest(
          `/api/admin/invites/${operation.invite.id}/revoke`,
          z.object({ revoked: z.literal(true) }),
          {},
        );
      setNotice('操作已完成，已记入审计日志。');
      setReload((value) => value + 1);
    } catch (cause) {
      setWriteError(
        cause instanceof Error ? cause.message : '操作失败，请重试',
      );
      if (cause instanceof ApiError && [401, 403].includes(cause.status))
        void auth.refresh();
      if (cause instanceof ApiError && cause.status === 409)
        setReload((value) => value + 1);
    } finally {
      setBusy(false);
    }
  }
  const current = resource?.tab === tab ? resource : undefined;
  function submitSearch(event: FormEvent) {
    event.preventDefault();
    setQuery(search.trim());
    setPage(1);
    setReload((value) => value + 1);
  }
  return (
    <section className="admin-page" aria-label="管理后台">
      <div className="page-header">
        <h1>管理后台</h1>
        <p>管理账号、注册邀请与操作记录。</p>
      </div>
      <nav className="admin-tabs" aria-label="后台栏目">
        {(
          [
            ['users', '用户管理'],
            ['invites', '邀请码管理'],
            ['audit', '审计日志'],
          ] as const
        ).map(([value, label]) => (
          <button
            key={value}
            className="button-quiet"
            aria-pressed={tab === value}
            disabled={busy}
            onClick={() => chooseTab(value)}
          >
            {label}
          </button>
        ))}
      </nav>
      {tab === 'users' && (
        <form onSubmit={submitSearch} className="admin-filter">
          <label htmlFor="admin-search">按邮箱或名称搜索用户</label>
          <input
            id="admin-search"
            type="search"
            maxLength={100}
            value={search}
            onChange={(event) => setSearch(event.target.value)}
          />
          <button className="action" disabled={busy}>
            搜索用户
          </button>
        </form>
      )}
      {tab === 'audit' && (
        <div className="admin-filter">
          <label htmlFor="audit-action">操作类型</label>
          <select
            id="audit-action"
            value={action}
            onChange={(event) => {
              setAction(event.target.value);
              setPage(1);
            }}
          >
            <option value="">全部操作</option>
            {Object.entries(actionNames).map(([key, label]) => (
              <option key={key} value={key}>
                {label}
              </option>
            ))}
          </select>
        </div>
      )}
      {tab === 'invites' && (
        <InviteIssuer
          disabled={busy}
          onCreated={() => {
            setPage(1);
            setReload((value) => value + 1);
          }}
        />
      )}
      {writeError && (
        <p role="alert" className="form-error">
          {writeError}
        </p>
      )}
      {notice && <p role="status">{notice}</p>}
      {loading ? (
        <p role="status">正在读取后台数据…</p>
      ) : readError ? (
        <div role="alert">
          <p>{readError}</p>
          <button
            className="action"
            onClick={() => setReload((value) => value + 1)}
          >
            重新读取
          </button>
        </div>
      ) : (
        current && (
          <>
            <p>共 {current.data.total} 条记录</p>
            {!current.data.items.length && <p>暂无记录。</p>}
            <div className="admin-list">
              {current.tab === 'users' &&
                current.data.items.map((user) => {
                  const lastAdmin =
                    user.role === 'admin' &&
                    !user.disabledAt &&
                    current.data.activeAdminCount <= 1;
                  return (
                    <article
                      className="panel admin-card"
                      aria-label={user.email}
                      key={user.id}
                    >
                      <h2>{user.displayName || user.email}</h2>
                      <p>{user.email}</p>
                      <p>
                        {user.role === 'admin' ? '管理员' : '普通用户'} ·{' '}
                        {user.disabledAt ? '已禁用' : '正常'}
                      </p>
                      <p>注册于 {date(user.createdAt)}</p>
                      {lastAdmin && (
                        <p className="field-help">
                          最后一个可用管理员，不能禁用或取消管理员。
                        </p>
                      )}
                      <div className="button-row">
                        <button
                          className="button-quiet"
                          disabled={busy || lastAdmin}
                          onClick={() =>
                            setPending({
                              user,
                              title: user.disabledAt
                                ? '启用账号？'
                                : '禁用账号？',
                              description: `${user.email}：修改后旧会话将失效，需重新登录。`,
                              body: {
                                action: 'setDisabled',
                                disabled: !user.disabledAt,
                              },
                            })
                          }
                        >
                          {user.disabledAt ? '启用账号' : '禁用账号'}
                        </button>
                        <button
                          className="button-quiet"
                          disabled={busy || lastAdmin}
                          onClick={() =>
                            setPending({
                              user,
                              title:
                                user.role === 'admin'
                                  ? '取消管理员？'
                                  : '设为管理员？',
                              description: `${user.email}：管理员可以管理所有账号和公共资料。修改后该账号需重新登录。`,
                              body: {
                                action: 'setRole',
                                role: user.role === 'admin' ? 'user' : 'admin',
                              },
                            })
                          }
                        >
                          {user.role === 'admin' ? '取消管理员' : '设为管理员'}
                        </button>
                        <button
                          className="button-quiet"
                          disabled={busy}
                          onClick={() =>
                            setPending({
                              user,
                              title: '重置密码？',
                              description: `${user.email}：将生成新密码，并使该账号在所有设备退出。新密码仅本次显示。`,
                              body: { action: 'resetPassword' },
                            })
                          }
                        >
                          重置密码
                        </button>
                      </div>
                    </article>
                  );
                })}
              {current.tab === 'invites' &&
                current.data.items.map((invite) => (
                  <article
                    className="panel admin-card"
                    aria-label={`邀请 ${invite.id.slice(0, 8)}`}
                    key={invite.id}
                  >
                    <h2>
                      {
                        {
                          available: '可使用',
                          used: '已使用',
                          expired: '已过期',
                          revoked: '已撤销',
                        }[invite.status]
                      }
                    </h2>
                    <p>创建者：{invite.createdBy}</p>
                    <p>创建于 {date(invite.createdAt)}</p>
                    <p>有效期至 {date(invite.expiresAt)}</p>
                    {invite.usedBy && <p>使用者：{invite.usedBy}</p>}
                    {invite.status === 'available' && (
                      <button
                        className="button-quiet"
                        disabled={busy}
                        onClick={() =>
                          setPending({
                            invite,
                            title: '撤销邀请码？',
                            description: '撤销后此邀请链接无法继续注册。',
                          })
                        }
                      >
                        撤销邀请码
                      </button>
                    )}
                  </article>
                ))}
              {current.tab === 'audit' &&
                current.data.items.map((item) => (
                  <article className="panel admin-card" key={item.id}>
                    <h2>{actionNames[item.action] ?? item.action}</h2>
                    <p>
                      {date(item.createdAt)} · {item.actorEmail ?? '系统'}
                    </p>
                    {item.target && (
                      <p>
                        对象：
                        {item.targetEmail ??
                          (item.action.startsWith('invite.')
                            ? `邀请 ${item.target.slice(0, 8)}`
                            : item.target)}
                      </p>
                    )}
                    {auditDescription(item.action, item.detail) && (
                      <p>{auditDescription(item.action, item.detail)}</p>
                    )}
                  </article>
                ))}
            </div>
            <div className="admin-pagination">
              <button
                className="button-secondary"
                disabled={busy || page <= 1}
                onClick={() => setPage((value) => value - 1)}
              >
                上一页
              </button>
              <span>第 {page} 页</span>
              <button
                className="button-secondary"
                disabled={
                  busy || page * current.data.pageSize >= current.data.total
                }
                onClick={() => setPage((value) => value + 1)}
              >
                下一页
              </button>
            </div>
          </>
        )
      )}
      {pending && (
        <ConfirmDialog
          title={pending.title}
          confirmLabel="确认操作"
          onCancel={() => setPending(undefined)}
          onConfirm={() => void apply(pending)}
        >
          <p>{pending.description}</p>
        </ConfirmDialog>
      )}
    </section>
  );
}
