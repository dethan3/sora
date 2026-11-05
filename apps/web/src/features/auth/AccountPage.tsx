import { LocalNotice, BackupTools } from '../local/BackupTools';
import { MyRequests } from '../ingest/MyRequests';
import '../ingest/ingest.css';
import { useState } from 'react';
import { Link, Navigate } from 'react-router';
import { useAuth } from './AuthProvider';
import './auth.css';
import { InviteIssuer } from './InviteIssuer';

export function AccountPage() {
  const auth = useAuth();
  const [error, setError] = useState('');
  const [busy, setBusy] = useState(false);
  if (!auth.enabled)
    return (
      <section className="panel auth-panel">
        <h1>当前版本未启用账号服务</h1>
        <Link to="/funds">浏览基金</Link>
      </section>
    );
  if (auth.status === 'loading') return <p role="status">正在确认登录…</p>;
  if (auth.status === 'error')
    return (
      <div role="alert">
        <p>{auth.error}</p>
        <button className="action" onClick={() => void auth.refresh()}>
          重试账号连接
        </button>
      </div>
    );
  if (!auth.user) return <Navigate replace to="/login?next=%2Faccount" />;
  return (
    <section className="panel auth-panel">
      <h1>我的账号</h1>
      <p>
        {auth.user.displayName} · {auth.user.email}
      </p>
      <p>{auth.user.role === 'admin' ? '管理员' : '普通用户'}</p>
      <LocalNotice />
      <BackupTools />
      <MyRequests key={auth.user.id} />
      <button
        className="button-quiet"
        disabled={busy}
        onClick={async () => {
          setBusy(true);
          setError('');
          try {
            await auth.logout();
          } catch {
            setError('退出失败，请重试');
          } finally {
            setBusy(false);
          }
        }}
      >
        退出登录
      </button>
      {error && <p role="alert">{error}</p>}
      {auth.user.role === 'admin' && (
        <>
          <p>
            <Link to="/admin">进入管理后台</Link>
          </p>
          <InviteIssuer disabled={busy} />
        </>
      )}
    </section>
  );
}
