import { useCallback, useEffect, useState, type FormEvent } from 'react';
import { Link, Navigate, useLocation } from 'react-router';
import { z } from 'zod';
import { authRequest } from './api';
import { useAuth } from './AuthProvider';
import { loginReturn } from './navigation';
import { Turnstile } from './Turnstile';
import './auth.css';

export function AuthPage({ mode }: { mode: 'login' | 'register' }) {
  const auth = useAuth();
  const location = useLocation();
  const params = new URLSearchParams(location.search);
  const destination = loginReturn(params.get('next'));
  const [email, setEmail] = useState('');
  const [password, setPassword] = useState('');
  const [invite, setInvite] = useState(params.get('invite') ?? '');
  const [displayName, setDisplayName] = useState('');
  const [siteKey, setSiteKey] = useState('');
  const [token, setToken] = useState('');
  const [error, setError] = useState('');
  const [challengeError, setChallengeError] = useState('');
  const [retry, setRetry] = useState(0);
  const [submitting, setSubmitting] = useState(false);
  const onToken = useCallback((value: string) => setToken(value), []);
  const onError = useCallback((value: string) => setChallengeError(value), []);
  useEffect(() => {
    if (!auth.enabled) return;
    const controller = new AbortController();
    setSiteKey('');
    setToken('');
    setChallengeError('');
    void authRequest(
      '/api/auth/config',
      z.object({ siteKey: z.string().min(1) }),
      undefined,
      controller.signal,
    )
      .then((result) => {
        if (!controller.signal.aborted) setSiteKey(result.siteKey);
      })
      .catch(() => {
        if (!controller.signal.aborted)
          setChallengeError('登录验证暂不可用，请重试或联系管理员');
      });
    return () => controller.abort();
  }, [auth.enabled, retry]);
  if (!auth.enabled)
    return (
      <section className="auth-panel panel">
        <h1>当前版本未启用账号服务</h1>
        <p>你可以继续浏览，关注与持仓保存在此浏览器。</p>
        <Link to="/funds">浏览基金</Link>
      </section>
    );
  if (auth.user) return <Navigate replace to={destination} />;
  async function submit(event: FormEvent) {
    event.preventDefault();
    if (!token || submitting) return;
    setSubmitting(true);
    setError('');
    try {
      await auth.authenticate(mode, {
        email,
        password,
        turnstileToken: token,
        ...(mode === 'register' ? { invite, displayName } : {}),
      });
      setPassword('');
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : '提交失败，请重试');
      setToken('');
      setRetry((value) => value + 1);
    } finally {
      setSubmitting(false);
    }
  }
  return (
    <section className="panel auth-panel">
      <p className="eyebrow">SORA ACCOUNT</p>
      <h1>{mode === 'login' ? '登录 Sora' : '邀请码注册'}</h1>
      <p>
        {mode === 'register'
          ? '使用管理员提供的一次性邀请码创建账号。'
          : '使用邮箱和密码登录，忘记密码请联系管理员。'}
      </p>
      <form onSubmit={submit}>
        <fieldset disabled={submitting} className="auth-fields">
          <label htmlFor="auth-email">邮箱</label>
          <input
            id="auth-email"
            type="email"
            required
            maxLength={254}
            autoComplete="username"
            value={email}
            onChange={(event) => setEmail(event.target.value)}
          />
          {mode === 'register' && (
            <>
              <label htmlFor="auth-name">显示名称（可选）</label>
              <input
                id="auth-name"
                maxLength={60}
                autoComplete="nickname"
                value={displayName}
                onChange={(event) => setDisplayName(event.target.value)}
              />
              <label htmlFor="auth-invite">邀请码</label>
              <input
                id="auth-invite"
                required
                maxLength={64}
                minLength={64}
                autoComplete="off"
                spellCheck={false}
                value={invite}
                onChange={(event) => setInvite(event.target.value)}
              />
            </>
          )}
          <label htmlFor="auth-password">
            密码{mode === 'register' ? '（12～128 个字符）' : ''}
          </label>
          <input
            id="auth-password"
            type="password"
            required
            minLength={mode === 'register' ? 12 : 1}
            maxLength={128}
            autoComplete={
              mode === 'register' ? 'new-password' : 'current-password'
            }
            value={password}
            onChange={(event) => setPassword(event.target.value)}
          />
        </fieldset>
        {siteKey && (
          <Turnstile
            key={`${mode}-${retry}`}
            siteKey={siteKey}
            action={mode}
            onToken={onToken}
            onError={onError}
          />
        )}
        {challengeError && (
          <div role="alert">
            <p>{challengeError}</p>
            <button
              className="button-quiet"
              type="button"
              onClick={() => setRetry((value) => value + 1)}
            >
              重试人机验证
            </button>
          </div>
        )}
        {error && (
          <p role="alert" className="form-error">
            {error}
          </p>
        )}
        <button
          className="action"
          disabled={!token || submitting}
          type="submit"
        >
          {submitting ? '正在提交…' : mode === 'login' ? '登录' : '创建账号'}
        </button>
      </form>
      <p className="field-help">
        登录后，基金关注与持仓保存到当前账号；未登录时保存在此浏览器。方向关注与研究复盘仍保存在本地。
      </p>
      <Link
        to={`/${mode === 'login' ? 'register' : 'login'}?${new URLSearchParams({ next: destination })}`}
      >
        {mode === 'login' ? '有邀请码？去注册' : '已有账号？去登录'}
      </Link>
    </section>
  );
}
