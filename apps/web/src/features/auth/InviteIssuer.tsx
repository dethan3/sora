import { useState } from 'react';
import { z } from 'zod';
import { ApiError, authRequest } from './api';
import { useAuth } from './AuthProvider';

const inviteSchema = z.object({
  invite: z.string(),
  expiresAt: z.string(),
  registerPath: z.string().regex(/^\/register\?invite=[a-f0-9]{64}$/),
});
export function InviteIssuer({
  disabled = false,
  onCreated,
}: {
  disabled?: boolean;
  onCreated?: () => void;
}) {
  const auth = useAuth();
  const [invitation, setInvitation] = useState<z.infer<typeof inviteSchema>>();
  const [error, setError] = useState('');
  const [busy, setBusy] = useState(false);
  return (
    <section aria-label="生成邀请码" className="invite-issuer">
      <h2>邀请注册</h2>
      <p>邀请码七天有效，仅可使用一次。生成后请自行保存并交给受邀人。</p>
      <button
        className="action"
        disabled={disabled || busy}
        onClick={async () => {
          setBusy(true);
          setError('');
          setInvitation(undefined);
          try {
            setInvitation(
              await authRequest('/api/admin/invites', inviteSchema, {}),
            );
            onCreated?.();
          } catch (cause) {
            if (cause instanceof ApiError && [401, 403].includes(cause.status))
              void auth.refresh();
            setError(
              cause instanceof Error ? cause.message : '生成失败，请重试',
            );
          } finally {
            setBusy(false);
          }
        }}
      >
        {busy ? '正在生成…' : '生成邀请码'}
      </button>
      {invitation && (
        <div role="status">
          <label htmlFor="invite-link">邀请链接（仅本次显示）</label>
          <input
            id="invite-link"
            readOnly
            autoComplete="off"
            value={
              new URL(invitation.registerPath, window.location.origin).href
            }
            onFocus={(event) => event.target.select()}
          />
          <p>
            有效期至 {new Date(invitation.expiresAt).toLocaleString('zh-CN')}
          </p>
        </div>
      )}
      {error && <p role="alert">{error}</p>}
    </section>
  );
}
