import {
  createContext,
  useCallback,
  useContext,
  useEffect,
  useRef,
  useState,
  type PropsWithChildren,
} from 'react';
import { useLocation } from 'react-router';
import {
  personalEnvelopeSchema,
  importPreviewSchema,
  type ImportPreview,
  type PersonalEnvelope,
  type importChoicesSchema,
  type z,
} from '@sora/market-data/personal';
import { useAuth } from '../auth/AuthProvider';
import { ApiError, authRequest } from '../auth/api';
import {
  emptyPersonal,
  readPersonal,
  personalSchema,
  STORAGE_KEY,
  writePersonal,
  type PersonalState,
  type Snapshot,
} from './state';

type Status = 'loading' | 'ready' | 'error' | 'conflict';
type PersonalContext = {
  state: PersonalState;
  issue: Snapshot['issue'];
  error: string;
  notice: string;
  raw: string | null;
  mode: 'local' | 'account' | 'blocked';
  status: Status;
  busy: boolean;
  canWrite: boolean;
  device: Snapshot;
  commit: (
    state: PersonalState,
    message?: string,
    recover?: boolean,
  ) => Promise<boolean>;
  refresh: () => Promise<void>;
  previewImport: (state: PersonalState) => Promise<ImportPreview>;
  importData: (
    preview: ImportPreview,
    choices: z.infer<typeof importChoicesSchema>,
  ) => Promise<boolean>;
};
const Context = createContext<PersonalContext | null>(null);
function initial(): Snapshot {
  try {
    return readPersonal(window.localStorage);
  } catch {
    return { state: emptyPersonal(), raw: null, issue: 'unavailable' };
  }
}
export function PersonalProvider({ children }: PropsWithChildren) {
  const auth = useAuth();
  const mode =
    !auth.enabled || (auth.status === 'ready' && !auth.user)
      ? 'local'
      : auth.status === 'ready' && auth.user
        ? 'account'
        : 'blocked';
  const owner = mode === 'account' ? auth.user!.id : mode;
  return (
    <PersonalSession
      key={owner}
      mode={mode}
      owner={owner}
      blockedRefresh={auth.refresh}
      blockedError={auth.status === 'error' ? auth.error : ''}
    >
      {children}
    </PersonalSession>
  );
}
function PersonalSession({
  children,
  mode,
  owner,
  blockedRefresh,
  blockedError,
}: PropsWithChildren<{
  mode: PersonalContext['mode'];
  owner: string;
  blockedRefresh: () => Promise<void>;
  blockedError: string;
}>) {
  const location = useLocation();
  const [device, setDevice] = useState(initial);
  const [snapshot, setSnapshot] = useState<Snapshot>(() =>
    mode === 'local'
      ? device
      : { state: emptyPersonal(), raw: null, issue: null },
  );
  const [status, setStatus] = useState<Status>(
    mode === 'local'
      ? 'ready'
      : mode === 'blocked' && blockedError
        ? 'error'
        : 'loading',
  );
  const current = useRef({ snapshot, revision: 0, status });
  const [error, setError] = useState(''),
    [notice, setNotice] = useState(''),
    [busy, setBusy] = useState(false);
  const readEpoch = useRef(0);
  const writing = useRef(false),
    alive = useRef(true),
    read = useRef<AbortController | null>(null),
    lifetime = useRef(new AbortController());
  const pending = useRef<{
    signature: string;
    path: string;
    body: Record<string, unknown>;
  } | null>(null);
  const setCloud = (data: PersonalEnvelope) => {
    if (data.ownerId !== owner)
      throw new ApiError(401, '当前账号已变化，请重新登录后读取');
    const next = { state: data.state, raw: null, issue: null };
    current.current = {
      snapshot: next,
      revision: data.revision,
      status: 'ready',
    };
    setSnapshot(next);
    setStatus('ready');
  };
  function clearPrivate() {
    const empty = { state: emptyPersonal(), raw: null, issue: null };
    current.current.snapshot = empty;
    setSnapshot(empty);
    void blockedRefresh();
  }
  const refresh = useCallback(async () => {
    if (mode === 'blocked') {
      await blockedRefresh();
      return;
    }
    if (mode === 'local') {
      const next = initial();
      current.current = { ...current.current, snapshot: next };
      setSnapshot(next);
      setDevice(next);
      setError('');
      return;
    }
    if (writing.current) return;
    read.current?.abort();
    const epoch = ++readEpoch.current;
    const controller = new AbortController();
    read.current = controller;
    current.current.status = 'loading';
    setStatus('loading');
    setError('');
    try {
      const data = await authRequest(
        '/api/me/personal',
        personalEnvelopeSchema,
        undefined,
        controller.signal,
      );
      if (
        !alive.current ||
        controller.signal.aborted ||
        epoch !== readEpoch.current
      )
        return;
      setCloud(data);
      pending.current = null;
    } catch (cause) {
      if (
        alive.current &&
        !controller.signal.aborted &&
        epoch === readEpoch.current
      ) {
        current.current.status = 'error';
        setStatus('error');
        if (cause instanceof ApiError && [401, 403].includes(cause.status))
          clearPrivate();
        setError(
          cause instanceof Error ? cause.message : '无法读取账号数据，请重试',
        );
      }
    }
  }, [mode, owner, blockedRefresh]);
  useEffect(() => {
    alive.current = true;
    lifetime.current = new AbortController();
    void refresh();
    return () => {
      alive.current = false;
      read.current?.abort();
      lifetime.current.abort();
    };
  }, [refresh]);
  useEffect(() => {
    setError('');
    setNotice('');
  }, [location.pathname]);
  useEffect(() => {
    const update = (event: StorageEvent) => {
      if (event.key !== STORAGE_KEY && event.key !== null) return;
      const next = initial();
      setDevice(next);
      if (mode === 'local') {
        current.current.snapshot = next;
        setSnapshot(next);
        setError('');
        setNotice('已同步此浏览器其他页面的修改。');
      }
    };
    const focus = () => {
      if (mode === 'account' && !pending.current) void refresh();
    };
    window.addEventListener('storage', update);
    window.addEventListener('focus', focus);
    return () => {
      window.removeEventListener('storage', update);
      window.removeEventListener('focus', focus);
    };
  }, [mode, refresh]);
  async function perform(
    path: string,
    input: Record<string, unknown>,
    message: string,
  ) {
    if (mode !== 'account' || writing.current) {
      setError('数据正在处理，请稍后重试');
      return false;
    }
    const signature = JSON.stringify({ path, input });
    if (pending.current && pending.current.signature !== signature) {
      setError('上次保存结果尚未确认，请重试原操作或重新读取账号数据。');
      return false;
    }
    const operation = pending.current ?? {
      signature,
      path,
      body: { ...input, operationId: crypto.randomUUID(), ownerId: owner },
    };
    pending.current = operation;
    read.current?.abort();
    ++readEpoch.current;
    writing.current = true;
    setBusy(true);
    setError('');
    setNotice('');
    try {
      const data = await authRequest(
        path,
        personalEnvelopeSchema,
        operation.body,
        lifetime.current.signal,
      );
      if (!alive.current) return false;
      setCloud(data);
      pending.current = null;
      setNotice(message);
      return true;
    } catch (cause) {
      if (!alive.current) return false;
      if (cause instanceof ApiError && cause.status < 500) {
        pending.current = null;
        if (cause.status === 409) {
          current.current.status = 'conflict';
          setStatus('conflict');
        } else if (cause.status === 401 || cause.status === 403) {
          current.current.status = 'error';
          setStatus('error');
          clearPrivate();
        }
        if (cause.status === 409 && cause.message.includes('登录账号已变化'))
          clearPrivate();
      }
      setError(
        cause instanceof ApiError
          ? cause.message
          : '保存结果尚未确认，请重试同一操作或重新读取账号数据',
      );
      return false;
    } finally {
      writing.current = false;
      if (alive.current) setBusy(false);
    }
  }
  async function commit(
    state: PersonalState,
    message?: string,
    recover = false,
  ) {
    setError('');
    setNotice('');
    const parsed = personalSchema.safeParse(state);
    if (!parsed.success) {
      setError('数据未通过校验。关注与持仓分别最多支持 200 条，请检查输入。');
      return false;
    }
    if (mode === 'blocked') {
      setError('请先确认登录状态');
      return false;
    }
    if (mode === 'account') {
      if (current.current.status !== 'ready') {
        setError('请先重新读取账号数据，再进行修改');
        return false;
      }
      return perform(
        '/api/me/personal',
        { state: parsed.data, revision: current.current.revision },
        message ?? '已保存到当前账号。',
      );
    }
    if (current.current.snapshot.issue && !recover) {
      setError('请先恢复本地数据，再进行修改。');
      return false;
    }
    try {
      const raw = writePersonal(
        window.localStorage,
        current.current.snapshot.raw,
        parsed.data,
      );
      const next = { state: parsed.data, raw, issue: null };
      current.current.snapshot = next;
      setSnapshot(next);
      setDevice(next);
      setNotice(message ?? '已保存到此浏览器。');
      return true;
    } catch (cause) {
      setError(
        cause instanceof Error && cause.message.includes('另一个页面')
          ? cause.message
          : '无法保存到浏览器，修改未生效。请检查存储权限或空间后重试。',
      );
      return false;
    }
  }
  async function previewImport(state: PersonalState) {
    if (mode !== 'account' || writing.current)
      throw new Error('请等待账号数据读取或保存完成');
    if (pending.current)
      throw new Error('上次保存结果尚未确认，请先重新读取账号数据。');
    read.current?.abort();
    const controller = new AbortController();
    read.current = controller;
    const epoch = ++readEpoch.current;
    const result = await authRequest(
      '/api/me/import/preview',
      importPreviewSchema,
      { ownerId: owner, localState: state },
      AbortSignal.any([lifetime.current.signal, controller.signal]),
    );
    if (
      !alive.current ||
      controller.signal.aborted ||
      epoch !== readEpoch.current ||
      result.ownerId !== owner
    )
      throw new Error('账号已变化，请重新打开导入');
    setCloud({
      ownerId: result.ownerId,
      revision: result.revision,
      state: result.current,
    });
    return result;
  }
  const importData = (
    preview: ImportPreview,
    choices: z.infer<typeof importChoicesSchema>,
  ) => {
    if (preview.ownerId !== owner) return Promise.resolve(false);
    return perform(
      '/api/me/import',
      { localState: preview.eligible, choices, revision: preview.revision },
      '已导入当前账号，原本地数据和备份文件均保留。',
    );
  };
  return (
    <Context.Provider
      value={{
        state: snapshot.state,
        raw: snapshot.raw,
        issue: snapshot.issue,
        error: blockedError || error,
        notice,
        mode,
        status:
          mode === 'blocked' ? (blockedError ? 'error' : 'loading') : status,
        busy,
        canWrite:
          mode !== 'blocked' && status === 'ready' && !snapshot.issue && !busy,
        device,
        commit,
        refresh,
        previewImport,
        importData,
      }}
    >
      {children}
    </Context.Provider>
  );
}
export function usePersonal() {
  const value = useContext(Context);
  if (!value) throw new Error('PersonalProvider missing');
  return value;
}
