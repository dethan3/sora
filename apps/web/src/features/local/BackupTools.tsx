import { useState, type ChangeEvent } from 'react';
import { ConfirmDialog } from '../../components/ConfirmDialog';
import { usePersonal } from './PersonalProvider';
import {
  emptyPersonal,
  MAX_IMPORT_BYTES,
  parsePersonal,
  type PersonalState,
} from './state';
import { AccountImportDialog, AccountImportOffer } from './AccountImport';
import { downloadText } from './download';
export { downloadText } from './download';

export function BackupTools({
  onBeforeReplace = (action) => action(),
}: {
  onBeforeReplace?: (action: () => void) => void;
}) {
  const personal = usePersonal(),
    account = personal.mode === 'account';
  const [pending, setPending] = useState<{
      state: PersonalState;
      title: string;
    }>(),
    [importing, setImporting] = useState<PersonalState>(),
    [error, setError] = useState('');
  const hasData =
    personal.state.positions.length > 0 || personal.state.watchlist.length > 0;
  async function importFile(event: ChangeEvent<HTMLInputElement>) {
    const file = event.target.files?.[0];
    event.target.value = '';
    setError('');
    if (!file) return;
    try {
      if (file.size > MAX_IMPORT_BYTES)
        throw new Error('请选择 1 MB 以内的 JSON 备份');
      const state = parsePersonal(await file.text());
      onBeforeReplace(() =>
        account
          ? setImporting(state)
          : setPending({ state, title: '导入并替换当前本地数据？' }),
      );
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : '无法读取文件');
    }
  }
  if (personal.mode === 'blocked') return <p>确认登录状态后可管理个人数据。</p>;
  return (
    <section
      className="local-tools"
      aria-label={account ? '账号数据管理' : '本地数据管理'}
    >
      <div className="local-tools-copy">
        <strong>
          {account ? '基金关注与持仓保存在当前账号' : '数据保存在此浏览器'}
        </strong>
        <p>
          {account
            ? '可导出当前账号备份；导入会合并关注，并逐项处理持仓冲突。方向关注和研究复盘仍在本地。'
            : '换设备前可导出备份；导入将替换关注与持仓，不会上传服务器。'}
        </p>
      </div>
      <div className="button-row">
        <button
          className="button-secondary"
          disabled={
            !!personal.issue ||
            !hasData ||
            personal.busy ||
            personal.status === 'loading'
          }
          onClick={() =>
            downloadText(
              JSON.stringify(personal.state, null, 2),
              account ? 'sora-account-backup.json' : 'sora-backup.json',
            )
          }
        >
          导出备份
        </button>
        <label className="button-secondary file-button">
          导入备份
          <input
            aria-label="导入备份"
            type="file"
            accept=".json,application/json"
            disabled={personal.busy || (account && personal.status !== 'ready')}
            onChange={importFile}
          />
        </label>
        <button
          className="button-quiet danger"
          disabled={
            personal.busy ||
            (account && !personal.canWrite) ||
            (!personal.issue && !hasData)
          }
          onClick={() =>
            onBeforeReplace(() =>
              setPending({
                state: emptyPersonal(),
                title: account
                  ? '清空当前账号的关注与持仓？'
                  : '清空本地关注与持仓？',
              }),
            )
          }
        >
          清空数据
        </button>
      </div>
      {error && (
        <p role="alert" className="form-error">
          {error}
        </p>
      )}
      {pending && (
        <ConfirmDialog
          title={pending.title}
          confirmLabel="确认替换"
          busy={personal.busy}
          onCancel={() => setPending(undefined)}
          onConfirm={async () => {
            if (
              await personal.commit(
                pending.state,
                account ? '账号数据已更新。' : '本地数据已更新。',
                true,
              )
            )
              setPending(undefined);
          }}
        >
          <p>
            替换后将有 {pending.state.watchlist.length} 项关注、
            {pending.state.positions.length}{' '}
            笔持仓。当前数据不会自动合并，请先导出需要保留的内容。
            {account
              ? '此操作会同步到当前账号的其他设备，不修改原本地记录。'
              : ''}
          </p>
          {personal.error && (
            <p role="alert" className="form-error">
              {personal.error}
            </p>
          )}
        </ConfirmDialog>
      )}
      {importing && (
        <AccountImportDialog
          source={importing}
          onClose={() => setImporting(undefined)}
        />
      )}
    </section>
  );
}
export function LocalNotice() {
  const personal = usePersonal();
  return (
    <>
      {personal.mode === 'blocked' ? (
        <div
          className="local-warning"
          role={personal.status === 'error' ? 'alert' : 'status'}
        >
          <p>
            {personal.status === 'error'
              ? '无法确认账号，个人数据暂不可用。'
              : '正在确认账号…'}
          </p>
          {personal.status === 'error' && (
            <button
              className="button-secondary"
              onClick={() => void personal.refresh()}
            >
              重试账号连接
            </button>
          )}
        </div>
      ) : personal.mode === 'account' ? (
        <div className="personal-sync-status">
          <p role="status">
            {personal.busy
              ? '正在保存到当前账号…'
              : personal.status === 'loading'
                ? '正在读取账号的关注与持仓…'
                : personal.status === 'conflict'
                  ? '其他页面或设备已修改数据，请重新读取后再操作。'
                  : personal.status === 'error'
                    ? '账号数据暂不可用，未切换为本地保存。'
                    : '基金关注与持仓保存在当前账号。'}
          </p>
          <button
            className="button-quiet"
            disabled={personal.busy || personal.status === 'loading'}
            onClick={() => void personal.refresh()}
          >
            重新读取账号数据
          </button>
        </div>
      ) : null}
      {personal.mode === 'local' && personal.issue && (
        <div className="local-warning" role="alert">
          <strong>
            {personal.issue === 'corrupt'
              ? '本地数据无法读取，原始内容已保留。'
              : '当前浏览器无法访问本地存储。'}
          </strong>
          <p>未自动重置或覆盖。可在下方导入有效备份，或确认清空后重新开始。</p>
          {personal.raw !== null && (
            <button
              className="button-secondary"
              onClick={() => downloadText(personal.raw!, 'sora-recovery.json')}
            >
              下载原始数据
            </button>
          )}
        </div>
      )}
      {personal.error && (
        <p
          className="form-error"
          role="alert"
          id="personal-save-error"
          tabIndex={-1}
        >
          {personal.error}
        </p>
      )}
      {personal.notice && (
        <p className="save-notice" role="status">
          {personal.notice}
        </p>
      )}
      <AccountImportOffer />
    </>
  );
}
