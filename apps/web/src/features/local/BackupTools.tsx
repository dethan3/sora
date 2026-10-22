import { useState, type ChangeEvent } from 'react';
import { ConfirmDialog } from '../../components/ConfirmDialog';
import { usePersonal } from './PersonalProvider';
import {
  emptyPersonal,
  MAX_IMPORT_BYTES,
  parsePersonal,
  type PersonalState,
} from './state';

export function downloadText(text: string, filename: string) {
  const url = URL.createObjectURL(
    new Blob([text], { type: 'application/json;charset=utf-8' }),
  );
  const link = document.createElement('a');
  link.href = url;
  link.download = filename;
  link.click();
  setTimeout(() => URL.revokeObjectURL(url), 1000);
}
export function BackupTools({
  onBeforeReplace = (action) => action(),
}: {
  onBeforeReplace?: (action: () => void) => void;
}) {
  const local = usePersonal();
  const [pending, setPending] = useState<{
    state: PersonalState;
    title: string;
  }>();
  const [error, setError] = useState('');
  const hasData =
    local.state.positions.length > 0 || local.state.watchlist.length > 0;
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
        setPending({ state, title: '导入并替换当前本地数据？' }),
      );
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : '无法读取文件');
    }
  }
  return (
    <section className="local-tools" aria-label="本地数据管理">
      <div className="local-tools-copy">
        <strong>数据保存在此浏览器</strong>
        <p>换设备前可导出备份；导入将替换关注与持仓，不会上传服务器。</p>
      </div>
      <div className="button-row">
        <button
          className="button-secondary"
          disabled={!!local.issue || !hasData}
          onClick={() =>
            downloadText(
              JSON.stringify(local.state, null, 2),
              'sora-backup.json',
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
            onChange={importFile}
          />
        </label>
        <button
          className="button-quiet danger"
          disabled={!local.issue && !hasData}
          onClick={() =>
            onBeforeReplace(() =>
              setPending({
                state: emptyPersonal(),
                title: '清空本地关注与持仓？',
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
          onCancel={() => setPending(undefined)}
          onConfirm={() => {
            if (local.commit(pending.state, '本地数据已更新。', true))
              setPending(undefined);
          }}
        >
          <p>
            替换后将有 {pending.state.watchlist.length} 项关注、
            {pending.state.positions.length}{' '}
            笔持仓。当前数据不会自动合并，请先导出需要保留的内容。
          </p>
          {local.error && (
            <p role="alert" className="form-error">
              {local.error}
            </p>
          )}
        </ConfirmDialog>
      )}
    </section>
  );
}
export function LocalNotice() {
  const local = usePersonal();
  return (
    <>
      {local.issue && (
        <div className="local-warning" role="alert">
          <strong>
            {local.issue === 'corrupt'
              ? '本地数据无法读取，原始内容已保留。'
              : '当前浏览器无法访问本地存储。'}
          </strong>
          <p>未自动重置或覆盖。可在下方导入有效备份，或确认清空后重新开始。</p>
          {local.raw !== null && (
            <button
              className="button-secondary"
              onClick={() => downloadText(local.raw!, 'sora-recovery.json')}
            >
              下载原始数据
            </button>
          )}
        </div>
      )}
      {local.error && (
        <p
          className="form-error"
          role="alert"
          id="personal-save-error"
          tabIndex={-1}
        >
          {local.error}
        </p>
      )}
      {local.notice && (
        <p className="save-notice" role="status">
          {local.notice}
        </p>
      )}
    </>
  );
}
