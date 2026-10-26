import { useState, useSyncExternalStore } from 'react';
import { ConfirmDialog } from '../components/ConfirmDialog';
import { pwa, type PwaRuntime } from './runtime';

export function PwaNotice({ runtime = pwa }: { runtime?: PwaRuntime }) {
  const state = useSyncExternalStore(runtime.subscribe, runtime.getSnapshot);
  const [confirm, setConfirm] = useState(false);
  return (
    <>
      {!state.online && (
        <div className="pwa-notice" role="status">
          <span className="pwa-dot" aria-hidden="true" />
          <span>
            {state.ready
              ? '当前离线 · 使用此设备缓存的数据，请以页面日期为准。'
              : '当前离线 · 离线内容尚未准备好，部分页面可能无法打开。'}
          </span>
        </div>
      )}
      {state.update && (
        <div className="pwa-notice pwa-update">
          <span role="status">有可用新版本，保存当前输入后可更新。</span>
          <button
            className="button-quiet"
            disabled={state.updating}
            onClick={() => setConfirm(true)}
          >
            {state.updating ? '正在更新…' : '更新应用'}
          </button>
        </div>
      )}
      {confirm && (
        <ConfirmDialog
          title="更新并刷新 Sora？"
          confirmLabel="更新并刷新"
          onCancel={() => setConfirm(false)}
          onConfirm={() => {
            setConfirm(false);
            runtime.applyUpdate();
          }}
        >
          <p>
            已保存的关注与持仓会保留。当前页面尚未保存的输入会丢失，请先保存，再更新。
          </p>
          <p>其他已打开的 Sora 页面不会被自动刷新。</p>
        </ConfirmDialog>
      )}
    </>
  );
}

export function PwaActions({ runtime = pwa }: { runtime?: PwaRuntime }) {
  const state = useSyncExternalStore(runtime.subscribe, runtime.getSnapshot);
  const [help, setHelp] = useState(false);
  return (
    <div className="pwa-actions">
      {state.supported && (
        <span className="pwa-readiness" role="status">
          {state.error || (state.ready ? '可离线使用' : '正在准备离线内容…')}
        </span>
      )}
      {state.error && state.supported && (
        <button
          className="button-quiet"
          onClick={() => void runtime.checkUpdate()}
        >
          重试
        </button>
      )}
      {!state.installed && (
        <button
          className="button-quiet"
          onClick={() =>
            state.canInstall ? void runtime.install() : setHelp(true)
          }
        >
          {state.canInstall ? '安装 Sora' : '安装说明'}
        </button>
      )}
      {help && (
        <ConfirmDialog
          title="将 Sora 添加到桌面"
          confirmLabel="知道了"
          onCancel={() => setHelp(false)}
          onConfirm={() => setHelp(false)}
        >
          <p>
            iPhone / iPad：在 Safari 中打开，点“分享”，再选择“添加到主屏幕”。
          </p>
          <p>
            Android /
            电脑：在支持的浏览器菜单中选择“安装应用”或“添加到主屏幕”。若没有该选项，可继续使用网页版。
          </p>
          <p>
            首次联网打开，显示“可离线使用”后即可离线访问。数据保存在此设备，不会自动同步到其他设备。
          </p>
        </ConfirmDialog>
      )}
    </div>
  );
}
