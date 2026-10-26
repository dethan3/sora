import { act, render, screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { createPwaRuntime, type PwaRuntime } from './runtime';
import { PwaActions, PwaNotice } from './PwaUi';

const runtimes: PwaRuntime[] = [];
beforeEach(() => {
  localStorage.clear();
  vi.spyOn(navigator, 'onLine', 'get').mockReturnValue(true);
});
afterEach(() => {
  runtimes.splice(0).forEach((runtime) => runtime.stop());
  vi.useRealTimers();
});

function setup(waiting = false, controlled = true) {
  const worker = Object.assign(new EventTarget(), {
    state: waiting ? 'installed' : 'activated',
    postMessage: vi.fn(),
  });
  const registration = Object.assign(new EventTarget(), {
    waiting: waiting ? worker : null,
    active: waiting ? null : worker,
    installing: null as typeof worker | null,
    update: vi.fn().mockResolvedValue(undefined),
  });
  const container = Object.assign(new EventTarget(), {
    controller: controlled ? {} : null,
    register: vi.fn().mockResolvedValue(registration),
    ready: Promise.resolve(registration),
  });
  const reload = vi.fn();
  const runtime = createPwaRuntime({
    enabled: true,
    serviceWorker: container as unknown as ServiceWorkerContainer,
    reload,
  });
  runtimes.push(runtime);
  return { runtime, worker, registration, container, reload };
}

describe('PWA lifecycle and user data', () => {
  it('registers once and prepares offline use without asking for an initial update', async () => {
    const { runtime, container, reload } = setup(false, false);
    runtime.start();
    runtime.start();
    await waitFor(() => expect(runtime.getSnapshot().ready).toBe(true));
    expect(container.register).toHaveBeenCalledExactlyOnceWith('/sw.js', {
      updateViaCache: 'none',
    });
    container.dispatchEvent(new Event('controllerchange'));
    expect(runtime.getSnapshot().update).toBeNull();
    expect(reload).not.toHaveBeenCalled();
  });

  it('keeps ordinary web use available without registering in development', () => {
    const { container } = setup();
    const runtime = createPwaRuntime({
      enabled: false,
      serviceWorker: container as unknown as ServiceWorkerContainer,
    });
    runtimes.push(runtime);
    runtime.start();
    expect(container.register).not.toHaveBeenCalled();
    expect(runtime.getSnapshot().supported).toBe(false);
  });

  it('shows explicit cached-snapshot messaging offline and removes it on reconnect', async () => {
    const { runtime } = setup();
    runtime.start();
    await waitFor(() => expect(runtime.getSnapshot().ready).toBe(true));
    render(<PwaNotice runtime={runtime} />);
    act(() => window.dispatchEvent(new Event('offline')));
    expect(screen.getByRole('status')).toHaveTextContent(
      '使用此设备缓存的数据',
    );
    act(() => window.dispatchEvent(new Event('online')));
    expect(screen.queryByText(/当前离线/)).not.toBeInTheDocument();
  });

  it('warns before offline preparation has completed', () => {
    const { runtime, container } = setup();
    container.register.mockReturnValue(new Promise(() => {}));
    runtime.start();
    render(<PwaNotice runtime={runtime} />);
    act(() => window.dispatchEvent(new Event('offline')));
    expect(screen.getByRole('status')).toHaveTextContent('离线内容尚未准备好');
  });

  it('waits for confirmation, preserves unsaved input on cancel and saved personal data on update', async () => {
    const { runtime, worker, container, reload } = setup(true);
    const personal = JSON.stringify({
      schemaVersion: 1,
      watchlist: ['DEMO-CN'],
      positions: [
        { code: 'DEMO-CN', units: '10', costMinor: 1000, currency: 'CNY' },
      ],
    });
    localStorage.setItem('sora.personal.v1', personal);
    runtime.start();
    await waitFor(() => expect(runtime.getSnapshot().update).toBe('waiting'));
    render(
      <>
        <PwaNotice runtime={runtime} />
        <input aria-label="未保存输入" defaultValue="123.45" />
      </>,
    );
    expect(reload).not.toHaveBeenCalled();
    await userEvent.click(screen.getByRole('button', { name: '更新应用' }));
    expect(screen.getByRole('dialog')).toHaveAccessibleDescription(
      /尚未保存的输入会丢失/,
    );
    await userEvent.click(screen.getByRole('button', { name: '取消' }));
    expect(screen.getByRole('textbox')).toHaveValue('123.45');
    expect(worker.postMessage).not.toHaveBeenCalled();
    await userEvent.click(screen.getByRole('button', { name: '更新应用' }));
    await userEvent.click(screen.getByRole('button', { name: '更新并刷新' }));
    expect(worker.postMessage).toHaveBeenCalledExactlyOnceWith({
      type: 'SKIP_WAITING',
    });
    act(() => container.dispatchEvent(new Event('controllerchange')));
    expect(reload).toHaveBeenCalledTimes(1);
    expect(localStorage.getItem('sora.personal.v1')).toBe(personal);
  });

  it('does not force reload when another tab activates a new version', async () => {
    const { runtime, container, reload } = setup();
    runtime.start();
    await waitFor(() => expect(runtime.getSnapshot().ready).toBe(true));
    container.dispatchEvent(new Event('controllerchange'));
    expect(reload).not.toHaveBeenCalled();
    expect(runtime.getSnapshot().update).toBe('active');
    runtime.applyUpdate();
    expect(reload).toHaveBeenCalledTimes(1);
  });

  it('detects updates installed after the page opens', async () => {
    const { runtime, registration, worker } = setup();
    runtime.start();
    await waitFor(() => expect(runtime.getSnapshot().ready).toBe(true));
    worker.state = 'installing';
    registration.installing = worker;
    registration.dispatchEvent(new Event('updatefound'));
    worker.state = 'installed';
    registration.waiting = worker;
    worker.dispatchEvent(new Event('statechange'));
    expect(runtime.getSnapshot().update).toBe('waiting');
  });

  it('retries a failed registration and failed update checks', async () => {
    const { runtime, container, registration } = setup();
    container.register.mockRejectedValueOnce(new Error('offline'));
    runtime.start();
    await waitFor(() =>
      expect(runtime.getSnapshot().error).toContain('尚未准备好'),
    );
    await runtime.checkUpdate();
    expect(runtime.getSnapshot().ready).toBe(true);
    registration.update.mockRejectedValueOnce(new Error('unavailable'));
    await runtime.checkUpdate();
    expect(runtime.getSnapshot().error).toContain('暂时无法检查更新');
    await runtime.checkUpdate();
    expect(runtime.getSnapshot().error).toBe('');
    expect(runtime.getSnapshot().ready).toBe(true);
  });

  it('recovers from an update activation timeout without a forced reload', async () => {
    const { runtime, reload } = setup(true);
    runtime.start();
    await waitFor(() => expect(runtime.getSnapshot().update).toBe('waiting'));
    vi.useFakeTimers();
    runtime.applyUpdate();
    vi.advanceTimersByTime(20_000);
    expect(runtime.getSnapshot().updating).toBe(false);
    expect(runtime.getSnapshot().error).toContain('稍后重试');
    expect(reload).not.toHaveBeenCalled();
  });

  it('offers installation only on a real browser prompt and consumes it once', async () => {
    const { runtime } = setup();
    runtime.start();
    render(<PwaActions runtime={runtime} />);
    expect(
      screen.queryByRole('button', { name: '安装 Sora' }),
    ).not.toBeInTheDocument();
    const prompt = vi.fn().mockResolvedValue({ outcome: 'dismissed' });
    const event = Object.assign(
      new Event('beforeinstallprompt', { cancelable: true }),
      { prompt },
    );
    act(() => window.dispatchEvent(event));
    expect(event.defaultPrevented).toBe(true);
    await userEvent.click(screen.getByRole('button', { name: '安装 Sora' }));
    await runtime.install();
    expect(prompt).toHaveBeenCalledTimes(1);
    expect(
      screen.getByRole('button', { name: '安装说明' }),
    ).toBeInTheDocument();
    act(() => window.dispatchEvent(new Event('appinstalled')));
    expect(
      screen.queryByRole('button', { name: '安装说明' }),
    ).not.toBeInTheDocument();
  });

  it('provides platform instructions and returns focus on closing', async () => {
    const { runtime } = setup();
    render(<PwaActions runtime={runtime} />);
    const trigger = screen.getByRole('button', { name: '安装说明' });
    await userEvent.click(trigger);
    expect(screen.getByRole('dialog')).toHaveTextContent('Safari');
    expect(screen.getByRole('dialog')).toHaveTextContent('不会自动同步');
    await userEvent.click(screen.getByRole('button', { name: '知道了' }));
    expect(trigger).toHaveFocus();
  });
});
