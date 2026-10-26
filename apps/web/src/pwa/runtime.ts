export interface InstallPrompt extends Event {
  prompt(): Promise<{ outcome: 'accepted' | 'dismissed' }>;
}

export type PwaState = {
  supported: boolean;
  online: boolean;
  ready: boolean;
  update: 'waiting' | 'active' | null;
  updating: boolean;
  canInstall: boolean;
  installed: boolean;
  error: string;
};

// Application caches and the personal localStorage store have separate lifecycles.
export function createPwaRuntime({
  enabled = import.meta.env.PROD,
  serviceWorker = navigator.serviceWorker,
  reload = () => window.location.reload(),
}: {
  enabled?: boolean;
  serviceWorker?: ServiceWorkerContainer;
  reload?: () => void;
} = {}) {
  let state: PwaState = {
    supported: enabled && !!serviceWorker,
    online: navigator.onLine,
    ready: false,
    update: null,
    updating: false,
    canInstall: false,
    installed:
      window.matchMedia?.('(display-mode: standalone)').matches ||
      !!(navigator as Navigator & { standalone?: boolean }).standalone,
    error: '',
  };
  let registration: ServiceWorkerRegistration | undefined;
  let prompt: InstallPrompt | undefined;
  let registering = false;
  let started = false;
  let reloadRequested = false;
  let controlled = !!serviceWorker?.controller;
  let lastCheck = 0;
  let updateTimeout: ReturnType<typeof setTimeout> | undefined;
  const listeners = new Set<() => void>();
  const removers: (() => void)[] = [];
  function set(patch: Partial<PwaState>) {
    state = { ...state, ...patch };
    listeners.forEach((listener) => listener());
  }
  function listen(target: EventTarget, type: string, listener: EventListener) {
    target.addEventListener(type, listener);
    removers.push(() => target.removeEventListener(type, listener));
  }
  function watch(worker: ServiceWorker) {
    const changed = () => {
      if (
        worker.state === 'installed' &&
        registration?.waiting &&
        serviceWorker?.controller
      )
        set({ update: 'waiting', error: '' });
      if (worker.state === 'activated') set({ ready: true });
      if (worker.state === 'redundant')
        set({ error: '离线内容准备失败，联网后可重试。' });
    };
    listen(worker, 'statechange', changed);
    changed();
  }
  async function register() {
    if (!enabled || !serviceWorker || registering || registration) return;
    registering = true;
    set({ error: '' });
    try {
      const result = await serviceWorker.register('/sw.js', {
        updateViaCache: 'none',
      });
      if (!started) return;
      registration = result;
      if (result.waiting && serviceWorker.controller)
        set({ update: 'waiting' });
      if (result.active?.state === 'activated') set({ ready: true });
      if (result.installing) watch(result.installing);
      listen(result, 'updatefound', () => {
        if (result.installing) watch(result.installing);
      });
      void serviceWorker.ready.then(() => {
        if (started) set({ ready: true });
      });
    } catch {
      if (started) set({ error: '离线内容尚未准备好，联网后可重试。' });
    } finally {
      registering = false;
    }
  }
  async function checkUpdate() {
    if (!navigator.onLine) return;
    if (!registration) return register();
    if (registration.installing || Date.now() - lastCheck < 60_000) return;
    lastCheck = Date.now();
    try {
      await registration.update();
      set({ error: '' });
    } catch {
      lastCheck = 0;
      set({ error: '暂时无法检查更新，当前内容仍可使用。' });
    }
  }
  return {
    getSnapshot: () => state,
    subscribe(listener: () => void) {
      listeners.add(listener);
      return () => listeners.delete(listener);
    },
    start() {
      if (started) return;
      started = true;
      listen(window, 'online', () => {
        set({ online: true });
        void checkUpdate();
      });
      listen(window, 'offline', () => set({ online: false }));
      listen(window, 'beforeinstallprompt', (event) => {
        event.preventDefault();
        prompt = event as InstallPrompt;
        if (!state.installed) set({ canInstall: true });
      });
      listen(window, 'appinstalled', () => {
        prompt = undefined;
        set({ installed: true, canInstall: false });
      });
      listen(document, 'visibilitychange', () => {
        if (document.visibilityState === 'visible') void checkUpdate();
      });
      if (enabled && serviceWorker) {
        listen(serviceWorker, 'controllerchange', () => {
          clearTimeout(updateTimeout);
          if (reloadRequested) {
            reloadRequested = false;
            reload();
          } else if (controlled) {
            // Another tab may activate an update. Never reload a form automatically.
            set({ update: 'active', updating: false });
          }
          controlled = true;
          set({ ready: true });
        });
        void register();
        const timer = setInterval(() => void checkUpdate(), 60 * 60_000);
        removers.push(() => clearInterval(timer));
      }
    },
    stop() {
      started = false;
      clearTimeout(updateTimeout);
      removers.splice(0).forEach((remove) => remove());
    },
    checkUpdate,
    async install() {
      const pending = prompt;
      prompt = undefined;
      set({ canInstall: false });
      if (!pending) return;
      try {
        const choice = await pending.prompt();
        if (choice.outcome === 'accepted') set({ installed: true });
      } catch {
        set({ error: '未能打开安装窗口，可使用浏览器菜单安装。' });
      }
    },
    applyUpdate() {
      if (!state.update || state.updating) return;
      if (state.update === 'active' || !registration?.waiting) {
        reload();
        return;
      }
      set({ updating: true, error: '' });
      reloadRequested = true;
      try {
        registration.waiting.postMessage({ type: 'SKIP_WAITING' });
      } catch {
        reloadRequested = false;
        set({ updating: false, error: '更新暂未完成，请稍后重试。' });
        return;
      }
      updateTimeout = setTimeout(() => {
        reloadRequested = false;
        set({ updating: false, error: '更新暂未完成，请稍后重试。' });
      }, 20_000);
    },
  };
}

export type PwaRuntime = ReturnType<typeof createPwaRuntime>;
export const pwa = createPwaRuntime();
