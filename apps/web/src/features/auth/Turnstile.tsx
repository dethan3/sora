import { useEffect, useRef } from 'react';

type TurnstileApi = {
  render: (container: HTMLElement, options: Record<string, unknown>) => string;
  remove: (id: string) => void;
};
declare global {
  interface Window {
    turnstile?: TurnstileApi;
  }
}
let pending: Promise<TurnstileApi> | undefined;
function loadWidget() {
  if (window.turnstile) return Promise.resolve(window.turnstile);
  if (pending) return pending;
  pending = new Promise<TurnstileApi>((resolve, reject) => {
    const script = document.createElement('script');
    script.src =
      'https://challenges.cloudflare.com/turnstile/v0/api.js?render=explicit';
    script.async = true;
    const timer = window.setTimeout(fail, 12000);
    function fail() {
      clearTimeout(timer);
      script.remove();
      pending = undefined;
      reject(new Error('无法载入人机验证，请检查连接后重试'));
    }
    script.onload = () => {
      clearTimeout(timer);
      if (window.turnstile) resolve(window.turnstile);
      else fail();
    };
    script.onerror = fail;
    document.head.append(script);
  });
  return pending;
}
export function Turnstile({
  siteKey,
  action,
  onToken,
  onError,
}: {
  siteKey: string;
  action: 'login' | 'register';
  onToken: (token: string) => void;
  onError: (error: string) => void;
}) {
  const container = useRef<HTMLDivElement>(null);
  useEffect(() => {
    let active = true;
    let widget: string | undefined;
    let api: TurnstileApi | undefined;
    void loadWidget()
      .then((loaded) => {
        if (!active || !container.current) return;
        api = loaded;
        widget = loaded.render(container.current, {
          sitekey: siteKey,
          action,
          size: 'compact',
          callback: (token: string) => {
            if (active) {
              onToken(token);
              onError('');
            }
          },
          'expired-callback': () => {
            if (active) onToken('');
          },
          'error-callback': () => {
            if (active) {
              onToken('');
              onError('人机验证失败，请重试');
            }
          },
        });
      })
      .catch(() => {
        if (active) onError('无法载入人机验证，请检查连接后重试');
      });
    return () => {
      active = false;
      if (widget !== undefined) api?.remove(widget);
    };
  }, [siteKey, action, onToken, onError]);
  return <div ref={container} aria-label="人机验证" />;
}
