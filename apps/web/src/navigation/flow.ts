const listPaths = ['/funds', '/discover', '/watchlist', '/portfolio'];
const positionKey = 'sora.navigation.v1';
type ReturnPoint = { href: string; code: string; y: number };
let memory: ReturnPoint | undefined;

// Return destinations are explicit application paths, never external URLs.
export function listSource(value: string | null): string | null {
  if (
    !value ||
    value.length > 1500 ||
    !value.startsWith('/') ||
    value.startsWith('//')
  )
    return null;
  const url = new URL(value, 'https://sora.invalid');
  if (
    url.origin !== 'https://sora.invalid' ||
    !listPaths.includes(url.pathname)
  )
    return null;
  const params = new URLSearchParams();
  const keys =
    url.pathname === '/funds'
      ? ['q', 'module']
      : url.pathname === '/portfolio'
        ? ['currency']
        : [];
  for (const key of keys) {
    const item = url.searchParams.get(key);
    if (item) params.set(key, item);
  }
  return url.pathname + (params.size ? `?${params}` : '');
}

export function detailUrl(code: string, source: string) {
  const from = listSource(source) ?? '/funds';
  return `/funds/${encodeURIComponent(code)}?${new URLSearchParams({ from })}`;
}

export function detailSource(value: string | null): string | null {
  if (
    !value ||
    value.length > 2000 ||
    !value.startsWith('/') ||
    value.startsWith('//')
  )
    return null;
  const url = new URL(value, 'https://sora.invalid');
  if (
    url.origin !== 'https://sora.invalid' ||
    !/^\/funds\/[^/]+$/.test(url.pathname)
  )
    return null;
  const from = listSource(url.searchParams.get('from'));
  return url.pathname + (from ? `?${new URLSearchParams({ from })}` : '');
}

export function rememberList(href: string, code: string) {
  memory = { href: listSource(href) ?? '/funds', code, y: window.scrollY };
  try {
    sessionStorage.setItem(positionKey, JSON.stringify(memory));
  } catch {
    /* Navigation still works without session storage. */
  }
}

export function returnPoint(href: string): ReturnPoint | undefined {
  try {
    const raw = sessionStorage.getItem(positionKey);
    if (raw) {
      const item = JSON.parse(raw) as ReturnPoint;
      if (
        typeof item.href === 'string' &&
        typeof item.code === 'string' &&
        Number.isFinite(item.y) &&
        item.y >= 0 &&
        item.y < 1_000_000
      )
        memory = item;
    } else memory = undefined;
  } catch {
    /* Use the in-memory point when storage is unavailable. */
  }
  return memory?.href === listSource(href) ? memory : undefined;
}

export const fundLinkId = (code: string) =>
  `fund-link-${encodeURIComponent(code)}`;
export const editPositionId = (code: string) =>
  `edit-position-${encodeURIComponent(code)}`;
export const sourceLabel = (href: string) =>
  href.startsWith('/watchlist')
    ? '我的关注'
    : href.startsWith('/portfolio')
      ? '我的组合'
      : href.startsWith('/discover')
        ? '发现市场'
        : '基金目录';
