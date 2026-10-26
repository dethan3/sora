const listPaths = ['/funds', '/discover', '/watchlist', '/portfolio'];
const positionKey = 'sora.navigation.v1';
type ReturnPoint = { href: string; code: string; y: number };
let memory: ReturnPoint[] = [];

// Return destinations are explicit application paths, never external URLs.
export function listSource(value: string | null, depth = 0): string | null {
  if (
    !value ||
    depth > 3 ||
    value.length > 1500 ||
    !value.startsWith('/') ||
    value.startsWith('//')
  )
    return null;
  const url = new URL(value, 'https://sora.invalid');
  if (
    url.origin !== 'https://sora.invalid' ||
    (!listPaths.includes(url.pathname) &&
      !/^\/discover\/[a-z0-9-]+$/.test(url.pathname))
  )
    return null;
  const params = new URLSearchParams();
  const keys =
    url.pathname === '/funds'
      ? ['q', 'module']
      : url.pathname === '/portfolio'
        ? ['currency']
        : url.pathname === '/discover'
          ? ['q', 'market', 'kind']
          : [];
  for (const key of keys) {
    const item = url.searchParams.get(key);
    if (item) params.set(key, item);
  }
  if (url.pathname === '/funds' || url.pathname.startsWith('/discover/')) {
    const from = listSource(url.searchParams.get('from'), depth + 1);
    if (
      from?.startsWith('/discover') &&
      (url.pathname === '/funds' || from.split('?')[0] === '/discover')
    )
      params.set('from', from);
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
  const point = { href: listSource(href) ?? '/funds', code, y: window.scrollY };
  memory = [...memory.filter((item) => item.href !== point.href), point].slice(
    -20,
  );
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
      const parsed = JSON.parse(raw);
      const items = Array.isArray(parsed) ? parsed : [parsed];
      memory = items
        .filter(
          (item) =>
            item &&
            typeof item.href === 'string' &&
            item.href === listSource(item.href) &&
            typeof item.code === 'string' &&
            Number.isFinite(item.y) &&
            item.y >= 0 &&
            item.y < 1_000_000,
        )
        .slice(-20);
    } else memory = [];
  } catch {
    /* Use the in-memory point when storage is unavailable. */
  }
  return memory.find((item) => item.href === listSource(href));
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
        ? href.startsWith('/discover/')
          ? '观察方向'
          : '发现市场'
        : '基金目录';
