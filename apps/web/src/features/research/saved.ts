import { useEffect, useState } from 'react';
import { z } from 'zod';

export const RESEARCH_KEY = 'sora.research.v1';
const schema = z
  .object({
    schemaVersion: z.literal(1),
    ids: z.array(z.string().regex(/^[a-z0-9-]{1,80}$/)).max(100),
  })
  .strict()
  .refine((value) => new Set(value.ids).size === value.ids.length);
export function readResearch() {
  try {
    const raw = localStorage.getItem(RESEARCH_KEY);
    return {
      ids: raw ? schema.parse(JSON.parse(raw)).ids : [],
      issue: '',
      raw,
    };
  } catch {
    return {
      ids: [] as string[],
      issue: '无法读取研究关注，原记录未改动。请检查浏览器存储或恢复备份。',
      raw: null,
    };
  }
}
export function useResearchSaved() {
  const [snapshot, setSnapshot] = useState(readResearch);
  const [error, setError] = useState('');
  useEffect(() => {
    const sync = (event: Event) => {
      if (
        event instanceof StorageEvent &&
        event.key !== RESEARCH_KEY &&
        event.key !== null
      )
        return;
      setSnapshot(readResearch());
      setError('');
    };
    window.addEventListener('storage', sync);
    window.addEventListener('sora-research', sync);
    return () => {
      window.removeEventListener('storage', sync);
      window.removeEventListener('sora-research', sync);
    };
  }, []);
  function toggle(id: string) {
    const current = readResearch();
    if (current.issue) {
      setSnapshot(current);
      return;
    }
    if (current.raw !== snapshot.raw) {
      setSnapshot(current);
      setError('研究关注已在其他页面变化，请重试。');
      return;
    }
    try {
      const ids = current.ids.includes(id)
        ? current.ids.filter((item) => item !== id)
        : [...current.ids, id];
      const value = schema.parse({ schemaVersion: 1, ids });
      localStorage.setItem(RESEARCH_KEY, JSON.stringify(value));
      window.dispatchEvent(new Event('sora-research'));
    } catch {
      setError('未能保存研究关注，请检查浏览器存储权限或空间后重试。');
    }
  }
  return { ...snapshot, error, toggle };
}
