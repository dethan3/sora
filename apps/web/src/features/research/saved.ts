import { useEffect, useState } from 'react';
import { z } from 'zod';

export const RESEARCH_KEY = 'sora.research.v1';
export const verdictSchema = z.enum(['valid', 'watch', 'invalid']);
export type ResearchVerdict = z.infer<typeof verdictSchema>;

const schema = z
  .object({
    schemaVersion: z.literal(1),
    ids: z.array(z.string().regex(/^[a-z0-9-]{1,80}$/)).max(100),
    verdicts: z
      .record(z.string().regex(/^[a-z0-9-]{1,80}$/), verdictSchema)
      .optional(),
  })
  .strict()
  .refine((value) => new Set(value.ids).size === value.ids.length);

export function readResearch() {
  try {
    const raw = localStorage.getItem(RESEARCH_KEY);
    const parsed = raw ? schema.parse(JSON.parse(raw)) : undefined;
    return {
      ids: parsed ? parsed.ids : [],
      verdicts: (parsed?.verdicts ?? {}) as Record<string, ResearchVerdict>,
      issue: '',
      raw,
    };
  } catch {
    return {
      ids: [] as string[],
      verdicts: {} as Record<string, ResearchVerdict>,
      issue: '无法读取研究关注，原记录未改动。请检查浏览器存储或恢复备份。',
      raw: null,
    };
  }
}

function serializeResearch(
  ids: string[],
  verdicts: Record<string, ResearchVerdict>,
) {
  const cleaned: Record<string, ResearchVerdict> = {};
  for (const id of ids) {
    if (verdicts[id]) cleaned[id] = verdicts[id];
  }
  const payload =
    Object.keys(cleaned).length > 0
      ? { schemaVersion: 1 as const, ids, verdicts: cleaned }
      : { schemaVersion: 1 as const, ids };
  return schema.parse(payload);
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
      const value = serializeResearch(ids, current.verdicts);
      localStorage.setItem(RESEARCH_KEY, JSON.stringify(value));
      window.dispatchEvent(new Event('sora-research'));
    } catch {
      setError('未能保存研究关注，请检查浏览器存储权限或空间后重试。');
    }
  }
  function setVerdict(id: string, verdict: ResearchVerdict | null) {
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
      const ids = current.ids.includes(id) ? current.ids : [...current.ids, id];
      const nextVerdicts = { ...current.verdicts };
      if (verdict === null) {
        delete nextVerdicts[id];
      } else {
        nextVerdicts[id] = verdict;
      }
      const value = serializeResearch(ids, nextVerdicts);
      localStorage.setItem(RESEARCH_KEY, JSON.stringify(value));
      window.dispatchEvent(new Event('sora-research'));
    } catch {
      setError('未能保存研究复盘状态，请检查浏览器存储权限或空间后重试。');
    }
  }
  return { ...snapshot, error, toggle, setVerdict };
}
