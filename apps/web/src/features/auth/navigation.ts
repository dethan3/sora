import {
  detailSource,
  listSource,
  researchSource,
} from '../../navigation/flow';
export function loginReturn(value: string | null) {
  if (value === '/' || value === '/account' || value === '/admin') return value;
  return (
    detailSource(value) ??
    researchSource(value) ??
    listSource(value) ??
    '/funds'
  );
}
