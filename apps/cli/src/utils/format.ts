export const DISCLAIMER =
  '\n─────────────────────────────────────────────────────\nℹ️  以上内容为信息分析，不构成任何投资建议。\n'

export function printDisclaimer(): void {
  console.log(DISCLAIMER)
}

export function dispWidth(s: string): number {
  let w = 0
  for (const c of s) {
    w += c.charCodeAt(0) > 127 ? 2 : 1
  }
  return w
}

function padEnd(s: string, targetWidth: number): string {
  const dw = dispWidth(s)
  return s + ' '.repeat(Math.max(0, targetWidth - dw))
}

export function printTable(headers: string[], rows: string[][]): void {
  const allRows = [headers, ...rows]
  const colWidths = headers.map((_, ci) =>
    Math.max(...allRows.map((r) => dispWidth(r[ci] ?? ''))) + 2
  )

  const renderRow = (row: string[]) =>
    row.map((cell, ci) => padEnd(cell ?? '', colWidths[ci])).join('')

  console.log(renderRow(headers))
  console.log(colWidths.map((w) => '─'.repeat(w)).join(''))
  rows.forEach((r) => console.log(renderRow(r)))
}

export function warnIcon(level: string): string {
  if (level === 'warning') return '⚠️ '
  if (level === 'critical') return '🚨'
  if (level === 'watch') return '👁 '
  return ''
}

export function fmtPct(v: number | null | undefined, decimals = 2): string {
  if (v == null) return 'N/A'
  const sign = v >= 0 ? '+' : ''
  return `${sign}${(v * 100).toFixed(decimals)}%`
}

export function fmtNum(v: number | null | undefined, decimals = 2): string {
  if (v == null) return 'N/A'
  return v.toFixed(decimals)
}
