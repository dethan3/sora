/**
 * Eastmoney JSONP response formats:
 *   1. `callbackName({...});`  — e.g. fundgz.1234567.com.cn/js/{code}.js
 *   2. `var name = {...};`     — e.g. some static JS endpoints
 */
export function parseJsonp(content: string): unknown {
  const trimmed = content.trim()

  const fnCallMatch = trimmed.match(/^\w[\w.]*\(([\s\S]+)\);?\s*$/)
  if (fnCallMatch) {
    return JSON.parse(fnCallMatch[1])
  }

  const varMatch = trimmed.match(/^(?:var\s+\w+\s*=\s*)([\s\S]+?);?\s*$/)
  if (varMatch) {
    return JSON.parse(varMatch[1])
  }

  throw new Error(`parseJsonp: unrecognised format — "${trimmed.slice(0, 80)}"`)
}
