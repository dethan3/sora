import { describe, it, expect } from 'vitest'
import { parseJsonp } from '../jsonp.js'

describe('parseJsonp', () => {
  it('parses standard JSONP callback format', () => {
    const raw = 'jsonpgz({"fundcode":"159941","name":"易方达纳指ETF","dwjz":"1.523"});'
    const result = parseJsonp(raw) as Record<string, string>
    expect(result.fundcode).toBe('159941')
    expect(result.dwjz).toBe('1.523')
  })

  it('parses JSONP callback without trailing semicolon', () => {
    const raw = 'cb({"key":"value"})'
    const result = parseJsonp(raw) as Record<string, string>
    expect(result.key).toBe('value')
  })

  it('parses var assignment format', () => {
    const raw = 'var r = {"code":"159941","data":[1,2,3]};'
    const result = parseJsonp(raw) as Record<string, unknown>
    expect(result.code).toBe('159941')
    expect(result.data).toEqual([1, 2, 3])
  })

  it('parses var assignment without spaces', () => {
    const raw = 'var r={"x":42}'
    const result = parseJsonp(raw) as Record<string, number>
    expect(result.x).toBe(42)
  })

  it('parses array response in JSONP', () => {
    const raw = 'cb([{"id":1},{"id":2}])'
    const result = parseJsonp(raw) as Array<{ id: number }>
    expect(result).toHaveLength(2)
    expect(result[0].id).toBe(1)
  })

  it('throws on unrecognised format', () => {
    expect(() => parseJsonp('plain text content')).toThrow('parseJsonp: unrecognised format')
  })

  it('throws on empty string', () => {
    expect(() => parseJsonp('')).toThrow('parseJsonp: unrecognised format')
  })
})
