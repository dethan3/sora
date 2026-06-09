import { readCache, writeCache, isCacheStale, ONE_DAY_MS, SEVEN_DAYS_MS, parseJsonp } from '@sora/shared'
import type { IFundDataSource, RawFundDetails, RawNavRecord, RawFundMetrics } from './types.js'

const NAV_URL = (code: string) => `http://fundgz.1234567.com.cn/js/${code}.js`
const HISTORY_URL = (code: string, per: number) =>
  `https://fund.eastmoney.com/f10/F10DataApi.aspx?type=lsjz&code=${code}&page=1&per=${per}`

interface EastmoneyNavRaw {
  fundcode: string
  name: string
  jzrq: string
  dwjz: string
  gsz?: string
  gszzl?: string
}

interface EastmoneyHistoryRaw {
  Data?: {
    LSJZList?: Array<{ FSRQ: string; DWJZ: string; JZZZL?: string }>
  }
  ErrCode?: number
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms))
}

interface Config {
  cacheDir: string
  requestDelayMs?: number
}

export class EastMoneyFundSource implements IFundDataSource {
  private delay: number

  constructor(private config: Config) {
    this.delay = config.requestDelayMs ?? 500
  }

  async getFundDetails(fundCode: string): Promise<RawFundDetails> {
    const cacheKey = `fund-details/${fundCode}`
    const cached = readCache<RawFundDetails>(this.config.cacheDir, cacheKey)
    if (cached && !isCacheStale(cached, SEVEN_DAYS_MS)) {
      return cached.data
    }

    const resp = await fetch(NAV_URL(fundCode))
    const text = await resp.text()
    const raw = parseJsonp(text) as EastmoneyNavRaw

    const result: RawFundDetails = {
      fundCode: raw.fundcode,
      fundName: raw.name,
      nav: parseFloat(raw.dwjz),
      navDate: raw.jzrq,
      estimatedNav: raw.gsz ? parseFloat(raw.gsz) : null,
      estimatedChangePercent: raw.gszzl ? parseFloat(raw.gszzl) : null,
      dataSource: 'eastmoney',
      fetchedAt: new Date().toISOString(),
    }

    writeCache(this.config.cacheDir, cacheKey, 'eastmoney', result)
    await sleep(this.delay)
    return result
  }

  async getFundNavHistory(fundCode: string, days: number): Promise<RawNavRecord[]> {
    const cacheKey = `fund-nav-history/${fundCode}-${days}d`
    const cached = readCache<RawNavRecord[]>(this.config.cacheDir, cacheKey)
    if (cached && !isCacheStale(cached, ONE_DAY_MS)) {
      return cached.data
    }

    const per = Math.min(Math.ceil(days * 0.75), 300)
    const resp = await fetch(HISTORY_URL(fundCode, per))
    const json = (await resp.json()) as EastmoneyHistoryRaw

    const list = json.Data?.LSJZList ?? []
    const result: RawNavRecord[] = list.map((item) => ({
      date: item.FSRQ,
      nav: parseFloat(item.DWJZ),
      changePercent: item.JZZZL ? parseFloat(item.JZZZL) : null,
    }))

    writeCache(this.config.cacheDir, cacheKey, 'eastmoney', result)
    await sleep(this.delay)
    return result
  }

  async getFundMetrics(fundCode: string): Promise<RawFundMetrics> {
    const details = await this.getFundDetails(fundCode)
    return {
      fundCode,
      nav: details.nav,
      price: null,
      premiumRate: null,
      volume: null,
      turnover: null,
      snapshotDate: details.navDate,
      dataSource: 'eastmoney',
    }
  }
}
