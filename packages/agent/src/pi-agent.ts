import { writeCache } from '@sora/shared'
import { ResearchCardSchema } from '@sora/core'
import type { ResearchCard } from '@sora/core'
import type { ResearchAgent, ResearchCardInput } from './types.js'

const SYSTEM_PROMPT = `你是专业的指数基金研究分析师，服务于面向中国投资者的全球宽基指数基金研究平台 Sora。

职责：
1. 基于提供的市场数据和基金执行质量分析，生成结构化研究卡片
2. 严守合规边界：只输出信息分析，不提供任何买卖建议
3. 输出必须为合法 JSON，严格符合以下字段（不得增删）：
   title, marketId, relatedIndexIds, relatedFundIds, summary, keyEvidence,
   fundExecutionRisks, marketImplication, risks, invalidationConditions, status

合规声明：本平台所有分析内容仅供参考，不构成投资建议。
status 枚举值：ignore | watch | active_watch | confirmed | invalidated`

interface PiConfig {
  apiKey: string
  baseUrl: string
  cacheDir: string
  model?: string
}

interface PiChatResponse {
  choices?: Array<{
    message?: { content?: string }
  }>
}

export class PiResearchAgent implements ResearchAgent {
  private model: string

  constructor(private config: PiConfig) {
    this.model = config.model ?? 'inflection-3-productivity'
  }

  async generateResearchCard(input: ResearchCardInput): Promise<ResearchCard> {
    const userContent = JSON.stringify({
      market: input.market,
      indexes: input.indexes,
      fundAnalyses: input.fundAnalyses,
      searchResults: input.searchResults ?? [],
    }, null, 2)

    const response = await fetch(`${this.config.baseUrl}/chat/completions`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Authorization': `Bearer ${this.config.apiKey}`,
      },
      body: JSON.stringify({
        model: this.model,
        messages: [
          { role: 'system', content: SYSTEM_PROMPT },
          { role: 'user', content: userContent },
        ],
        response_format: { type: 'json_object' },
      }),
    })

    if (!response.ok) {
      throw new Error(`Pi API error: ${response.status} ${response.statusText}`)
    }

    const raw = (await response.json()) as PiChatResponse
    const content = raw.choices?.[0]?.message?.content
    if (!content) throw new Error('Pi API returned empty content')

    const parsed = JSON.parse(content) as Record<string, unknown>
    const now = new Date().toISOString()

    const cardData = {
      id: `card-${input.market.id}-${now.split('T')[0]}`,
      generatedAt: now,
      ...parsed,
    }

    const validated = ResearchCardSchema.parse(cardData)

    writeCache(this.config.cacheDir, `agent-responses/pi-${input.market.id}-${now.split('T')[0]}`, 'pi', raw)

    return validated
  }

  async summarizeMarketSignal(signals: string[]): Promise<string> {
    const response = await fetch(`${this.config.baseUrl}/chat/completions`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Authorization': `Bearer ${this.config.apiKey}`,
      },
      body: JSON.stringify({
        model: this.model,
        messages: [
          { role: 'system', content: SYSTEM_PROMPT },
          { role: 'user', content: `请用2-3句话总结以下市场信号：\n${signals.join('\n')}` },
        ],
      }),
    })

    if (!response.ok) throw new Error(`Pi API error: ${response.status}`)
    const raw = (await response.json()) as PiChatResponse
    return raw.choices?.[0]?.message?.content ?? ''
  }

  async explainTransmissionPath(marketId: string): Promise<string> {
    const response = await fetch(`${this.config.baseUrl}/chat/completions`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Authorization': `Bearer ${this.config.apiKey}`,
      },
      body: JSON.stringify({
        model: this.model,
        messages: [
          { role: 'system', content: SYSTEM_PROMPT },
          { role: 'user', content: `请简要说明 ${marketId} 市场与中国投资者可投基金的传导路径（仅分析，不做投资建议）。` },
        ],
      }),
    })

    if (!response.ok) throw new Error(`Pi API error: ${response.status}`)
    const raw = (await response.json()) as PiChatResponse
    return raw.choices?.[0]?.message?.content ?? ''
  }

  async generateFollowUpTasks(card: ResearchCard): Promise<string[]> {
    const response = await fetch(`${this.config.baseUrl}/chat/completions`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Authorization': `Bearer ${this.config.apiKey}`,
      },
      body: JSON.stringify({
        model: this.model,
        messages: [
          { role: 'system', content: SYSTEM_PROMPT },
          { role: 'user', content: `基于以下研究卡片，列出3-5个后续跟进任务（输出JSON数组，每项为字符串）：\n${JSON.stringify(card)}` },
        ],
        response_format: { type: 'json_object' },
      }),
    })

    if (!response.ok) throw new Error(`Pi API error: ${response.status}`)
    const raw = (await response.json()) as PiChatResponse
    const content = raw.choices?.[0]?.message?.content ?? '{"tasks":[]}'
    const parsed = JSON.parse(content) as { tasks?: string[] }
    return parsed.tasks ?? []
  }
}
