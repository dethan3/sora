import type { ResearchCard } from '@sora/core'
import type { ResearchAgent, ResearchCardInput } from '@sora/agent'
import { generateDeterministicCard } from './deterministic.js'

export class ResearchService {
  constructor(private agent: ResearchAgent | null = null) {}

  async generateCard(input: ResearchCardInput): Promise<ResearchCard> {
    if (this.agent) {
      try {
        return await this.agent.generateResearchCard(input)
      } catch (err) {
        const msg = err instanceof Error ? err.message : String(err)
        console.warn(`[ResearchService] Pi Agent 失败，降级到确定性生成器：${msg}`)
      }
    }
    return generateDeterministicCard(input)
  }
}
