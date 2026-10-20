import { ActionLink, PageHeader, Panel } from '../components/ui';

export function Welcome() {
  return (
    <>
      <PageHeader
        eyebrow="A CLEARER PERSPECTIVE"
        title="看清每一份投资。"
        description="少一点信息噪音，多一点从容。由市场出发，逐步认识你的基金与组合。"
      />
      <div className="welcome-grid">
        <section className="welcome-story">
          <span className="index-label">01 / 从这里开始</span>
          <h2>
            把复杂的市场，
            <br />
            留给清晰的视角。
          </h2>
          <p className="muted">
            发现值得了解的市场，整理长期关注的基金，让每一次回看都有迹可循。
          </p>
          <ActionLink to="/discover">探索 Sora</ActionLink>
        </section>
        <Panel title="你的投资观察路径">
          <ol className="steps">
            <li>
              <strong>发现市场</strong>
              <p>先了解地区与主题，再进入具体基金。</p>
            </li>
            <li>
              <strong>建立关注</strong>
              <p>把想持续了解的标的放在一起。</p>
            </li>
            <li>
              <strong>回看组合</strong>
              <p>用一个视图理解持仓与变化。</p>
            </li>
          </ol>
          <p className="muted text-sm">
            当前为 M0 原型，关注与组合均为只读示例。
          </p>
        </Panel>
      </div>
    </>
  );
}
