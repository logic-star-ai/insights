export type AgentInfo = {
  logo: string;
  name: string;

  logo_invert: boolean;
  url?: string;
}

export enum AgentType {
  HUMAN = "human",
  BOT = "bot",
  CODEX = "codex",
  CLAUDE = "claude",
  DEVIN = "devin",
  CODEGEN = "codegen",
  COPILOT = "copilot",
  JULES = "jules",
  CURSOR = "cursor",
  TEMBO = "tembo",
  COSINE = "cosine",
  OPENHANDS = "openhands",
}

export const agentsInfo: Record<AgentType, AgentInfo> = {
  [AgentType.HUMAN]: {
    logo: "/icons/human.svg",
    logo_invert: false,
    name: "Human"
  },
  [AgentType.BOT]: {
    logo: "/icons/bot.png",
    logo_invert: true,
    name: "Bot"
  },
  [AgentType.CODEX]: {
    logo: "icons/openai.svg",
    logo_invert: true,
    name: "OpenAI Codex",
    url: "https://openai.com/index/introducing-codex/"
  },
  [AgentType.JULES]: {
    logo: "/icons/jules.svg",
    logo_invert: false,
    name: "Google Jules",
    url: "https://blog.google/technology/google-labs/jules/"
  },
  [AgentType.COPILOT]: {
    logo: "/icons/copilot.svg",
    logo_invert: true,
    name: "GitHub Copilot",
    url: "https://docs.github.com/en/copilot/how-tos/agents/copilot-coding-agent/using-copilot-to-work-on-an-issue"
  },
  [AgentType.CLAUDE]: {
    logo: "/icons/claude.png",
    logo_invert: false,
    name: "Claude Agent"
  },
  [AgentType.DEVIN]: {
    logo: "/icons/devin.svg",
    logo_invert: false,
    name: "Devin",
    url: "https://devin.ai/"
  },
  [AgentType.CODEGEN]: {
    logo: "/icons/codegen.svg",
    logo_invert: true,
    name: "Codegen",
    url: "https://codegen.com/"
  },
  [AgentType.CURSOR]: {
    logo: "/icons/cursor.svg",
    logo_invert: true,
    name: "Cursor Agent",
    url: "https://docs.cursor.com/background-agent"
  },
  [AgentType.TEMBO]: {
    logo: "/icons/tembo.jpeg",
    logo_invert: true,
    name: "Tembo",
    url: "https://www.tembo.io/"
  },
  [AgentType.OPENHANDS]: {
    logo: "/icons/openhands.png",
    logo_invert: false,
    name: "OpenHands",
    url: "https://docs.all-hands.dev/"
  },
  [AgentType.COSINE]: {
    logo: "/icons/cosine.png",
    logo_invert: true,
    name: "Cosine",
    url: "https://cosine.sh/"
  },
}

export const agentsList: Array<AgentType> = Object.keys(agentsInfo) as Array<AgentType>
