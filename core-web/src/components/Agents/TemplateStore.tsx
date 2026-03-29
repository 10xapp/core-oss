import { useState, useEffect } from "react";
import { X } from "lucide-react";
import { Icon } from "../ui/Icon";
import {
  getAgentTemplates,
  createAgent,
  uploadAgentAvatar,
  type AgentTemplate,
  type AgentInstance,
} from "../../api/client";

const CATEGORY_COLORS: Record<string, string> = {
  general: "bg-gray-100 text-gray-600",
  engineering: "bg-blue-100 text-blue-600",
  marketing: "bg-purple-100 text-purple-600",
  research: "bg-green-100 text-green-600",
  content: "bg-orange-100 text-orange-600",
};

const TEMPLATE_GRADIENTS: Record<string, string> = {
  base: "linear-gradient(135deg, #6B7280 0%, #9CA3AF 100%)",
  "ai-engineer": "linear-gradient(135deg, #3B82F6 0%, #6366F1 100%)",
  "ai-marketer": "linear-gradient(135deg, #8B5CF6 0%, #EC4899 100%)",
  "ai-researcher": "linear-gradient(135deg, #10B981 0%, #059669 100%)",
  "ai-writer": "linear-gradient(135deg, #F59E0B 0%, #EF4444 100%)",
  "ai-brand-influencer": "linear-gradient(135deg, #EC4899 0%, #F97316 100%)",
};

interface TemplateStoreProps {
  workspaceId: string;
  onClose: () => void;
  onCreated: (agent: AgentInstance) => void;
}

export default function TemplateStore({ workspaceId, onClose, onCreated }: TemplateStoreProps) {
  const [templates, setTemplates] = useState<AgentTemplate[]>([]);
  const [loading, setLoading] = useState(true);
  const [selectedTemplate, setSelectedTemplate] = useState<AgentTemplate | null>(null);
  const [agentName, setAgentName] = useState("");
  const [creating, setCreating] = useState(false);

  // Identity fields
  const [backstory, setBackstory] = useState("");
  const [objective, setObjective] = useState("");
  const [personality, setPersonality] = useState("");
  const [avatarFile, setAvatarFile] = useState<File | null>(null);
  const [avatarPreview, setAvatarPreview] = useState<string | null>(null);

  useEffect(() => {
    getAgentTemplates()
      .then((result) => setTemplates(result.templates))
      .catch(console.error)
      .finally(() => setLoading(false));
  }, []);

  const supportsIdentity = selectedTemplate?.default_config &&
    (selectedTemplate.default_config as Record<string, unknown>).supports_identity === true;

  const handleAvatarChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    const file = e.target.files?.[0];
    if (file) {
      setAvatarFile(file);
      const reader = new FileReader();
      reader.onload = () => setAvatarPreview(reader.result as string);
      reader.readAsDataURL(file);
    }
  };

  const handleDeploy = async () => {
    if (!selectedTemplate || !agentName.trim()) return;
    setCreating(true);
    try {
      const agent = await createAgent(workspaceId, {
        name: agentName.trim(),
        template_slug: selectedTemplate.slug,
        ...(supportsIdentity ? {
          role: selectedTemplate.name,
          backstory: backstory.trim() || undefined,
          objective: objective.trim() || undefined,
          personality: personality.trim() || undefined,
        } : {}),
      });

      // Upload avatar if provided
      if (avatarFile) {
        try {
          const updated = await uploadAgentAvatar(agent.id, avatarFile);
          onCreated(updated);
          return;
        } catch (err) {
          console.error("Avatar upload failed (non-blocking):", err);
        }
      }

      onCreated(agent);
    } catch (err) {
      console.error("Failed to create agent:", err);
    } finally {
      setCreating(false);
    }
  };

  const handleBack = () => {
    setSelectedTemplate(null);
    setAgentName("");
    setBackstory("");
    setObjective("");
    setPersonality("");
    setAvatarFile(null);
    setAvatarPreview(null);
  };

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/40">
      <div className="bg-white rounded-xl shadow-2xl w-[600px] max-h-[80vh] flex flex-col overflow-hidden">
        {/* Header */}
        <div className="px-6 py-4 border-b border-border-light flex items-center justify-between">
          <div>
            <h2 className="text-base font-semibold text-text-body">
              {selectedTemplate ? "Deploy Agent" : "Agent Templates"}
            </h2>
            <p className="text-xs text-text-tertiary mt-0.5">
              {selectedTemplate
                ? supportsIdentity
                  ? "Give your agent an identity"
                  : "Name your agent and deploy it"
                : "Choose an archetype to deploy"}
            </p>
          </div>
          <button
            onClick={onClose}
            className="p-1.5 rounded-md hover:bg-bg-gray text-text-tertiary"
          >
            <Icon icon={X} size={16} />
          </button>
        </div>

        {/* Content */}
        <div className="flex-1 overflow-y-auto p-6">
          {loading ? (
            <div className="flex items-center justify-center py-12">
              <p className="text-sm text-text-tertiary">Loading templates...</p>
            </div>
          ) : selectedTemplate ? (
            /* Deploy form */
            <div className="space-y-4">
              {/* Selected template card */}
              <div className="flex items-center gap-3 p-3 rounded-lg bg-bg-mini-app">
                <div
                  className="w-10 h-10 rounded-lg flex items-center justify-center text-white font-bold text-sm shrink-0"
                  style={{ background: TEMPLATE_GRADIENTS[selectedTemplate.slug] || TEMPLATE_GRADIENTS.base }}
                >
                  {selectedTemplate.name.charAt(0)}
                </div>
                <div>
                  <p className="text-sm font-medium text-text-body">{selectedTemplate.name}</p>
                  <p className="text-xs text-text-tertiary">{selectedTemplate.description}</p>
                </div>
              </div>

              {/* Avatar upload (identity templates only) */}
              {supportsIdentity && (
                <div>
                  <label className="block text-xs font-medium text-text-secondary mb-1.5">
                    Face Image
                  </label>
                  <div className="flex items-center gap-3">
                    {avatarPreview ? (
                      <img src={avatarPreview} className="w-16 h-16 rounded-xl object-cover" />
                    ) : (
                      <div className="w-16 h-16 rounded-xl bg-bg-mini-app flex items-center justify-center text-text-tertiary text-2xl">
                        {agentName ? agentName.charAt(0).toUpperCase() : "?"}
                      </div>
                    )}
                    <label className="cursor-pointer text-xs px-3 py-1.5 rounded-lg border border-border-gray text-text-secondary hover:bg-white transition-colors">
                      Upload Image
                      <input type="file" accept="image/*" className="hidden" onChange={handleAvatarChange} />
                    </label>
                  </div>
                </div>
              )}

              {/* Name input */}
              <div>
                <label className="block text-xs font-medium text-text-secondary mb-1.5">
                  Agent Name
                </label>
                <input
                  type="text"
                  value={agentName}
                  onChange={(e) => setAgentName(e.target.value)}
                  onKeyDown={(e) => e.key === "Enter" && !supportsIdentity && handleDeploy()}
                  placeholder={supportsIdentity ? "e.g. Aria, Marcus, Luna..." : "e.g. Marketing Lead, Code Reviewer..."}
                  className="w-full text-sm px-3 py-2 rounded-lg border border-border-gray bg-white focus:outline-none focus:ring-1 focus:ring-brand-primary"
                  autoFocus
                />
              </div>

              {/* Identity fields (only for templates that support it) */}
              {supportsIdentity && (
                <>
                  <div>
                    <label className="block text-xs font-medium text-text-secondary mb-1.5">
                      Backstory / Description
                    </label>
                    <textarea
                      value={backstory}
                      onChange={(e) => setBackstory(e.target.value)}
                      placeholder="A 26-year-old lifestyle content creator who built a following around sustainable fashion and wellness..."
                      className="w-full text-sm px-3 py-2 rounded-lg border border-border-gray bg-white focus:outline-none focus:ring-1 focus:ring-brand-primary resize-none h-20"
                    />
                  </div>

                  <div>
                    <label className="block text-xs font-medium text-text-secondary mb-1.5">
                      Objective
                    </label>
                    <textarea
                      value={objective}
                      onChange={(e) => setObjective(e.target.value)}
                      placeholder="Build brand awareness and engagement through authentic, relatable content..."
                      className="w-full text-sm px-3 py-2 rounded-lg border border-border-gray bg-white focus:outline-none focus:ring-1 focus:ring-brand-primary resize-none h-16"
                    />
                  </div>

                  <div>
                    <label className="block text-xs font-medium text-text-secondary mb-1.5">
                      Personality
                    </label>
                    <input
                      type="text"
                      value={personality}
                      onChange={(e) => setPersonality(e.target.value)}
                      placeholder="warm, witty, trend-savvy"
                      className="w-full text-sm px-3 py-2 rounded-lg border border-border-gray bg-white focus:outline-none focus:ring-1 focus:ring-brand-primary"
                    />
                  </div>
                </>
              )}

              {/* Actions */}
              <div className="flex gap-2 pt-2">
                <button
                  onClick={handleBack}
                  className="text-sm px-4 py-2 rounded-lg text-text-secondary hover:bg-bg-gray"
                >
                  Back
                </button>
                <button
                  onClick={handleDeploy}
                  disabled={creating || !agentName.trim()}
                  className="flex-1 text-sm px-4 py-2 rounded-lg bg-black text-white hover:bg-gray-800 disabled:opacity-50 transition-colors"
                >
                  {creating ? "Deploying..." : "Deploy Agent"}
                </button>
              </div>
            </div>
          ) : (
            /* Template grid */
            <div className="grid grid-cols-2 gap-3">
              {templates.map((template) => (
                <button
                  key={template.id}
                  onClick={() => setSelectedTemplate(template)}
                  className="text-left p-4 rounded-lg border border-border-light hover:border-gray-300 hover:shadow-sm transition-all cursor-pointer group"
                >
                  <div className="flex items-start gap-3">
                    <div
                      className="w-9 h-9 rounded-lg flex items-center justify-center text-white font-bold text-sm shrink-0"
                      style={{ background: TEMPLATE_GRADIENTS[template.slug] || TEMPLATE_GRADIENTS.base }}
                    >
                      {template.name.charAt(0)}
                    </div>
                    <div className="flex-1 min-w-0">
                      <p className="text-sm font-medium text-text-body group-hover:text-black">
                        {template.name}
                      </p>
                      <span className={`inline-block text-[10px] px-1.5 py-0.5 rounded-full font-medium mt-1 ${CATEGORY_COLORS[template.category] || CATEGORY_COLORS.general}`}>
                        {template.category}
                      </span>
                    </div>
                  </div>
                  <p className="text-xs text-text-tertiary mt-2 line-clamp-2">
                    {template.description}
                  </p>
                </button>
              ))}
            </div>
          )}
        </div>
      </div>
    </div>
  );
}
