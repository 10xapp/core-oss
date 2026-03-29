import { useState } from "react";
import { useBuilderStore } from "../../stores/builderStore";
import { Plus, ArrowLeft, ArrowUp } from "lucide-react";
import { Icon } from "../ui/Icon";

function autoTitle(prompt: string): string {
  // Take first ~50 chars, cut at last word boundary
  const clean = prompt.replace(/\s+/g, " ").trim();
  if (clean.length <= 50) return clean;
  const cut = clean.substring(0, 50);
  const lastSpace = cut.lastIndexOf(" ");
  return (lastSpace > 20 ? cut.substring(0, lastSpace) : cut) + "...";
}

export default function ProjectList({
  onSelectProject,
}: {
  onSelectProject: (id: string) => void;
}) {
  const { projects, isLoadingProjects, createProject, setPendingPrompt } = useBuilderStore();
  const [prompt, setPrompt] = useState("");
  const [creating, setCreating] = useState(false);
  const [showCreateForm, setShowCreateForm] = useState(false);
  const [newProjectName, setNewProjectName] = useState("");

  const handlePromptSubmit = async () => {
    if (!prompt.trim() || creating) return;
    setCreating(true);
    try {
      const title = autoTitle(prompt.trim());
      const project = await createProject(title);
      setPendingPrompt(prompt.trim());
      setPrompt("");
      onSelectProject(project.id);
    } catch (err) {
      console.error("Failed to create project:", err);
    } finally {
      setCreating(false);
    }
  };

  const handleCreateNamed = async () => {
    if (!newProjectName.trim() || creating) return;
    setCreating(true);
    try {
      const project = await createProject(newProjectName.trim());
      setNewProjectName("");
      setShowCreateForm(false);
      onSelectProject(project.id);
    } catch (err) {
      console.error("Failed to create project:", err);
    } finally {
      setCreating(false);
    }
  };

  return (
    <div className="h-screen w-screen bg-bg-white flex flex-col">
      {/* Top bar */}
      <div className="h-14 border-b border-border-light flex items-center px-4 gap-3 shrink-0">
        <a
          href="/chat"
          className="p-1.5 rounded-lg hover:bg-gray-50 text-text-secondary"
        >
          <Icon icon={ArrowLeft} size={18} />
        </a>
        <h1 className="text-sm font-medium text-text-body">App Builder</h1>
      </div>

      {/* Main content */}
      <div className="flex-1 overflow-y-auto">
        {/* Hero / Prompt area */}
        <div className="max-w-2xl mx-auto px-6 pt-20 pb-12">
          <div className="text-center mb-8">
            <h2 className="text-2xl font-medium text-text-body mb-2">
              What do you want to build?
            </h2>
            <p className="text-sm text-text-secondary">
              Describe your app and we'll build it for you
            </p>
          </div>

          {/* Prompt input */}
          <div className="relative">
            <div className="border border-border-gray rounded-2xl bg-bg-white shadow-sm focus-within:border-border-gray-dark transition-colors">
              <textarea
                value={prompt}
                onChange={(e) => setPrompt(e.target.value)}
                onKeyDown={(e) => {
                  if (e.key === "Enter" && !e.shiftKey) {
                    e.preventDefault();
                    void handlePromptSubmit();
                  }
                }}
                placeholder="A task manager app with categories and due dates..."
                rows={3}
                disabled={creating}
                className="w-full resize-none outline-none text-sm text-text-body placeholder:text-text-tertiary px-4 pt-4 pb-2 rounded-2xl disabled:opacity-60"
              />
              <div className="flex items-center justify-between px-3 pb-3">
                <span className="text-[11px] text-text-tertiary font-mono">React Native</span>
                <button
                  onClick={() => void handlePromptSubmit()}
                  disabled={creating || !prompt.trim()}
                  className="p-2 rounded-full bg-gray-900 text-white disabled:opacity-30 hover:opacity-90 transition-opacity"
                >
                  <Icon icon={ArrowUp} size={14} />
                </button>
              </div>
            </div>
          </div>

          {/* Suggestion chips */}
          <div className="flex flex-wrap gap-2 mt-4 justify-center">
            {[
              "A recipe organizer",
              "Habit tracker with streaks",
              "Expense splitter for groups",
              "Flashcard study app",
            ].map((suggestion) => (
              <button
                key={suggestion}
                onClick={() => setPrompt(suggestion)}
                className="px-3 py-1.5 text-xs text-text-secondary border border-border-light rounded-full hover:bg-gray-50 transition-colors"
              >
                {suggestion}
              </button>
            ))}
          </div>
        </div>

        {/* Projects section */}
        <div className="max-w-3xl mx-auto px-6 pb-12">
          {isLoadingProjects ? (
            <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-3">
              {[...Array(3)].map((_, i) => (
                <div key={i} className="h-28 rounded-xl border border-border-light bg-gray-50 animate-pulse" />
              ))}
            </div>
          ) : projects.length > 0 ? (
            <>
              <div className="flex items-center justify-between mb-4">
                <h3 className="text-xs font-medium text-text-tertiary uppercase tracking-wide">
                  Your projects
                </h3>
                <button
                  onClick={() => setShowCreateForm(true)}
                  className="flex items-center gap-1 text-xs text-text-secondary hover:text-text-body transition-colors"
                >
                  <Icon icon={Plus} size={12} />
                  New
                </button>
              </div>

              {/* Inline create form */}
              {showCreateForm && (
                <div className="mb-4 p-4 border border-border-light rounded-xl bg-gray-50">
                  <input
                    type="text"
                    value={newProjectName}
                    onChange={(e) => setNewProjectName(e.target.value)}
                    placeholder="Project name"
                    className="w-full border border-border-gray rounded-lg px-3 py-2 text-sm outline-none focus:border-border-gray-dark bg-white"
                    autoFocus
                    onKeyDown={(e) => {
                      if (e.key === "Enter") void handleCreateNamed();
                      if (e.key === "Escape") setShowCreateForm(false);
                    }}
                  />
                  <div className="flex gap-2 mt-3 justify-end">
                    <button
                      onClick={() => setShowCreateForm(false)}
                      className="px-3 py-1.5 text-sm text-text-secondary hover:text-text-body"
                    >
                      Cancel
                    </button>
                    <button
                      onClick={() => void handleCreateNamed()}
                      disabled={creating || !newProjectName.trim()}
                      className="px-4 py-1.5 text-sm font-medium rounded-full bg-gray-900 text-white hover:opacity-90 disabled:opacity-50"
                    >
                      {creating ? "Creating..." : "Create"}
                    </button>
                  </div>
                </div>
              )}

              <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-3">
                {projects.map((project) => (
                  <button
                    key={project.id}
                    onClick={() => onSelectProject(project.id)}
                    className="text-left p-4 rounded-xl border border-border-light hover:border-border-gray transition-all group"
                  >
                    <h3 className="text-sm font-medium text-text-body truncate group-hover:text-gray-900">
                      {project.name}
                    </h3>
                    {project.description && (
                      <p className="text-xs text-text-secondary mt-1 line-clamp-2">
                        {project.description}
                      </p>
                    )}
                    <p className="text-[11px] text-text-tertiary mt-2 font-mono">
                      {new Date(project.updated_at).toLocaleDateString()}
                    </p>
                  </button>
                ))}
              </div>
            </>
          ) : null}
        </div>
      </div>
    </div>
  );
}
